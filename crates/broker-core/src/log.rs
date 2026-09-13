//! Append-only partition log with Kafka offset semantics.
//!
//! A [`PartitionLog`] is an ordered sequence of batches. Each [`append`] is
//! atomic: either every record of the batch lands with a contiguous offset
//! range, or none does. Offsets start at zero, never move backwards, and are
//! assigned by the log itself.
//!
//! The log is pure in-memory state. Durability, segmenting, and replication
//! live on the runtime side of the crate boundary; see
//! `docs/architecture.md`.
//!
//! [`append`]: PartitionLog::append

use alloc::vec::Vec;

use crate::error::LogError;

/// Maximum records accepted in one atomic append.
pub const MAX_APPEND_RECORDS: usize = 1024;

/// Per-record byte overhead charged against a fetch window's byte budget.
pub const RECORD_OVERHEAD_BYTES: usize = 32;

/// Borrowed record input for an append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordInput<'a> {
    /// Producer timestamp in milliseconds.
    pub timestamp_ms: i64,
    /// Optional record key.
    pub key: Option<&'a [u8]>,
    /// Optional record value.
    pub value: Option<&'a [u8]>,
}

/// Owned record input that can cross a shard boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordData {
    /// Producer timestamp in milliseconds.
    pub timestamp_ms: i64,
    /// Optional record key.
    pub key: Option<Vec<u8>>,
    /// Optional record value.
    pub value: Option<Vec<u8>>,
}

impl RecordData {
    /// Borrows this record as append input.
    pub fn as_input(&self) -> RecordInput<'_> {
        RecordInput {
            timestamp_ms: self.timestamp_ms,
            key: self.key.as_deref(),
            value: self.value.as_deref(),
        }
    }
}

impl From<RecordInput<'_>> for RecordData {
    fn from(input: RecordInput<'_>) -> Self {
        Self {
            timestamp_ms: input.timestamp_ms,
            key: input.key.map(Vec::from),
            value: input.value.map(Vec::from),
        }
    }
}

impl From<Record> for RecordData {
    fn from(record: Record) -> Self {
        Self {
            timestamp_ms: record.timestamp_ms,
            key: record.key,
            value: record.value,
        }
    }
}

/// A stored record with its assigned offset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    /// Log-assigned offset.
    pub offset: i64,
    /// Producer timestamp in milliseconds.
    pub timestamp_ms: i64,
    /// Optional record key.
    pub key: Option<Vec<u8>>,
    /// Optional record value.
    pub value: Option<Vec<u8>>,
}

/// Result of a fetch: owned records plus the partition high watermark.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchWindow {
    /// Next offset to be assigned; readers at this offset see no records.
    pub high_watermark: i64,
    /// Owned records beginning at the requested offset.
    pub records: Vec<Record>,
}

#[derive(Debug, Eq, PartialEq)]
struct StoredBatch {
    base_offset: i64,
    records: Vec<Record>,
}

/// One partition's ordered record history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct PartitionLog {
    batches: Vec<StoredBatch>,
    next_offset: i64,
}

impl PartitionLog {
    /// Creates an empty log whose first assigned offset is zero.
    pub const fn new() -> Self {
        Self {
            batches: Vec::new(),
            next_offset: 0,
        }
    }

    /// The next offset that will be assigned.
    pub const fn high_watermark(&self) -> i64 {
        self.next_offset
    }

    /// The earliest offset retained by this log.
    pub const fn start_offset(&self) -> i64 {
        0
    }

    /// Total number of stored records.
    pub fn record_count(&self) -> usize {
        self.batches.iter().map(|batch| batch.records.len()).sum()
    }

    /// Appends one batch of borrowed records atomically and returns its base
    /// offset.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::EmptyAppend`] for an empty batch and
    /// [`LogError::TooManyRecords`] beyond [`MAX_APPEND_RECORDS`].
    pub fn append(&mut self, inputs: &[RecordInput<'_>]) -> Result<i64, LogError> {
        if inputs.is_empty() {
            return Err(LogError::EmptyAppend);
        }
        if inputs.len() > MAX_APPEND_RECORDS {
            return Err(LogError::TooManyRecords);
        }

        let base_offset = self.next_offset;
        let mut records = Vec::with_capacity(inputs.len());
        for (index, input) in inputs.iter().enumerate() {
            records.push(Record {
                offset: base_offset + index as i64,
                timestamp_ms: input.timestamp_ms,
                key: input.key.map(Vec::from),
                value: input.value.map(Vec::from),
            });
        }
        self.next_offset = base_offset + records.len() as i64;
        self.batches.push(StoredBatch {
            base_offset,
            records,
        });
        Ok(base_offset)
    }

    /// Appends owned records atomically and returns the base offset.
    pub fn append_data(&mut self, records: &[RecordData]) -> Result<i64, LogError> {
        let inputs: Vec<RecordInput<'_>> = records.iter().map(RecordData::as_input).collect();
        self.append(&inputs)
    }

    /// Reads records beginning at `offset`, bounded by `max_records` and an
    /// approximate byte budget.
    ///
    /// The first available record is returned even when it exceeds `max_bytes`,
    /// so a reader always makes progress. A fetch at the high watermark
    /// returns an empty window rather than an error.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::OffsetOutOfRange`] for a negative offset or one
    /// beyond the high watermark.
    pub fn fetch(
        &self,
        offset: i64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<FetchWindow, LogError> {
        if offset < self.start_offset() || offset > self.next_offset {
            return Err(LogError::OffsetOutOfRange);
        }

        let mut records = Vec::new();
        let mut bytes = 0usize;
        if max_records > 0 && offset < self.next_offset {
            let start_batch = self.batch_index_for(offset);
            'batches: for batch in &self.batches[start_batch..] {
                for record in &batch.records {
                    if record.offset < offset {
                        continue;
                    }
                    if records.len() == max_records {
                        break 'batches;
                    }
                    let size = record_size(record);
                    if !records.is_empty() && bytes + size > max_bytes {
                        break 'batches;
                    }
                    bytes += size;
                    records.push(record.clone());
                }
            }
        }

        Ok(FetchWindow {
            high_watermark: self.next_offset,
            records,
        })
    }

    /// Reads the earliest or latest offset of this partition.
    pub const fn list_offset(&self, earliest: bool) -> i64 {
        if earliest {
            self.start_offset()
        } else {
            self.next_offset
        }
    }

    /// Index of the last batch whose base offset is at or below `offset`.
    fn batch_index_for(&self, offset: i64) -> usize {
        let mut low = 0usize;
        let mut high = self.batches.len();
        while low < high {
            let middle = low + (high - low) / 2;
            if self.batches[middle].base_offset <= offset {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low.saturating_sub(1)
    }
}

/// Approximate stored size of one record.
fn record_size(record: &Record) -> usize {
    RECORD_OVERHEAD_BYTES
        + record.key.as_ref().map_or(0, Vec::len)
        + record.value.as_ref().map_or(0, Vec::len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(value: &[u8]) -> RecordInput<'_> {
        RecordInput {
            timestamp_ms: 1_000,
            key: None,
            value: Some(value),
        }
    }

    #[test]
    fn append_assigns_contiguous_offsets() {
        let mut log = PartitionLog::new();
        assert_eq!(log.append(&[input(b"a"), input(b"b")]), Ok(0));
        assert_eq!(log.append(&[input(b"c")]), Ok(2));
        assert_eq!(log.high_watermark(), 3);
        assert_eq!(log.record_count(), 3);
        assert_eq!(log.start_offset(), 0);
        assert_eq!(log.list_offset(true), 0);
        assert_eq!(log.list_offset(false), 3);
    }

    #[test]
    fn fetch_returns_owned_records_from_the_requested_offset() {
        let mut log = PartitionLog::new();
        log.append(&[input(b"a"), input(b"b"), input(b"c")]).expect("append");
        let window = log.fetch(1, 10, 1024).expect("fetch");
        assert_eq!(window.high_watermark, 3);
        assert_eq!(window.records.len(), 2);
        assert_eq!(window.records[0].offset, 1);
        assert_eq!(window.records[0].value.as_deref(), Some(b"b".as_slice()));
        assert_eq!(window.records[1].offset, 2);
    }

    #[test]
    fn fetch_honors_record_and_byte_budgets() {
        let mut log = PartitionLog::new();
        log.append(&[input(b"a"), input(b"b"), input(b"c")]).expect("append");
        let window = log.fetch(0, 2, 1024).expect("fetch");
        assert_eq!(window.records.len(), 2);
        let window = log.fetch(0, 10, RECORD_OVERHEAD_BYTES + 2).expect("fetch");
        assert_eq!(window.records.len(), 1, "byte budget admits only the first record");
    }

    #[test]
    fn fetch_at_high_watermark_is_empty() {
        let mut log = PartitionLog::new();
        log.append(&[input(b"a")]).expect("append");
        let window = log.fetch(1, 10, 1024).expect("fetch");
        assert!(window.records.is_empty());
        assert_eq!(window.high_watermark, 1);
    }

    #[test]
    fn fetch_out_of_range_is_rejected() {
        let mut log = PartitionLog::new();
        log.append(&[input(b"a")]).expect("append");
        assert_eq!(log.fetch(-1, 10, 1024), Err(LogError::OffsetOutOfRange));
        assert_eq!(log.fetch(2, 10, 1024), Err(LogError::OffsetOutOfRange));
    }

    #[test]
    fn append_rejects_empty_and_oversized_batches() {
        let mut log = PartitionLog::new();
        assert_eq!(log.append(&[]), Err(LogError::EmptyAppend));
        let oversized = alloc::vec![input(b"x"); MAX_APPEND_RECORDS + 1];
        assert_eq!(log.append(&oversized), Err(LogError::TooManyRecords));
        assert_eq!(log.high_watermark(), 0, "rejected appends assign no offsets");
    }

    #[test]
    fn fetch_spans_multiple_batches() {
        let mut log = PartitionLog::new();
        log.append(&[input(b"a")]).expect("append");
        log.append(&[input(b"b"), input(b"c")]).expect("append");
        let window = log.fetch(1, 10, 1024).expect("fetch");
        assert_eq!(window.records.len(), 2);
        assert_eq!(window.records[0].offset, 1);
        assert_eq!(window.records[1].offset, 2);
    }
}

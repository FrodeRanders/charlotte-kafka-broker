//! Kafka record batch v2 encoding and decoding.
//!
//! Produce requests carry one or more record batches in a partition's `records`
//! field; fetch responses carry them back. The broker assigns offsets, so
//! decoding drops the producer-provided offsets and encoding writes the stored
//! offsets relative to the batch's base offset.
//!
//! Compression and control batches are rejected. Transactional batches are
//! accepted when their producer identity is present and are resolved by the
//! runtime coordinator.

use alloc::vec::Vec;

use broker_core::{
    Record,
    RecordData,
};

use crate::{
    codec::{
        Decoder,
        put_varint,
        put_varlong,
        read_varint,
        read_varlong,
    },
    protocol::{
        Error,
        MAX_ARRAY_LEN,
        MAX_FRAME_LEN,
        MAX_RECORDS,
    },
};

/// Records decoded from a produce request and their optional producer identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedRecordBatch {
    pub records: Vec<RecordData>,
    pub producer: Option<(i64, i16)>,
}

/// CRC32C (Castagnoli) of `bytes`, as required by record batch v2.
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82f6_3b78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Encodes stored records as one record batch v2 with the given base offset.
///
/// # Errors
///
/// Returns [`Error::Invalid`] for an empty or oversized record slice and
/// [`Error::TooLarge`] when the batch exceeds [`MAX_FRAME_LEN`].
pub fn encode_record_batch(base_offset: i64, records: &[Record]) -> Result<Vec<u8>, Error> {
    if records.is_empty() || records.len() > MAX_RECORDS {
        return Err(Error::Invalid);
    }

    let first_timestamp = records[0].timestamp_ms;
    let mut encoded_records = Vec::new();
    for record in records {
        let mut body = Vec::new();
        body.push(0);
        put_varlong(&mut body, record.timestamp_ms.saturating_sub(first_timestamp));
        put_varint(&mut body, (record.offset - base_offset) as i32);
        write_optional_bytes(&mut body, record.key.as_deref())?;
        write_optional_bytes(&mut body, record.value.as_deref())?;
        put_varint(&mut body, 0);

        put_varint(&mut encoded_records, body.len() as i32);
        encoded_records.extend_from_slice(&body);
    }

    let max_timestamp =
        records.iter().map(|record| record.timestamp_ms).max().unwrap_or(first_timestamp);
    let mut body = Vec::new();
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.push(2);
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    body.extend_from_slice(&((records.len() - 1) as i32).to_be_bytes());
    body.extend_from_slice(&first_timestamp.to_be_bytes());
    body.extend_from_slice(&max_timestamp.to_be_bytes());
    body.extend_from_slice(&(-1i64).to_be_bytes());
    body.extend_from_slice(&(-1i16).to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&(records.len() as i32).to_be_bytes());
    body.extend_from_slice(&encoded_records);

    let crc = crc32c(&body[9..]);
    body[5..9].copy_from_slice(&crc.to_be_bytes());

    let mut batch = Vec::new();
    batch.extend_from_slice(&base_offset.to_be_bytes());
    batch.extend_from_slice(&(body.len() as i32).to_be_bytes());
    batch.extend_from_slice(&body);
    if batch.len() > MAX_FRAME_LEN {
        return Err(Error::TooLarge);
    }
    Ok(batch)
}

/// Decodes every record batch or legacy message set in a produce request.
///
/// Kafka brokers accept message format v0/v1 sets in Produce requests and
/// up-convert them; independent clients such as kafka-python still send them
/// inside Produce v3. The record-set magic byte sits at offset 16 in both the
/// v2 record batch and the legacy message set layouts, so the format is
/// detected before decoding.
///
/// # Errors
///
/// Returns [`Error::Checksum`] for a CRC mismatch,
/// [`Error::UnsupportedVersion`] for compressed, control, or transactional
/// batches, and [`Error::Invalid`] or [`Error::Incomplete`] for malformed
/// input.
pub fn decode_record_batches(bytes: &[u8]) -> Result<Vec<RecordData>, Error> {
    Ok(decode_record_batches_with_identity(bytes)?.records)
}

/// Decodes records and the producer identity carried by a v2 batch.
pub fn decode_record_batches_with_identity(bytes: &[u8]) -> Result<DecodedRecordBatch, Error> {
    if bytes.is_empty() {
        return Err(Error::Invalid);
    }
    match bytes.get(16) {
        Some(2) => decode_v2_record_set(bytes),
        Some(0) | Some(1) => decode_legacy_message_set(bytes).map(|records| DecodedRecordBatch {
            records,
            producer: None,
        }),
        _ => Err(Error::UnsupportedVersion),
    }
}

fn decode_v2_record_set(bytes: &[u8]) -> Result<DecodedRecordBatch, Error> {
    if bytes.is_empty() {
        return Err(Error::Invalid);
    }

    let mut decoder = Decoder::new(bytes);
    let mut records = Vec::new();
    let mut identity = None;
    while !decoder.done() {
        if decoder.remaining() < 12 {
            return Err(Error::Incomplete);
        }
        let _base_offset = decoder.i64()?;
        let batch_len = decoder.i32()?;
        if batch_len < 49 {
            return Err(Error::Invalid);
        }
        let batch = decoder.take(batch_len as usize)?;

        let mut batch_decoder = Decoder::new(batch);
        let _partition_leader_epoch = batch_decoder.i32()?;
        if batch_decoder.i8()? != 2 {
            return Err(Error::UnsupportedVersion);
        }
        let expected_crc = batch_decoder.i32()? as u32;
        if crc32c(&batch[9..]) != expected_crc {
            return Err(Error::Checksum);
        }
        let attributes = batch_decoder.i16()?;
        if attributes & 0x07 != 0 || attributes & 0x20 != 0 {
            return Err(Error::UnsupportedVersion);
        }
        let _last_offset_delta = batch_decoder.i32()?;
        let first_timestamp = batch_decoder.i64()?;
        let _max_timestamp = batch_decoder.i64()?;
        let producer_id = batch_decoder.i64()?;
        let producer_epoch = batch_decoder.i16()?;
        let _base_sequence = batch_decoder.i32()?;
        let count = batch_decoder.i32()?;
        if count < 0 || count as usize > MAX_RECORDS {
            return Err(Error::TooLarge);
        }

        if attributes & 0x10 != 0 {
            let next = (producer_id, producer_epoch);
            if identity.is_some_and(|previous| previous != next) {
                return Err(Error::Invalid);
            }
            identity = Some(next);
        }
        for _ in 0..count {
            let len = read_varint(&mut batch_decoder)?;
            if len < 0 {
                return Err(Error::Invalid);
            }
            let body = batch_decoder.take(len as usize)?;
            let mut record = Decoder::new(body);
            let _attributes = record.i8()?;
            let timestamp_delta = read_varlong(&mut record)?;
            let _offset_delta = read_varint(&mut record)?;
            let key_len = read_varint(&mut record)?;
            let key = read_optional_bytes(&mut record, key_len)?;
            let value_len = read_varint(&mut record)?;
            let value = read_optional_bytes(&mut record, value_len)?;
            let headers = read_varint(&mut record)?;
            if headers < 0 || headers as usize > MAX_ARRAY_LEN {
                return Err(Error::Invalid);
            }
            for _ in 0..headers {
                let header_key_len = read_varint(&mut record)?;
                if header_key_len < 0 {
                    return Err(Error::Invalid);
                }
                let _ = record.take(header_key_len as usize)?;
                let header_value_len = read_varint(&mut record)?;
                if header_value_len >= 0 {
                    let _ = record.take(header_value_len as usize)?;
                } else if header_value_len != -1 {
                    return Err(Error::Invalid);
                }
            }
            if !record.done() {
                return Err(Error::Invalid);
            }
            records.push(RecordData {
                timestamp_ms: first_timestamp.saturating_add(timestamp_delta),
                key,
                value,
            });
        }
        if !batch_decoder.done() {
            return Err(Error::Invalid);
        }
    }
    Ok(DecodedRecordBatch {
        records,
        producer: identity,
    })
}

/// Decodes a legacy message format v0/v1 set.
fn decode_legacy_message_set(bytes: &[u8]) -> Result<Vec<RecordData>, Error> {
    let mut decoder = Decoder::new(bytes);
    let mut records = Vec::new();
    while !decoder.done() {
        let _offset = decoder.i64()?;
        let message_size = decoder.i32()?;
        if message_size < 14 {
            return Err(Error::Invalid);
        }
        let message = decoder.take(message_size as usize)?;
        let expected_crc = u32::from_be_bytes(message[..4].try_into().map_err(|_| Error::Invalid)?);
        if crc32_ieee(&message[4..]) != expected_crc {
            return Err(Error::Checksum);
        }
        let magic = message[4];
        if magic > 1 {
            return Err(Error::UnsupportedVersion);
        }
        let attributes = message[5];
        if attributes & 0x07 != 0 {
            return Err(Error::UnsupportedVersion);
        }
        let mut cursor = 6usize;
        let timestamp_ms = if magic == 1 {
            let end = cursor + 8;
            let value = message.get(cursor..end).ok_or(Error::Incomplete)?;
            cursor = end;
            i64::from_be_bytes(value.try_into().map_err(|_| Error::Invalid)?)
        } else {
            0
        };
        let key = legacy_optional_bytes(message, &mut cursor)?;
        let value = legacy_optional_bytes(message, &mut cursor)?;
        if cursor != message.len() {
            return Err(Error::Invalid);
        }
        records.push(RecordData {
            timestamp_ms,
            key,
            value,
        });
    }
    Ok(records)
}

fn legacy_optional_bytes(message: &[u8], cursor: &mut usize) -> Result<Option<Vec<u8>>, Error> {
    let end = *cursor + 4;
    let length = i32::from_be_bytes(
        message
            .get(*cursor..end)
            .ok_or(Error::Incomplete)?
            .try_into()
            .map_err(|_| Error::Invalid)?,
    );
    *cursor = end;
    if length == -1 {
        return Ok(None);
    }
    if length < 0 {
        return Err(Error::Invalid);
    }
    let end = *cursor + length as usize;
    let value = message.get(*cursor..end).ok_or(Error::Incomplete)?;
    *cursor = end;
    Ok(Some(value.to_vec()))
}

/// CRC32 (IEEE) used by legacy v0/v1 message sets.
fn crc32_ieee(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn write_optional_bytes(output: &mut Vec<u8>, value: Option<&[u8]>) -> Result<(), Error> {
    match value {
        Some(value) => {
            put_varint(output, i32::try_from(value.len()).map_err(|_| Error::TooLarge)?);
            output.extend_from_slice(value);
            Ok(())
        }
        None => {
            put_varint(output, -1);
            Ok(())
        }
    }
}

fn read_optional_bytes(decoder: &mut Decoder<'_>, len: i32) -> Result<Option<Vec<u8>>, Error> {
    if len == -1 {
        Ok(None)
    } else if len >= 0 {
        Ok(Some(decoder.take(len as usize)?.to_vec()))
    } else {
        Err(Error::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(offset: i64, timestamp_ms: i64, key: Option<&[u8]>, value: Option<&[u8]>) -> Record {
        Record {
            offset,
            timestamp_ms,
            key: key.map(Vec::from),
            value: value.map(Vec::from),
        }
    }

    #[test]
    fn crc32c_matches_known_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn encode_decode_round_trip_preserves_records() {
        let batch = encode_record_batch(
            7,
            &[
                record(7, 1_000, Some(b"a"), Some(b"first")),
                record(8, 1_004, None, Some(b"second")),
            ],
        )
        .expect("encode");
        let records = decode_record_batches(&batch).expect("decode");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].timestamp_ms, 1_000);
        assert_eq!(records[0].key.as_deref(), Some(b"a".as_slice()));
        assert_eq!(records[0].value.as_deref(), Some(b"first".as_slice()));
        assert_eq!(records[1].timestamp_ms, 1_004);
        assert!(records[1].key.is_none());
        assert_eq!(records[1].value.as_deref(), Some(b"second".as_slice()));
    }

    #[test]
    fn multiple_batches_decode_in_order() {
        let first = encode_record_batch(0, &[record(0, 1, None, Some(b"a"))]).expect("encode");
        let second = encode_record_batch(1, &[record(1, 2, None, Some(b"b"))]).expect("encode");
        let mut set = first.clone();
        set.extend_from_slice(&second);
        let records = decode_record_batches(&set).expect("decode");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].value.as_deref(), Some(b"a".as_slice()));
        assert_eq!(records[1].value.as_deref(), Some(b"b".as_slice()));
    }

    #[test]
    fn checksum_mismatch_is_rejected() {
        let mut batch =
            encode_record_batch(0, &[record(0, 1, None, Some(b"value"))]).expect("encode");
        let last = batch.len() - 1;
        batch[last] ^= 0xff;
        assert_eq!(decode_record_batches(&batch), Err(Error::Checksum));
    }

    #[test]
    fn compressed_batch_is_rejected() {
        let mut batch =
            encode_record_batch(0, &[record(0, 1, None, Some(b"value"))]).expect("encode");
        let attributes = 12 + 4 + 1 + 4;
        batch[attributes..attributes + 2].copy_from_slice(&2i16.to_be_bytes());
        let crc = crc32c(&batch[12 + 9..]);
        batch[12 + 5..12 + 9].copy_from_slice(&crc.to_be_bytes());
        assert_eq!(decode_record_batches(&batch), Err(Error::UnsupportedVersion));
    }

    #[test]
    fn empty_record_set_is_rejected() {
        assert_eq!(decode_record_batches(&[]), Err(Error::Invalid));
    }

    fn legacy_message_set(
        magic: i8,
        timestamp_ms: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Vec<u8> {
        let mut body = alloc::vec![magic as u8, 0];
        if magic == 1 {
            body.extend_from_slice(&timestamp_ms.to_be_bytes());
        }
        for field in [key, value] {
            match field {
                Some(bytes) => {
                    body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    body.extend_from_slice(bytes);
                }
                None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        let crc = crc32_ieee(&body);
        let mut message = crc.to_be_bytes().to_vec();
        message.extend_from_slice(&body);
        let mut set = 0i64.to_be_bytes().to_vec();
        set.extend_from_slice(&(message.len() as i32).to_be_bytes());
        set.extend_from_slice(&message);
        set
    }

    #[test]
    fn legacy_message_sets_decode() {
        let v1 = legacy_message_set(1, 1_234, Some(b"k"), Some(b"value"));
        let records = decode_record_batches(&v1).expect("v1 decode");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].timestamp_ms, 1_234);
        assert_eq!(records[0].key.as_deref(), Some(b"k".as_slice()));
        assert_eq!(records[0].value.as_deref(), Some(b"value".as_slice()));

        let v0 = legacy_message_set(0, 0, None, Some(b"legacy"));
        let records = decode_record_batches(&v0).expect("v0 decode");
        assert_eq!(records[0].timestamp_ms, 0);
        assert!(records[0].key.is_none());
        assert_eq!(records[0].value.as_deref(), Some(b"legacy".as_slice()));
    }

    #[test]
    fn legacy_crc_mismatch_is_rejected() {
        let mut v1 = legacy_message_set(1, 1, None, Some(b"value"));
        *v1.last_mut().expect("non-empty") ^= 0xff;
        assert_eq!(decode_record_batches(&v1), Err(Error::Checksum));
    }

    #[test]
    fn crc32_ieee_matches_known_vector() {
        assert_eq!(crc32_ieee(b"123456789"), 0xcbf4_3926);
    }
}

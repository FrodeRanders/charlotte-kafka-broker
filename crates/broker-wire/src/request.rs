//! Request decoding for the supported Kafka subset.

use alloc::vec::Vec;

use broker_core::RecordData;

use crate::{
    codec::Decoder,
    protocol::{
        self,
        Error,
        MAX_FRAME_LEN,
        api,
        version,
    },
    record_batch::decode_record_batches_with_identity,
};

/// Kafka request header version 1 (non-flexible).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header {
    /// API key.
    pub api_key: i16,
    /// API version.
    pub api_version: i16,
    /// Client correlation id echoed in the response.
    pub correlation_id: i32,
    /// Optional client id.
    pub client_id: Option<Vec<u8>>,
}

/// One partition entry of a produce request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProducePartition {
    /// Partition index.
    pub partition: i32,
    /// Decoded records to append.
    pub records: Vec<RecordData>,
    /// Producer ID/epoch from a transactional record batch, if present.
    pub producer: Option<(i64, i16)>,
}

/// One topic entry of a produce request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProduceTopic {
    /// Topic name.
    pub name: Vec<u8>,
    /// Partitions carried by this request.
    pub partitions: Vec<ProducePartition>,
}

/// One partition entry of a fetch request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FetchPartition {
    /// Partition index.
    pub partition: i32,
    /// Next offset the client wants to read.
    pub fetch_offset: i64,
    /// Per-partition byte cap.
    pub partition_max_bytes: i32,
}

/// One topic entry of a fetch request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchTopic {
    /// Topic name.
    pub name: Vec<u8>,
    /// Partitions carried by this request.
    pub partitions: Vec<FetchPartition>,
}

/// One partition entry of a list-offsets request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListOffsetsPartition {
    /// Partition index.
    pub partition: i32,
    /// `-2` for earliest, `-1` for latest.
    pub timestamp: i64,
}

/// One topic entry of a list-offsets request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListOffsetsTopic {
    /// Topic name.
    pub name: Vec<u8>,
    /// Partitions carried by this request.
    pub partitions: Vec<ListOffsetsPartition>,
}

/// The body of a decoded request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequestBody {
    /// ApiVersions has no body.
    ApiVersions,
    /// Finds the coordinator for a group or transactional id.
    FindCoordinator {
        key: Vec<u8>,
        key_type: i8,
    },
    /// Allocates a producer identity for a transactional id.
    InitProducerId {
        transactional_id: Option<Vec<u8>>,
        timeout_ms: i32,
    },
    AddPartitionsToTxn {
        transactional_id: Vec<u8>,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<(Vec<u8>, Vec<i32>)>,
    },
    EndTxn {
        transactional_id: Vec<u8>,
        producer_id: i64,
        producer_epoch: i16,
        commit: bool,
    },
    JoinGroup {
        group_id: Vec<u8>,
        member_id: Vec<u8>,
        subscriptions: Vec<Vec<u8>>,
    },
    SyncGroup {
        group_id: Vec<u8>,
        generation: i32,
        member_id: Vec<u8>,
        assignment: Vec<u8>,
    },
    Heartbeat {
        group_id: Vec<u8>,
        generation: i32,
        member_id: Vec<u8>,
    },
    LeaveGroup {
        group_id: Vec<u8>,
        member_id: Vec<u8>,
    },
    OffsetCommit {
        group_id: Vec<u8>,
        generation: i32,
        member_id: Vec<u8>,
        topic: Vec<u8>,
        partition: i32,
        offset: i64,
    },
    OffsetFetch {
        group_id: Vec<u8>,
        topic: Vec<u8>,
        partition: i32,
    },
    /// Metadata for the listed topics, or every topic when `None`.
    Metadata {
        /// Requested topic names.
        topics: Option<Vec<Vec<u8>>>,
    },
    /// Produce records.
    Produce {
        /// Transactional id; only `None` is supported.
        transactional_id: Option<Vec<u8>>,
        /// Requested acknowledgement mode echoed by the client.
        acks: i16,
        /// Client timeout hint.
        timeout_ms: i32,
        /// Topics carried by this request.
        topics: Vec<ProduceTopic>,
    },
    /// Fetch records.
    Fetch {
        /// Replica id; `-1` for consumers.
        replica_id: i32,
        /// Maximum time the broker may wait before answering.
        max_wait_ms: i32,
        /// Minimum bytes before answering early.
        min_bytes: i32,
        /// Overall byte cap.
        max_bytes: i32,
        /// Whether the client asked for read-committed isolation.
        read_committed: bool,
        /// Topics carried by this request.
        topics: Vec<FetchTopic>,
    },
    /// List partition offsets.
    ListOffsets {
        /// Replica id; `-1` for consumers.
        replica_id: i32,
        /// Topics carried by this request.
        topics: Vec<ListOffsetsTopic>,
    },
}

/// A decoded request frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Request {
    /// Wire header.
    pub header: Header,
    /// Typed body.
    pub body: RequestBody,
}

/// Reads only the fixed request header fields.
///
/// This exists for the ApiVersions downgrade response: a request using a
/// version this broker does not implement cannot be decoded, but the error
/// response still needs its correlation id. The header fields used here are
/// identical in the v1 and flexible v2 request headers.
pub fn peek_header(frame: &[u8]) -> Result<Header, Error> {
    if frame.len() < 12 {
        return Err(Error::Incomplete);
    }
    Ok(Header {
        api_key: i16::from_be_bytes(frame[4..6].try_into().map_err(|_| Error::Invalid)?),
        api_version: i16::from_be_bytes(frame[6..8].try_into().map_err(|_| Error::Invalid)?),
        correlation_id: i32::from_be_bytes(frame[8..12].try_into().map_err(|_| Error::Invalid)?),
        client_id: None,
    })
}

/// Decodes one length-prefixed Kafka request frame.
///
/// # Errors
///
/// Returns [`Error::UnsupportedApi`] or [`Error::UnsupportedVersion`] for
/// anything outside the documented subset, and structural errors otherwise.
pub fn decode_request(frame: &[u8]) -> Result<Request, Error> {
    if frame.len() < 4 {
        return Err(Error::Incomplete);
    }
    let declared = i32::from_be_bytes(frame[..4].try_into().map_err(|_| Error::Invalid)?);
    if declared < 0 {
        return Err(Error::Invalid);
    }
    if declared as usize > MAX_FRAME_LEN {
        return Err(Error::TooLarge);
    }
    if declared as usize + 4 != frame.len() {
        return Err(Error::Incomplete);
    }

    let mut decoder = Decoder::new(&frame[4..]);
    let api_key = decoder.i16()?;
    let api_version = decoder.i16()?;
    let correlation_id = decoder.i32()?;
    let client_id = decoder.nullable_string_bytes()?.map(Vec::from);
    if !protocol::is_supported_api(api_key) {
        return Err(Error::UnsupportedApi);
    }

    let body = match (api_key, api_version) {
        (api::API_VERSIONS, version::API_VERSIONS) => RequestBody::ApiVersions,
        (api::FIND_COORDINATOR, version::FIND_COORDINATOR) => {
            decode_find_coordinator(&mut decoder)?
        }
        (api::INIT_PRODUCER_ID, version::INIT_PRODUCER_ID) => {
            decode_init_producer_id(&mut decoder)?
        }
        (api::ADD_PARTITIONS_TO_TXN, version::ADD_PARTITIONS_TO_TXN) => {
            decode_add_partitions_to_txn(&mut decoder)?
        }
        (api::END_TXN, version::END_TXN) => decode_end_txn(&mut decoder)?,
        (api::JOIN_GROUP, version::JOIN_GROUP) => decode_join_group(&mut decoder)?,
        (api::SYNC_GROUP, version::SYNC_GROUP) => decode_sync_group(&mut decoder)?,
        (api::HEARTBEAT, version::HEARTBEAT) => decode_heartbeat(&mut decoder)?,
        (api::LEAVE_GROUP, version::LEAVE_GROUP) => decode_leave_group(&mut decoder)?,
        (api::OFFSET_COMMIT, version::OFFSET_COMMIT) => decode_offset_commit(&mut decoder)?,
        (api::OFFSET_FETCH, version::OFFSET_FETCH) => decode_offset_fetch(&mut decoder)?,
        (api::METADATA, version::METADATA) => decode_metadata(&mut decoder)?,
        (api::PRODUCE, version::PRODUCE) => decode_produce(&mut decoder)?,
        (api::FETCH, version::FETCH) => decode_fetch(&mut decoder)?,
        (api::LIST_OFFSETS, version::LIST_OFFSETS) => decode_list_offsets(&mut decoder)?,
        _ => return Err(Error::UnsupportedVersion),
    };
    if !decoder.done() {
        return Err(Error::Invalid);
    }

    Ok(Request {
        header: Header {
            api_key,
            api_version,
            correlation_id,
            client_id,
        },
        body,
    })
}

fn decode_find_coordinator(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    let key = decoder.string_bytes()?.to_vec();
    if key.is_empty() {
        return Err(Error::Invalid);
    }
    let key_type = decoder.i8()?;
    if key_type != 0 && key_type != 1 {
        return Err(Error::Invalid);
    }
    Ok(RequestBody::FindCoordinator {
        key,
        key_type,
    })
}

fn decode_init_producer_id(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    Ok(RequestBody::InitProducerId {
        transactional_id: decoder.nullable_string_bytes()?.map(Vec::from),
        timeout_ms: decoder.i32()?,
    })
}

fn decode_add_partitions_to_txn(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    let transactional_id = decoder.string_bytes()?.to_vec();
    let producer_id = decoder.i64()?;
    let producer_epoch = decoder.i16()?;
    let topic_count = decoder.array_len()?;
    let mut topics = Vec::with_capacity(topic_count.min(decoder.remaining()));
    for _ in 0..topic_count {
        let topic = decoder.string_bytes()?.to_vec();
        let count = decoder.array_len()?;
        let mut partitions = Vec::with_capacity(count.min(decoder.remaining()));
        for _ in 0..count {
            let partition = decoder.i32()?;
            if partition < 0 {
                return Err(Error::Invalid);
            }
            partitions.push(partition);
        }
        topics.push((topic, partitions));
    }
    Ok(RequestBody::AddPartitionsToTxn {
        transactional_id,
        producer_id,
        producer_epoch,
        topics,
    })
}

fn decode_end_txn(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    let transactional_id = decoder.string_bytes()?.to_vec();
    let producer_id = decoder.i64()?;
    let producer_epoch = decoder.i16()?;
    let commit = decoder.i8()?;
    if commit != 0 && commit != 1 {
        return Err(Error::Invalid);
    }
    Ok(RequestBody::EndTxn {
        transactional_id,
        producer_id,
        producer_epoch,
        commit: commit != 0,
    })
}

fn decode_join_group(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    let group_id = decoder.string_bytes()?.to_vec();
    let session_timeout = decoder.i32()?;
    let rebalance_timeout = decoder.i32()?;
    if session_timeout < 0 || rebalance_timeout < 0 {
        return Err(Error::Invalid);
    }
    let member_id = decoder.string_bytes()?.to_vec();
    let _protocol_type = decoder.string_bytes()?;
    let protocols = decoder.array_len()?;
    if protocols == 0 {
        return Err(Error::Invalid);
    }
    let _name = decoder.string_bytes()?;
    let metadata = decoder.bytes()?.ok_or(Error::Invalid)?;
    let mut metadata_decoder = Decoder::new(metadata);
    let _version = metadata_decoder.i16()?;
    let topic_count = metadata_decoder.array_len()?;
    let mut subscriptions = Vec::with_capacity(topic_count.min(metadata_decoder.remaining()));
    for _ in 0..topic_count {
        subscriptions.push(metadata_decoder.string_bytes()?.to_vec());
    }
    let _ = metadata_decoder.bytes()?;
    for _ in 1..protocols {
        let _ = decoder.string_bytes()?;
        let _ = decoder.bytes()?.ok_or(Error::Invalid)?;
    }
    Ok(RequestBody::JoinGroup {
        group_id,
        member_id,
        subscriptions,
    })
}

fn decode_sync_group(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    let group_id = decoder.string_bytes()?.to_vec();
    let generation = decoder.i32()?;
    let member_id = decoder.string_bytes()?.to_vec();
    let assignments = decoder.array_len()?;
    let mut assignment = Vec::new();
    for _ in 0..assignments {
        let _member = decoder.string_bytes()?;
        let bytes = decoder.bytes()?.ok_or(Error::Invalid)?;
        if assignment.is_empty() {
            assignment = bytes.to_vec();
        }
    }
    Ok(RequestBody::SyncGroup {
        group_id,
        generation,
        member_id,
        assignment,
    })
}

fn decode_heartbeat(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    Ok(RequestBody::Heartbeat {
        group_id: decoder.string_bytes()?.to_vec(),
        generation: decoder.i32()?,
        member_id: decoder.string_bytes()?.to_vec(),
    })
}

fn decode_leave_group(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    Ok(RequestBody::LeaveGroup {
        group_id: decoder.string_bytes()?.to_vec(),
        member_id: decoder.string_bytes()?.to_vec(),
    })
}

fn decode_offset_commit(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    let group_id = decoder.string_bytes()?.to_vec();
    let generation = decoder.i32()?;
    let member_id = decoder.string_bytes()?.to_vec();
    let _retention = decoder.i64()?;
    let topics = decoder.array_len()?;
    if topics != 1 {
        return Err(Error::Invalid);
    }
    let topic = decoder.string_bytes()?.to_vec();
    let partitions = decoder.array_len()?;
    if partitions != 1 {
        return Err(Error::Invalid);
    }
    let partition = decoder.i32()?;
    let offset = decoder.i64()?;
    let _metadata = decoder.nullable_string_bytes()?;
    Ok(RequestBody::OffsetCommit {
        group_id,
        generation,
        member_id,
        topic,
        partition,
        offset,
    })
}

fn decode_offset_fetch(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    let group_id = decoder.string_bytes()?.to_vec();
    let topics = decoder.array_len()?;
    if topics != 1 {
        return Err(Error::Invalid);
    }
    let topic = decoder.string_bytes()?.to_vec();
    let partitions = decoder.array_len()?;
    if partitions != 1 {
        return Err(Error::Invalid);
    }
    Ok(RequestBody::OffsetFetch {
        group_id,
        topic,
        partition: decoder.i32()?,
    })
}

fn decode_metadata(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    let topics = match decoder.nullable_array_len()? {
        None => None,
        Some(count) => {
            let mut topics = Vec::with_capacity(count.min(decoder.remaining()));
            for _ in 0..count {
                let name = decoder.string_bytes()?;
                if name.is_empty() {
                    return Err(Error::Invalid);
                }
                topics.push(Vec::from(name));
            }
            Some(topics)
        }
    };
    Ok(RequestBody::Metadata {
        topics,
    })
}

fn decode_produce(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    let transactional_id = decoder.nullable_string_bytes()?.map(Vec::from);
    let acks = decoder.i16()?;
    let timeout_ms = decoder.i32()?;
    let topic_count = decoder.array_len()?;
    let mut topics = Vec::with_capacity(topic_count.min(decoder.remaining()));
    for _ in 0..topic_count {
        let name = decoder.string_bytes()?;
        if name.is_empty() {
            return Err(Error::Invalid);
        }
        let partition_count = decoder.array_len()?;
        let mut partitions = Vec::with_capacity(partition_count.min(decoder.remaining()));
        for _ in 0..partition_count {
            let partition = decoder.i32()?;
            let record_set = decoder.bytes()?.ok_or(Error::Invalid)?;
            let decoded = decode_record_batches_with_identity(record_set)?;
            let records = decoded.records;
            let producer = decoded.producer;
            if records.is_empty() {
                return Err(Error::Invalid);
            }
            partitions.push(ProducePartition {
                partition,
                records,
                producer,
            });
        }
        topics.push(ProduceTopic {
            name: Vec::from(name),
            partitions,
        });
    }
    Ok(RequestBody::Produce {
        transactional_id,
        acks,
        timeout_ms,
        topics,
    })
}

fn decode_fetch(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    let replica_id = decoder.i32()?;
    let max_wait_ms = decoder.i32()?;
    let min_bytes = decoder.i32()?;
    let max_bytes = decoder.i32()?;
    if max_wait_ms < 0 || min_bytes < 0 || max_bytes < 0 {
        return Err(Error::Invalid);
    }
    let isolation = decoder.i8()?;
    if isolation != 0 && isolation != 1 {
        return Err(Error::Invalid);
    }
    let topic_count = decoder.array_len()?;
    let mut topics = Vec::with_capacity(topic_count.min(decoder.remaining()));
    for _ in 0..topic_count {
        let name = decoder.string_bytes()?;
        if name.is_empty() {
            return Err(Error::Invalid);
        }
        let partition_count = decoder.array_len()?;
        let mut partitions = Vec::with_capacity(partition_count.min(decoder.remaining()));
        for _ in 0..partition_count {
            let partition = decoder.i32()?;
            let fetch_offset = decoder.i64()?;
            let partition_max_bytes = decoder.i32()?;
            if partition_max_bytes < 0 {
                return Err(Error::Invalid);
            }
            partitions.push(FetchPartition {
                partition,
                fetch_offset,
                partition_max_bytes,
            });
        }
        topics.push(FetchTopic {
            name: Vec::from(name),
            partitions,
        });
    }
    Ok(RequestBody::Fetch {
        replica_id,
        max_wait_ms,
        min_bytes,
        max_bytes,
        read_committed: isolation == 1,
        topics,
    })
}

fn decode_list_offsets(decoder: &mut Decoder<'_>) -> Result<RequestBody, Error> {
    let replica_id = decoder.i32()?;
    let topic_count = decoder.array_len()?;
    let mut topics = Vec::with_capacity(topic_count.min(decoder.remaining()));
    for _ in 0..topic_count {
        let name = decoder.string_bytes()?;
        if name.is_empty() {
            return Err(Error::Invalid);
        }
        let partition_count = decoder.array_len()?;
        let mut partitions = Vec::with_capacity(partition_count.min(decoder.remaining()));
        for _ in 0..partition_count {
            let partition = decoder.i32()?;
            let timestamp = decoder.i64()?;
            partitions.push(ListOffsetsPartition {
                partition,
                timestamp,
            });
        }
        topics.push(ListOffsetsTopic {
            name: Vec::from(name),
            partitions,
        });
    }
    Ok(RequestBody::ListOffsets {
        replica_id,
        topics,
    })
}

#[cfg(test)]
mod tests {
    use broker_core::Record;

    use super::*;

    fn frame(
        api_key: i16,
        api_version: i16,
        correlation: i32,
        client_id: &[u8],
        body: &[u8],
    ) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&0i32.to_be_bytes());
        frame.extend_from_slice(&api_key.to_be_bytes());
        frame.extend_from_slice(&api_version.to_be_bytes());
        frame.extend_from_slice(&correlation.to_be_bytes());
        frame.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
        frame.extend_from_slice(client_id);
        frame.extend_from_slice(body);
        let payload = (frame.len() - 4) as i32;
        frame[..4].copy_from_slice(&payload.to_be_bytes());
        frame
    }

    fn metadata_body(topics: &[&[u8]]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&(topics.len() as i32).to_be_bytes());
        for topic in topics {
            body.extend_from_slice(&(topic.len() as i16).to_be_bytes());
            body.extend_from_slice(topic);
        }
        body
    }

    #[test]
    fn decodes_metadata_request_header_and_topics() {
        let frame = frame(
            api::METADATA,
            version::METADATA,
            0x0102_0304,
            b"charlotte",
            &metadata_body(&[b"events", b"results"]),
        );
        let request = decode_request(&frame).expect("decode");
        assert_eq!(request.header.api_key, api::METADATA);
        assert_eq!(request.header.correlation_id, 0x0102_0304);
        assert_eq!(request.header.client_id.as_deref(), Some(b"charlotte".as_slice()));
        match request.body {
            RequestBody::Metadata {
                topics: Some(topics),
            } => {
                assert_eq!(topics, [b"events".to_vec(), b"results".to_vec()]);
            }
            other => panic!("unexpected body: {other:?}"),
        }
    }

    #[test]
    fn decodes_api_versions_request_with_empty_body() {
        let frame = frame(api::API_VERSIONS, version::API_VERSIONS, 9, b"client", &[]);
        let request = decode_request(&frame).expect("decode");
        assert_eq!(request.body, RequestBody::ApiVersions);
    }

    #[test]
    fn decodes_produce_request_with_record_batch() {
        let batch = crate::record_batch::encode_record_batch(
            0,
            &[
                Record {
                    offset: 0,
                    timestamp_ms: 1_000,
                    key: Some(b"k".to_vec()),
                    value: Some(b"first".to_vec()),
                },
                Record {
                    offset: 1,
                    timestamp_ms: 1_005,
                    key: None,
                    value: Some(b"second".to_vec()),
                },
            ],
        )
        .expect("encode batch");

        let mut body = Vec::new();
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&30_000i32.to_be_bytes());
        body.extend_from_slice(&1i32.to_be_bytes());
        body.extend_from_slice(&6i16.to_be_bytes());
        body.extend_from_slice(b"events");
        body.extend_from_slice(&1i32.to_be_bytes());
        body.extend_from_slice(&2i32.to_be_bytes());
        body.extend_from_slice(&(batch.len() as i32).to_be_bytes());
        body.extend_from_slice(&batch);

        let frame = frame(api::PRODUCE, version::PRODUCE, 11, b"client", &body);
        let request = decode_request(&frame).expect("decode");
        match request.body {
            RequestBody::Produce {
                transactional_id,
                acks,
                topics,
                ..
            } => {
                assert!(transactional_id.is_none());
                assert_eq!(acks, -1);
                assert_eq!(topics.len(), 1);
                assert_eq!(topics[0].name, b"events");
                assert_eq!(topics[0].partitions.len(), 1);
                assert_eq!(topics[0].partitions[0].partition, 2);
                let records = &topics[0].partitions[0].records;
                assert_eq!(records.len(), 2);
                assert_eq!(records[0].key.as_deref(), Some(b"k".as_slice()));
                assert_eq!(records[1].timestamp_ms, 1_005);
            }
            other => panic!("unexpected body: {other:?}"),
        }
    }

    #[test]
    fn rejects_unsupported_api_and_version() {
        let request = frame(99, 0, 1, b"client", &[]);
        assert_eq!(decode_request(&request), Err(Error::UnsupportedApi));
        let request = frame(api::PRODUCE, 99, 1, b"client", &[]);
        assert_eq!(decode_request(&request), Err(Error::UnsupportedVersion));
    }

    #[test]
    fn rejects_truncated_and_trailing_frames() {
        let mut request = frame(api::API_VERSIONS, version::API_VERSIONS, 1, b"client", &[]);
        request.truncate(request.len() - 1);
        assert_eq!(decode_request(&request), Err(Error::Incomplete));

        let request = frame(api::API_VERSIONS, version::API_VERSIONS, 1, b"client", &[0]);
        assert_eq!(decode_request(&request), Err(Error::Invalid));
    }
}

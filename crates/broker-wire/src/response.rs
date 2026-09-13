//! Response encoding for the supported Kafka subset.

use alloc::vec::Vec;

use broker_core::Record;

use crate::{
    codec::Encoder,
    protocol::{
        Error,
        NO_ERROR,
    },
    record_batch::encode_record_batch,
};

/// One broker in a metadata response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerMetadata {
    /// Broker node id.
    pub node_id: i32,
    /// Advertised host.
    pub host: Vec<u8>,
    /// Advertised port.
    pub port: i32,
}

/// One partition in a metadata response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionMetadata {
    /// Per-partition error code.
    pub error: i16,
    /// Partition index.
    pub partition: i32,
    /// Leader node id.
    pub leader: i32,
    /// Replica node ids.
    pub replicas: Vec<i32>,
    /// In-sync replica node ids.
    pub isr: Vec<i32>,
}

/// One topic in a metadata response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicMetadata {
    /// Per-topic error code.
    pub error: i16,
    /// Topic name.
    pub name: Vec<u8>,
    /// Whether the topic is internal.
    pub is_internal: bool,
    /// Partitions of this topic.
    pub partitions: Vec<PartitionMetadata>,
}

/// A complete metadata response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataResponse {
    /// Cluster brokers.
    pub brokers: Vec<BrokerMetadata>,
    /// Controller node id.
    pub controller_id: i32,
    /// Topic metadata.
    pub topics: Vec<TopicMetadata>,
}

/// One produce result partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducePartitionResult {
    /// Partition index.
    pub partition: i32,
    /// Per-partition error code.
    pub error: i16,
    /// First offset assigned to the appended batch.
    pub base_offset: i64,
    /// Broker append time, or `-1` when disabled.
    pub log_append_time_ms: i64,
}

/// One produce result topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProduceTopicResult {
    /// Topic name.
    pub name: Vec<u8>,
    /// Per-partition results.
    pub partitions: Vec<ProducePartitionResult>,
}

/// A complete produce response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProduceResponse {
    /// Per-topic results.
    pub topics: Vec<ProduceTopicResult>,
    /// Broker throttle time.
    pub throttle_time_ms: i32,
}

/// One fetch result partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchPartitionResult {
    /// Partition index.
    pub partition: i32,
    /// Per-partition error code.
    pub error: i16,
    /// Next offset after the returned records.
    pub high_watermark: i64,
    /// Last stable offset; equal to the high watermark without transactions.
    pub last_stable_offset: i64,
    /// Aborted producer ranges, or `None` when the broker sends null.
    pub aborted_transactions: Option<Vec<(i64, i64)>>,
    /// Records beginning at the requested offset.
    pub records: Vec<Record>,
}

/// One fetch result topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchTopicResult {
    /// Topic name.
    pub name: Vec<u8>,
    /// Per-partition results.
    pub partitions: Vec<FetchPartitionResult>,
}

/// A complete fetch response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchResponse {
    /// Broker throttle time.
    pub throttle_time_ms: i32,
    /// Per-topic results.
    pub topics: Vec<FetchTopicResult>,
}

/// One list-offsets result partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListOffsetsPartitionResult {
    /// Partition index.
    pub partition: i32,
    /// Per-partition error code.
    pub error: i16,
    /// Timestamp associated with the returned offset.
    pub timestamp_ms: i64,
    /// The earliest or latest offset.
    pub offset: i64,
}

/// One list-offsets result topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListOffsetsTopicResult {
    /// Topic name.
    pub name: Vec<u8>,
    /// Per-partition results.
    pub partitions: Vec<ListOffsetsPartitionResult>,
}

/// A complete list-offsets response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListOffsetsResponse {
    /// Per-topic results.
    pub topics: Vec<ListOffsetsTopicResult>,
}

/// Encodes an ApiVersions v0 response for `(api_key, min, max)` entries.
pub fn encode_api_versions(
    correlation_id: i32,
    versions: &[(i16, i16, i16)],
) -> Result<Vec<u8>, Error> {
    let mut encoder = Encoder::response(correlation_id);
    encoder.i16(NO_ERROR);
    encoder.array_len(versions.len())?;
    for (api_key, min, max) in versions {
        encoder.i16(*api_key);
        encoder.i16(*min);
        encoder.i16(*max);
    }
    Ok(encoder.finish())
}

/// Encodes a Metadata v1 response.
pub fn encode_metadata(correlation_id: i32, response: &MetadataResponse) -> Result<Vec<u8>, Error> {
    let mut encoder = Encoder::response(correlation_id);
    encoder.array_len(response.brokers.len())?;
    for broker in &response.brokers {
        encoder.i32(broker.node_id);
        encoder.string(&broker.host)?;
        encoder.i32(broker.port);
        encoder.nullable_string(None)?;
    }
    encoder.i32(response.controller_id);
    encoder.array_len(response.topics.len())?;
    for topic in &response.topics {
        encoder.i16(topic.error);
        encoder.string(&topic.name)?;
        encoder.bool(topic.is_internal);
        encoder.array_len(topic.partitions.len())?;
        for partition in &topic.partitions {
            encoder.i16(partition.error);
            encoder.i32(partition.partition);
            encoder.i32(partition.leader);
            encoder.array_len(partition.replicas.len())?;
            for replica in &partition.replicas {
                encoder.i32(*replica);
            }
            encoder.array_len(partition.isr.len())?;
            for replica in &partition.isr {
                encoder.i32(*replica);
            }
        }
    }
    Ok(encoder.finish())
}

/// Encodes a Produce v3 response.
pub fn encode_produce(correlation_id: i32, response: &ProduceResponse) -> Result<Vec<u8>, Error> {
    let mut encoder = Encoder::response(correlation_id);
    encoder.array_len(response.topics.len())?;
    for topic in &response.topics {
        encoder.string(&topic.name)?;
        encoder.array_len(topic.partitions.len())?;
        for partition in &topic.partitions {
            encoder.i32(partition.partition);
            encoder.i16(partition.error);
            encoder.i64(partition.base_offset);
            encoder.i64(partition.log_append_time_ms);
        }
    }
    encoder.i32(response.throttle_time_ms);
    Ok(encoder.finish())
}

/// Encodes a Fetch v4 response, re-encoding stored records as record batch v2.
pub fn encode_fetch(correlation_id: i32, response: &FetchResponse) -> Result<Vec<u8>, Error> {
    let mut encoder = Encoder::response(correlation_id);
    encoder.i32(response.throttle_time_ms);
    encoder.array_len(response.topics.len())?;
    for topic in &response.topics {
        encoder.string(&topic.name)?;
        encoder.array_len(topic.partitions.len())?;
        for partition in &topic.partitions {
            encoder.i32(partition.partition);
            encoder.i16(partition.error);
            encoder.i64(partition.high_watermark);
            encoder.i64(partition.last_stable_offset);
            match &partition.aborted_transactions {
                Some(aborted) => {
                    encoder.array_len(aborted.len())?;
                    for (producer_id, first_offset) in aborted {
                        encoder.i64(*producer_id);
                        encoder.i64(*first_offset);
                    }
                }
                None => encoder.nullable_array_len(None)?,
            }
            if partition.records.is_empty() {
                encoder.nullable_bytes(None)?;
            } else {
                let base_offset = partition.records[0].offset;
                let batch = encode_record_batch(base_offset, &partition.records)?;
                encoder.bytes(&batch)?;
            }
        }
    }
    Ok(encoder.finish())
}

/// Encodes a ListOffsets v1 response.
pub fn encode_list_offsets(
    correlation_id: i32,
    response: &ListOffsetsResponse,
) -> Result<Vec<u8>, Error> {
    let mut encoder = Encoder::response(correlation_id);
    encoder.array_len(response.topics.len())?;
    for topic in &response.topics {
        encoder.string(&topic.name)?;
        encoder.array_len(topic.partitions.len())?;
        for partition in &topic.partitions {
            encoder.i32(partition.partition);
            encoder.i16(partition.error);
            encoder.i64(partition.timestamp_ms);
            encoder.i64(partition.offset);
        }
    }
    Ok(encoder.finish())
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use super::*;
    use crate::{
        codec::Decoder,
        protocol::{
            SUPPORTED_VERSIONS,
            api,
            version,
        },
        record_batch::decode_record_batches,
    };

    fn response_body<'a>(frame: &'a [u8], correlation_id: i32) -> Decoder<'a> {
        let declared = i32::from_be_bytes(frame[..4].try_into().expect("length"));
        assert_eq!(declared as usize, frame.len() - 4);
        let correlation = i32::from_be_bytes(frame[4..8].try_into().expect("correlation"));
        assert_eq!(correlation, correlation_id);
        Decoder::new(&frame[8..])
    }

    #[test]
    fn api_versions_response_lists_the_subset() {
        let frame = encode_api_versions(7, SUPPORTED_VERSIONS).expect("encode");
        let mut body = response_body(&frame, 7);
        assert_eq!(body.i16(), Ok(NO_ERROR));
        let count = body.array_len().expect("count");
        assert_eq!(count, SUPPORTED_VERSIONS.len());
        for (api_key, min, max) in SUPPORTED_VERSIONS {
            assert_eq!(body.i16(), Ok(*api_key));
            assert_eq!(body.i16(), Ok(*min));
            assert_eq!(body.i16(), Ok(*max));
        }
        assert!(body.done());
    }

    #[test]
    fn metadata_response_round_trips() {
        let response = MetadataResponse {
            brokers: vec![BrokerMetadata {
                node_id: 1,
                host: b"node-1.test".to_vec(),
                port: 9092,
            }],
            controller_id: 1,
            topics: vec![TopicMetadata {
                error: NO_ERROR,
                name: b"events".to_vec(),
                is_internal: false,
                partitions: vec![PartitionMetadata {
                    error: NO_ERROR,
                    partition: 0,
                    leader: 1,
                    replicas: vec![1],
                    isr: vec![1],
                }],
            }],
        };
        let frame = encode_metadata(21, &response).expect("encode");
        let mut body = response_body(&frame, 21);
        assert_eq!(body.array_len(), Ok(1));
        assert_eq!(body.i32(), Ok(1));
        assert_eq!(body.string_bytes(), Ok(b"node-1.test".as_slice()));
        assert_eq!(body.i32(), Ok(9092));
        assert_eq!(body.nullable_string_bytes(), Ok(None));
        assert_eq!(body.i32(), Ok(1));
        assert_eq!(body.array_len(), Ok(1));
        assert_eq!(body.i16(), Ok(NO_ERROR));
        assert_eq!(body.string_bytes(), Ok(b"events".as_slice()));
        assert_eq!(body.i8(), Ok(0));
        assert_eq!(body.array_len(), Ok(1));
        assert_eq!(body.i16(), Ok(NO_ERROR));
        assert_eq!(body.i32(), Ok(0));
        assert_eq!(body.i32(), Ok(1));
        assert_eq!(body.array_len(), Ok(1));
        assert_eq!(body.i32(), Ok(1));
        assert_eq!(body.array_len(), Ok(1));
        assert_eq!(body.i32(), Ok(1));
        assert!(body.done());
    }

    #[test]
    fn produce_response_round_trips() {
        let response = ProduceResponse {
            topics: vec![ProduceTopicResult {
                name: b"events".to_vec(),
                partitions: vec![ProducePartitionResult {
                    partition: 2,
                    error: NO_ERROR,
                    base_offset: 41,
                    log_append_time_ms: -1,
                }],
            }],
            throttle_time_ms: 0,
        };
        let frame = encode_produce(3, &response).expect("encode");
        let mut body = response_body(&frame, 3);
        assert_eq!(body.array_len(), Ok(1));
        assert_eq!(body.string_bytes(), Ok(b"events".as_slice()));
        assert_eq!(body.array_len(), Ok(1));
        assert_eq!(body.i32(), Ok(2));
        assert_eq!(body.i16(), Ok(NO_ERROR));
        assert_eq!(body.i64(), Ok(41));
        assert_eq!(body.i64(), Ok(-1));
        assert_eq!(body.i32(), Ok(0));
        assert!(body.done());
    }

    #[test]
    fn fetch_response_carries_record_batches() {
        let records = vec![
            Record {
                offset: 5,
                timestamp_ms: 1_000,
                key: Some(b"k".to_vec()),
                value: Some(b"first".to_vec()),
            },
            Record {
                offset: 6,
                timestamp_ms: 1_001,
                key: None,
                value: Some(b"second".to_vec()),
            },
        ];
        let response = FetchResponse {
            throttle_time_ms: 0,
            topics: vec![FetchTopicResult {
                name: b"events".to_vec(),
                partitions: vec![FetchPartitionResult {
                    partition: 0,
                    error: NO_ERROR,
                    high_watermark: 7,
                    last_stable_offset: 7,
                    aborted_transactions: None,
                    records: records.clone(),
                }],
            }],
        };
        let frame = encode_fetch(4, &response).expect("encode");
        let mut body = response_body(&frame, 4);
        assert_eq!(body.i32(), Ok(0));
        assert_eq!(body.array_len(), Ok(1));
        assert_eq!(body.string_bytes(), Ok(b"events".as_slice()));
        assert_eq!(body.array_len(), Ok(1));
        assert_eq!(body.i32(), Ok(0));
        assert_eq!(body.i16(), Ok(NO_ERROR));
        assert_eq!(body.i64(), Ok(7));
        assert_eq!(body.i64(), Ok(7));
        assert_eq!(body.nullable_array_len(), Ok(None));
        let set = body.bytes().expect("bytes").expect("record set");
        assert_eq!(
            decode_record_batches(set).expect("decode"),
            records.into_iter().map(Into::into).collect::<Vec<_>>()
        );
        assert!(body.done());
    }

    #[test]
    fn list_offsets_response_round_trips() {
        let response = ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResult {
                name: b"events".to_vec(),
                partitions: vec![ListOffsetsPartitionResult {
                    partition: 1,
                    error: NO_ERROR,
                    timestamp_ms: -1,
                    offset: 9,
                }],
            }],
        };
        let frame = encode_list_offsets(5, &response).expect("encode");
        let mut body = response_body(&frame, 5);
        assert_eq!(body.array_len(), Ok(1));
        assert_eq!(body.string_bytes(), Ok(b"events".as_slice()));
        assert_eq!(body.array_len(), Ok(1));
        assert_eq!(body.i32(), Ok(1));
        assert_eq!(body.i16(), Ok(NO_ERROR));
        assert_eq!(body.i64(), Ok(-1));
        assert_eq!(body.i64(), Ok(9));
        assert!(body.done());
    }

    #[test]
    fn api_versions_advertises_only_the_implemented_versions() {
        for (api_key, min, max) in SUPPORTED_VERSIONS {
            assert_eq!(min, max);
            assert!(matches!(
                *api_key,
                api::PRODUCE | api::FETCH | api::LIST_OFFSETS | api::METADATA | api::API_VERSIONS
            ));
            assert_eq!(
                *min,
                match *api_key {
                    api::PRODUCE => version::PRODUCE,
                    api::FETCH => version::FETCH,
                    api::LIST_OFFSETS => version::LIST_OFFSETS,
                    api::METADATA => version::METADATA,
                    api::API_VERSIONS => version::API_VERSIONS,
                    _ => unreachable!(),
                }
            );
        }
    }
}

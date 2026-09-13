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
    record_batch::decode_record_batches,
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
            let records = decode_record_batches(record_set)?;
            if records.is_empty() {
                return Err(Error::Invalid);
            }
            partitions.push(ProducePartition {
                partition,
                records,
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

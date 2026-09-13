//! Request dispatch and Kafka error mapping.
//!
//! The engine is the only layer that knows both the wire subset and the shard
//! runtime. Each supported request maps to one response; per-item failures
//! become Kafka error codes in the response instead of failing the whole
//! connection.

use alloc::{
    vec,
    vec::Vec,
};

use broker_core::LogError;
use broker_runtime::{
    Broker,
    BrokerError,
};
use broker_wire::{
    BrokerMetadata,
    Error,
    FetchPartitionResult,
    FetchResponse,
    FetchTopicResult,
    ListOffsetsPartitionResult,
    ListOffsetsResponse,
    ListOffsetsTopicResult,
    MetadataResponse,
    PartitionMetadata,
    ProducePartitionResult,
    ProduceResponse,
    ProduceTopicResult,
    Request,
    RequestBody,
    TopicMetadata,
    decode_request,
    encode_api_versions,
    encode_api_versions_error,
    encode_fetch,
    encode_list_offsets,
    encode_metadata,
    encode_produce,
    peek_header,
    protocol::{
        NO_ERROR,
        OFFSET_OUT_OF_RANGE,
        SUPPORTED_VERSIONS,
        UNKNOWN_SERVER_ERROR,
        UNKNOWN_TOPIC_OR_PARTITION,
        UNSUPPORTED_VERSION,
        api,
    },
};

/// Default record cap for one fetch partition.
pub const DEFAULT_MAX_FETCH_RECORDS: usize = 1024;

/// Default byte cap for one fetch partition.
pub const DEFAULT_MAX_FETCH_BYTES: usize = 1024 * 1024;

/// How the broker identifies itself in metadata responses.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerIdentity {
    /// Broker node id.
    pub node_id: i32,
    /// Advertised host.
    pub host: Vec<u8>,
    /// Advertised port.
    pub port: i32,
}

impl BrokerIdentity {
    /// Creates an identity from an advertised address.
    pub fn new(node_id: i32, host: &[u8], port: i32) -> Self {
        Self {
            node_id,
            host: Vec::from(host),
            port,
        }
    }
}

/// Engine tuning and identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineConfig {
    /// Identity reported in metadata.
    pub identity: BrokerIdentity,
    /// Maximum records returned per fetch partition.
    pub max_fetch_records: usize,
    /// Maximum approximate bytes returned per fetch partition.
    pub max_fetch_bytes: usize,
}

impl EngineConfig {
    /// Creates a configuration with the default fetch bounds.
    pub fn new(identity: BrokerIdentity) -> Self {
        Self {
            identity,
            max_fetch_records: DEFAULT_MAX_FETCH_RECORDS,
            max_fetch_bytes: DEFAULT_MAX_FETCH_BYTES,
        }
    }
}

/// Dispatches decoded requests against a broker.
pub struct Engine {
    broker: Broker,
    identity: BrokerIdentity,
    max_fetch_records: usize,
    max_fetch_bytes: usize,
}

impl Engine {
    /// Creates an engine over an already started broker.
    pub fn new(broker: Broker, config: EngineConfig) -> Self {
        Self {
            broker,
            identity: config.identity,
            max_fetch_records: config.max_fetch_records,
            max_fetch_bytes: config.max_fetch_bytes,
        }
    }

    /// The broker behind this engine.
    pub fn broker(&self) -> &Broker {
        &self.broker
    }

    /// Decodes and dispatches one length-prefixed request frame.
    ///
    /// # Errors
    ///
    /// Returns the decode or encode error. A malformed frame has no usable
    /// correlation id, so the caller is expected to close the connection.
    pub fn handle_frame(&self, frame: &[u8]) -> Result<Vec<u8>, Error> {
        match decode_request(frame) {
            Ok(request) => self.handle_request(request),
            Err(Error::UnsupportedVersion) => {
                // Kafka's ApiVersions downgrade: answer a known API key using
                // a version we do not implement with a v0 error body so the
                // client can retry with the advertised version.
                if let Ok(header) = peek_header(frame)
                    && header.api_key == api::API_VERSIONS
                {
                    return encode_api_versions_error(
                        header.correlation_id,
                        UNSUPPORTED_VERSION,
                        SUPPORTED_VERSIONS,
                    );
                }
                Err(Error::UnsupportedVersion)
            }
            Err(error) => Err(error),
        }
    }

    /// Dispatches one decoded request.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] only when the response cannot be encoded; per-item
    /// broker failures are reported as Kafka error codes inside the response.
    pub fn handle_request(&self, request: Request) -> Result<Vec<u8>, Error> {
        let correlation_id = request.header.correlation_id;
        match request.body {
            RequestBody::ApiVersions => encode_api_versions(correlation_id, SUPPORTED_VERSIONS),
            RequestBody::Metadata {
                topics,
            } => self.metadata(correlation_id, topics),
            RequestBody::Produce {
                transactional_id,
                topics,
                ..
            } => self.produce(correlation_id, transactional_id.is_some(), topics),
            RequestBody::Fetch {
                max_bytes,
                topics,
                ..
            } => self.fetch(correlation_id, max_bytes, topics),
            RequestBody::ListOffsets {
                topics,
                ..
            } => self.list_offsets(correlation_id, topics),
        }
    }

    fn metadata(
        &self,
        correlation_id: i32,
        topics: Option<Vec<Vec<u8>>>,
    ) -> Result<Vec<u8>, Error> {
        let response_topics = match topics {
            None => self
                .broker
                .metadata()
                .into_iter()
                .map(|topic| TopicMetadata {
                    error: NO_ERROR,
                    name: topic.name,
                    is_internal: false,
                    partitions: self.partitions(topic.partitions),
                })
                .collect(),
            Some(names) => names
                .into_iter()
                .map(|name| match self.broker.partition_count(&name) {
                    Some(partitions) => TopicMetadata {
                        error: NO_ERROR,
                        name,
                        is_internal: false,
                        partitions: self.partitions(partitions),
                    },
                    None => TopicMetadata {
                        error: UNKNOWN_TOPIC_OR_PARTITION,
                        name,
                        is_internal: false,
                        partitions: Vec::new(),
                    },
                })
                .collect(),
        };

        encode_metadata(
            correlation_id,
            &MetadataResponse {
                brokers: vec![BrokerMetadata {
                    node_id: self.identity.node_id,
                    host: self.identity.host.clone(),
                    port: self.identity.port,
                }],
                controller_id: self.identity.node_id,
                topics: response_topics,
            },
        )
    }

    fn produce(
        &self,
        correlation_id: i32,
        transactional: bool,
        topics: Vec<broker_wire::ProduceTopic>,
    ) -> Result<Vec<u8>, Error> {
        let response_topics = topics
            .into_iter()
            .map(|topic| {
                let broker_wire::ProduceTopic {
                    name,
                    partitions,
                } = topic;
                let partitions = partitions
                    .into_iter()
                    .map(|partition| {
                        if transactional {
                            return ProducePartitionResult {
                                partition: partition.partition,
                                error: UNSUPPORTED_VERSION,
                                base_offset: -1,
                                log_append_time_ms: -1,
                            };
                        }
                        match self.broker.produce(&name, partition.partition, partition.records) {
                            Ok(base_offset) => ProducePartitionResult {
                                partition: partition.partition,
                                error: NO_ERROR,
                                base_offset,
                                log_append_time_ms: -1,
                            },
                            Err(error) => ProducePartitionResult {
                                partition: partition.partition,
                                error: error_code(error),
                                base_offset: -1,
                                log_append_time_ms: -1,
                            },
                        }
                    })
                    .collect();
                ProduceTopicResult {
                    name,
                    partitions,
                }
            })
            .collect();

        encode_produce(
            correlation_id,
            &ProduceResponse {
                topics: response_topics,
                throttle_time_ms: 0,
            },
        )
    }

    fn fetch(
        &self,
        correlation_id: i32,
        request_max_bytes: i32,
        topics: Vec<broker_wire::FetchTopic>,
    ) -> Result<Vec<u8>, Error> {
        let response_topics = topics
            .into_iter()
            .map(|topic| {
                let broker_wire::FetchTopic {
                    name,
                    partitions,
                } = topic;
                let partitions = partitions
                    .into_iter()
                    .map(|partition| {
                        let max_bytes =
                            (partition.partition_max_bytes.min(request_max_bytes).max(0) as usize)
                                .min(self.max_fetch_bytes);
                        match self.broker.fetch(
                            &name,
                            partition.partition,
                            partition.fetch_offset,
                            self.max_fetch_records,
                            max_bytes,
                        ) {
                            Ok(window) => FetchPartitionResult {
                                partition: partition.partition,
                                error: NO_ERROR,
                                high_watermark: window.high_watermark,
                                last_stable_offset: window.high_watermark,
                                aborted_transactions: None,
                                records: window.records,
                            },
                            Err(error) => FetchPartitionResult {
                                partition: partition.partition,
                                error: error_code(error),
                                high_watermark: -1,
                                last_stable_offset: -1,
                                aborted_transactions: None,
                                records: Vec::new(),
                            },
                        }
                    })
                    .collect();
                FetchTopicResult {
                    name,
                    partitions,
                }
            })
            .collect();

        encode_fetch(
            correlation_id,
            &FetchResponse {
                throttle_time_ms: 0,
                topics: response_topics,
            },
        )
    }

    fn list_offsets(
        &self,
        correlation_id: i32,
        topics: Vec<broker_wire::ListOffsetsTopic>,
    ) -> Result<Vec<u8>, Error> {
        let response_topics = topics
            .into_iter()
            .map(|topic| {
                let broker_wire::ListOffsetsTopic {
                    name,
                    partitions,
                } = topic;
                let partitions = partitions
                    .into_iter()
                    .map(|partition| match partition.timestamp {
                        -2 | -1 => {
                            let earliest = partition.timestamp == -2;
                            match self.broker.list_offset(&name, partition.partition, earliest) {
                                Ok(offset) => ListOffsetsPartitionResult {
                                    partition: partition.partition,
                                    error: NO_ERROR,
                                    timestamp_ms: -1,
                                    offset,
                                },
                                Err(error) => ListOffsetsPartitionResult {
                                    partition: partition.partition,
                                    error: error_code(error),
                                    timestamp_ms: -1,
                                    offset: -1,
                                },
                            }
                        }
                        _ => ListOffsetsPartitionResult {
                            partition: partition.partition,
                            error: UNSUPPORTED_VERSION,
                            timestamp_ms: -1,
                            offset: -1,
                        },
                    })
                    .collect();
                ListOffsetsTopicResult {
                    name,
                    partitions,
                }
            })
            .collect();

        encode_list_offsets(
            correlation_id,
            &ListOffsetsResponse {
                topics: response_topics,
            },
        )
    }

    fn partitions(&self, count: i32) -> Vec<PartitionMetadata> {
        (0..count)
            .map(|partition| PartitionMetadata {
                error: NO_ERROR,
                partition,
                leader: self.identity.node_id,
                replicas: vec![self.identity.node_id],
                isr: vec![self.identity.node_id],
            })
            .collect()
    }
}

fn error_code(error: BrokerError) -> i16 {
    match error {
        BrokerError::Core(LogError::UnknownTopic | LogError::UnknownPartition) => {
            UNKNOWN_TOPIC_OR_PARTITION
        }
        BrokerError::Core(LogError::OffsetOutOfRange) => OFFSET_OUT_OF_RANGE,
        _ => UNKNOWN_SERVER_ERROR,
    }
}

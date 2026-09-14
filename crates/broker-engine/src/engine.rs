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
    ProducerIdentity,
};
use broker_wire::{
    BrokerMetadata,
    Error,
    FetchPartitionResult,
    FetchResponse,
    FetchTopicResult,
    FindCoordinatorResponse,
    InitProducerIdResponse,
    JoinGroupResponse,
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
    TransactionPartitionResult,
    decode_request,
    encode_add_partitions_to_txn,
    encode_api_versions,
    encode_api_versions_error,
    encode_end_txn,
    encode_fetch,
    encode_find_coordinator,
    encode_group_error,
    encode_init_producer_id,
    encode_join_group,
    encode_list_offsets,
    encode_metadata,
    encode_offset_commit,
    encode_offset_fetch,
    encode_produce,
    encode_sync_group,
    peek_header,
    protocol::{
        INVALID_PRODUCER_EPOCH,
        INVALID_REQUEST,
        INVALID_TXN_STATE,
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

struct GroupOffsetCommit {
    group_id: Vec<u8>,
    generation: i32,
    topic: Vec<u8>,
    partition: i32,
    offset: i64,
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
            RequestBody::FindCoordinator {
                ..
            } => self.find_coordinator(correlation_id),
            RequestBody::InitProducerId {
                transactional_id,
                ..
            } => self.init_producer(correlation_id, transactional_id),
            RequestBody::AddPartitionsToTxn {
                transactional_id,
                producer_id,
                producer_epoch,
                topics,
            } => self.add_partitions_to_txn(
                correlation_id,
                transactional_id,
                producer_id,
                producer_epoch,
                topics,
            ),
            RequestBody::EndTxn {
                transactional_id,
                producer_id,
                producer_epoch,
                commit,
            } => {
                self.end_txn(correlation_id, transactional_id, producer_id, producer_epoch, commit)
            }
            RequestBody::JoinGroup {
                group_id,
                member_id,
                subscriptions,
            } => self.join_group(correlation_id, group_id, member_id, subscriptions),
            RequestBody::SyncGroup {
                group_id,
                generation,
                member_id,
                assignment,
            } => self.sync_group(correlation_id, group_id, generation, member_id, assignment),
            RequestBody::Heartbeat {
                group_id,
                generation,
                member_id,
            } => self.heartbeat(correlation_id, group_id, member_id, generation),
            RequestBody::LeaveGroup {
                group_id,
                member_id,
            } => self.leave_group(correlation_id, group_id, member_id),
            RequestBody::OffsetCommit {
                group_id,
                generation,
                member_id: _,
                topic,
                partition,
                offset,
            } => self.offset_commit(
                correlation_id,
                GroupOffsetCommit {
                    group_id,
                    generation,
                    topic,
                    partition,
                    offset,
                },
            ),
            RequestBody::OffsetFetch {
                group_id,
                topic,
                partition,
            } => self.offset_fetch(correlation_id, group_id, topic, partition),
            RequestBody::Metadata {
                topics,
            } => self.metadata(correlation_id, topics),
            RequestBody::Produce {
                transactional_id,
                topics,
                ..
            } => self.produce(correlation_id, transactional_id, topics),
            RequestBody::Fetch {
                max_bytes,
                read_committed,
                topics,
                ..
            } => self.fetch(correlation_id, max_bytes, read_committed, topics),
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

    fn find_coordinator(&self, correlation_id: i32) -> Result<Vec<u8>, Error> {
        encode_find_coordinator(
            correlation_id,
            &FindCoordinatorResponse {
                error: NO_ERROR,
                node_id: self.identity.node_id,
                host: self.identity.host.clone(),
                port: self.identity.port,
            },
        )
    }

    fn init_producer(
        &self,
        correlation_id: i32,
        transactional_id: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, Error> {
        let response = match transactional_id {
            Some(transactional_id) => match self.broker.init_producer(&transactional_id) {
                Ok(identity) => InitProducerIdResponse {
                    error: NO_ERROR,
                    producer_id: identity.id,
                    producer_epoch: identity.epoch,
                },
                Err(_) => InitProducerIdResponse {
                    error: UNKNOWN_SERVER_ERROR,
                    producer_id: -1,
                    producer_epoch: -1,
                },
            },
            None => InitProducerIdResponse {
                error: INVALID_REQUEST,
                producer_id: -1,
                producer_epoch: -1,
            },
        };
        encode_init_producer_id(correlation_id, response)
    }

    fn join_group(
        &self,
        correlation_id: i32,
        group_id: Vec<u8>,
        member_id: Vec<u8>,
        subscriptions: Vec<Vec<u8>>,
    ) -> Result<Vec<u8>, Error> {
        let member_id = if member_id.is_empty() {
            generated_member_id(correlation_id)
        } else {
            member_id
        };
        let result = self.broker.join_group(&group_id, &member_id, subscriptions.clone());
        match result {
            Ok((generation, assignments)) => {
                let members = assignments
                    .iter()
                    .map(|assignment| {
                        encode_subscription(&subscriptions)
                            .map(|metadata| (assignment.member_id.clone(), metadata))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                encode_join_group(
                    correlation_id,
                    &JoinGroupResponse {
                        error: NO_ERROR,
                        generation,
                        protocol: b"charlotte-fixed-v1".to_vec(),
                        leader: member_id.clone(),
                        member_id,
                        members,
                    },
                )
            }
            Err(error) => encode_join_group(
                correlation_id,
                &JoinGroupResponse {
                    error: coordination_error_code(error),
                    generation: -1,
                    protocol: Vec::new(),
                    leader: Vec::new(),
                    member_id,
                    members: Vec::new(),
                },
            ),
        }
    }

    fn sync_group(
        &self,
        correlation_id: i32,
        _group_id: Vec<u8>,
        _generation: i32,
        _member_id: Vec<u8>,
        assignment: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        encode_sync_group(correlation_id, NO_ERROR, &assignment)
    }

    fn heartbeat(
        &self,
        correlation_id: i32,
        group_id: Vec<u8>,
        member_id: Vec<u8>,
        generation: i32,
    ) -> Result<Vec<u8>, Error> {
        let error = self
            .broker
            .heartbeat_group(&group_id, &member_id, generation)
            .map_or_else(coordination_error_code, |_| NO_ERROR);
        encode_group_error(correlation_id, error)
    }

    fn leave_group(
        &self,
        correlation_id: i32,
        group_id: Vec<u8>,
        member_id: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        let error = self
            .broker
            .leave_group_any(&group_id, &member_id)
            .map_or_else(coordination_error_code, |_| NO_ERROR);
        encode_group_error(correlation_id, error)
    }

    fn offset_commit(
        &self,
        correlation_id: i32,
        commit: GroupOffsetCommit,
    ) -> Result<Vec<u8>, Error> {
        let error = self
            .broker
            .commit_group_offset(
                &commit.group_id,
                commit.generation,
                &commit.topic,
                commit.partition,
                commit.offset,
            )
            .map_or_else(coordination_error_code, |_| NO_ERROR);
        encode_offset_commit(correlation_id, &commit.topic, commit.partition, error)
    }

    fn offset_fetch(
        &self,
        correlation_id: i32,
        group_id: Vec<u8>,
        topic: Vec<u8>,
        partition: i32,
    ) -> Result<Vec<u8>, Error> {
        match self.broker.group_offset(&group_id, &topic, partition) {
            Ok(Some(offset)) => {
                encode_offset_fetch(correlation_id, &topic, partition, offset, NO_ERROR)
            }
            Ok(None) => encode_offset_fetch(correlation_id, &topic, partition, -1, NO_ERROR),
            Err(error) => encode_offset_fetch(
                correlation_id,
                &topic,
                partition,
                -1,
                coordination_error_code(error),
            ),
        }
    }

    fn produce(
        &self,
        correlation_id: i32,
        transactional_id: Option<Vec<u8>>,
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
                        if transactional_id.is_some() && partition.producer.is_none() {
                            return ProducePartitionResult {
                                partition: partition.partition,
                                error: UNSUPPORTED_VERSION,
                                base_offset: -1,
                                log_append_time_ms: -1,
                            };
                        }
                        let result = match (transactional_id.as_deref(), partition.producer) {
                            (Some(id), Some((producer_id, producer_epoch))) => {
                                self.broker.produce_transactional(
                                    id,
                                    ProducerIdentity {
                                        id: producer_id,
                                        epoch: producer_epoch,
                                    },
                                    &name,
                                    partition.partition,
                                    partition.records,
                                )
                            }
                            (Some(_), None) => {
                                unreachable!("transactional batches were checked above")
                            }
                            (None, _) => {
                                self.broker.produce(&name, partition.partition, partition.records)
                            }
                        };
                        match result {
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

    fn add_partitions_to_txn(
        &self,
        correlation_id: i32,
        transactional_id: Vec<u8>,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<(Vec<u8>, Vec<i32>)>,
    ) -> Result<Vec<u8>, Error> {
        let producer = ProducerIdentity {
            id: producer_id,
            epoch: producer_epoch,
        };
        let response_topics = topics
            .into_iter()
            .map(|(topic, partitions)| {
                let results = partitions
                    .into_iter()
                    .map(|partition| {
                        let error = self
                            .broker
                            .add_transaction_partition(
                                &transactional_id,
                                producer,
                                &topic,
                                partition,
                            )
                            .map_or_else(coordination_error_code, |_| NO_ERROR);
                        (partition, error)
                    })
                    .collect();
                TransactionPartitionResult {
                    topic,
                    partitions: results,
                }
            })
            .collect::<Vec<_>>();
        encode_add_partitions_to_txn(correlation_id, &response_topics)
    }

    fn end_txn(
        &self,
        correlation_id: i32,
        transactional_id: Vec<u8>,
        producer_id: i64,
        producer_epoch: i16,
        commit: bool,
    ) -> Result<Vec<u8>, Error> {
        let error = self
            .broker
            .end_transaction(
                &transactional_id,
                ProducerIdentity {
                    id: producer_id,
                    epoch: producer_epoch,
                },
                commit,
            )
            .map_or_else(broker_error_code, |_| NO_ERROR);
        encode_end_txn(correlation_id, error)
    }

    fn fetch(
        &self,
        correlation_id: i32,
        request_max_bytes: i32,
        read_committed: bool,
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
                        match self.broker.fetch_with_isolation(
                            &name,
                            partition.partition,
                            partition.fetch_offset,
                            self.max_fetch_records,
                            max_bytes,
                            read_committed,
                        ) {
                            Ok(window) => FetchPartitionResult {
                                partition: partition.partition,
                                error: NO_ERROR,
                                high_watermark: window.high_watermark,
                                last_stable_offset: window.last_stable_offset,
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

fn coordination_error_code(error: BrokerError) -> i16 {
    match error {
        BrokerError::Coordination(broker_core::CoordinationError::ProducerFenced) => {
            INVALID_PRODUCER_EPOCH
        }
        BrokerError::Coordination(broker_core::CoordinationError::InvalidTransactionState) => {
            INVALID_TXN_STATE
        }
        BrokerError::Coordination(_) => INVALID_REQUEST,
        other => error_code(other),
    }
}

fn broker_error_code(error: BrokerError) -> i16 {
    coordination_error_code(error)
}

fn encode_subscription(topics: &[Vec<u8>]) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(0i16).to_be_bytes());
    bytes.extend_from_slice(&(topics.len() as i32).to_be_bytes());
    for topic in topics {
        if topic.len() > i16::MAX as usize {
            return Err(Error::TooLarge);
        }
        bytes.extend_from_slice(&(topic.len() as i16).to_be_bytes());
        bytes.extend_from_slice(topic);
    }
    bytes.extend_from_slice(&(-1i32).to_be_bytes());
    Ok(bytes)
}

fn generated_member_id(correlation_id: i32) -> Vec<u8> {
    let mut digits = [0u8; 12];
    let mut value = correlation_id.unsigned_abs();
    let mut index = digits.len();
    if value == 0 {
        index -= 1;
        digits[index] = b'0';
    } else {
        while value > 0 {
            index -= 1;
            digits[index] = b'0' + (value % 10) as u8;
            value /= 10;
        }
    }
    let mut member = b"member-".to_vec();
    member.extend_from_slice(&digits[index..]);
    member
}

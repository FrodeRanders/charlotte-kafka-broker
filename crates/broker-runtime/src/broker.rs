//! The broker facade: shard startup, routing, calls, and shutdown.
//!
//! [`Broker`] owns the mailbox sender for every partition shard. Callers give
//! it a topic and partition; it validates them against the catalog, routes to
//! the deterministic owning shard, and waits on a bounded reply channel while
//! parking between polls.

use alloc::{
    sync::Arc,
    vec::Vec,
};
use core::{
    sync::atomic::{
        AtomicBool,
        Ordering,
    },
    time::Duration,
};

use broker_core::{
    FetchWindow,
    GroupAssignment,
    ProducerIdentity,
    RecordData,
    TopicCatalog,
    TopicMetadata,
    TransactionOffset,
};
use sitas_core::{
    shard::ShardId,
    shard_runtime::{
        ShardParker,
        ShardReceiver,
        ShardRuntime,
        ShardSender,
        channel,
    },
};

use crate::{
    error::BrokerError,
    message::{
        CoordinationRequest,
        CoordinationResult,
        PartitionCommand,
        PartitionResult,
    },
    partition::spawn_partition_shard,
};

/// Default number of commands a shard mailbox holds before callers park.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 64;

/// Default park interval used while a caller waits for capacity or a reply.
pub const DEFAULT_PARK_TIMEOUT: Duration = Duration::from_millis(5);

/// Default number of park intervals before a wait is abandoned.
pub const DEFAULT_MAX_RETRIES: u32 = 2_000;

/// One topic's desired partition count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicSpec {
    /// Topic name.
    pub name: Vec<u8>,
    /// Number of partitions; must be positive.
    pub partitions: i32,
}

/// Static broker configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerConfig {
    /// Number of physical partition shards.
    pub shard_count: usize,
    /// Topics created at startup.
    pub topics: Vec<TopicSpec>,
    /// Capacity of each shard mailbox.
    pub mailbox_capacity: usize,
    /// Park interval between retries.
    pub park_timeout: Duration,
    /// Retry budget for mailbox sends and reply waits.
    pub max_retries: u32,
    /// Optional approximate retained-byte budget per partition.
    pub partition_max_bytes: Option<usize>,
}

impl BrokerConfig {
    /// Creates a configuration with the default mailbox and wait policy.
    pub fn new(shard_count: usize, topics: Vec<TopicSpec>) -> Self {
        Self {
            shard_count,
            topics,
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
            park_timeout: DEFAULT_PARK_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            partition_max_bytes: None,
        }
    }

    /// Overrides the shard mailbox capacity.
    pub fn with_mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = capacity;
        self
    }

    /// Overrides the park interval and retry budget.
    pub fn with_wait_policy(mut self, park_timeout: Duration, max_retries: u32) -> Self {
        self.park_timeout = park_timeout;
        self.max_retries = max_retries;
        self
    }

    /// Bounds each partition to an approximate retained-byte budget.
    pub fn with_partition_max_bytes(mut self, max_bytes: usize) -> Self {
        self.partition_max_bytes = Some(max_bytes);
        self
    }
}

/// A shard-per-partition Kafka log broker.
pub struct Broker {
    shards: Vec<ShardSender<PartitionCommand>>,
    catalog: Arc<TopicCatalog>,
    parker: Arc<dyn ShardParker>,
    park_timeout: Duration,
    max_retries: u32,
    stopped: AtomicBool,
}

impl Broker {
    /// Starts every partition shard on `runtime`.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::InvalidConfig`] for a zero shard count, an empty
    /// topic list, a mailbox capacity below two, or a rejected topic
    /// definition.
    pub fn start<R>(runtime: &R, config: BrokerConfig) -> Result<Self, BrokerError>
    where
        R: ShardRuntime + ?Sized,
    {
        if config.shard_count == 0 || config.topics.is_empty() || config.mailbox_capacity < 2 {
            return Err(BrokerError::InvalidConfig);
        }

        let mut catalog = TopicCatalog::new();
        for topic in &config.topics {
            catalog.create(&topic.name, topic.partitions)?;
        }
        let catalog = Arc::new(catalog);
        let parker = runtime.parker();

        let mut shards = Vec::with_capacity(config.shard_count);
        for index in 0..config.shard_count {
            let (sender, receiver) = channel(queue_capacity(config.mailbox_capacity))
                .map_err(|_| BrokerError::InvalidConfig)?;
            shards.push(sender);
            spawn_partition_shard(
                runtime,
                ShardId(index),
                Arc::clone(&catalog),
                Arc::clone(&parker),
                receiver,
                config.park_timeout,
                config.partition_max_bytes,
            );
        }

        Ok(Self {
            shards,
            catalog,
            parker,
            park_timeout: config.park_timeout,
            max_retries: config.max_retries,
            stopped: AtomicBool::new(false),
        })
    }

    /// Appends records atomically and returns the batch's base offset.
    pub fn produce(
        &self,
        topic: &[u8],
        partition: i32,
        records: Vec<RecordData>,
    ) -> Result<i64, BrokerError> {
        self.catalog.check_partition(topic, partition)?;
        let result = self.call(topic, partition, |reply| PartitionCommand::Produce {
            topic: Vec::from(topic),
            partition,
            records,
            reply,
        })?;
        match result {
            PartitionResult::Produced {
                base_offset,
            } => Ok(base_offset),
            PartitionResult::Failed(error) => Err(BrokerError::Core(error)),
            _ => Err(BrokerError::UnexpectedReply),
        }
    }

    /// Reads records beginning at `offset`.
    pub fn fetch(
        &self,
        topic: &[u8],
        partition: i32,
        offset: i64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<FetchWindow, BrokerError> {
        self.fetch_with_isolation(topic, partition, offset, max_records, max_bytes, false)
    }

    /// Reads a partition with explicit Kafka isolation semantics.
    pub fn fetch_with_isolation(
        &self,
        topic: &[u8],
        partition: i32,
        offset: i64,
        max_records: usize,
        max_bytes: usize,
        read_committed: bool,
    ) -> Result<FetchWindow, BrokerError> {
        self.catalog.check_partition(topic, partition)?;
        let result = self.call(topic, partition, |reply| PartitionCommand::Fetch {
            topic: Vec::from(topic),
            partition,
            offset,
            max_records,
            max_bytes,
            read_committed,
            reply,
        })?;
        match result {
            PartitionResult::Fetched(window) => Ok(window),
            PartitionResult::Failed(error) => Err(BrokerError::Core(error)),
            _ => Err(BrokerError::UnexpectedReply),
        }
    }

    /// Reads the earliest or latest offset of a partition.
    pub fn list_offset(
        &self,
        topic: &[u8],
        partition: i32,
        earliest: bool,
    ) -> Result<i64, BrokerError> {
        self.catalog.check_partition(topic, partition)?;
        let result = self.call(topic, partition, |reply| PartitionCommand::ListOffset {
            topic: Vec::from(topic),
            partition,
            earliest,
            reply,
        })?;
        match result {
            PartitionResult::Offset(offset) => Ok(offset),
            PartitionResult::Failed(error) => Err(BrokerError::Core(error)),
            _ => Err(BrokerError::UnexpectedReply),
        }
    }

    /// Metadata for every configured topic.
    pub fn metadata(&self) -> Vec<TopicMetadata> {
        self.catalog.metadata()
    }

    /// Partition count for a topic, if configured.
    pub fn partition_count(&self, topic: &[u8]) -> Option<i32> {
        self.catalog.partition_count(topic)
    }

    /// Issues (or fences) the producer identity for a transactional id.
    pub fn init_producer(&self, transactional_id: &[u8]) -> Result<ProducerIdentity, BrokerError> {
        match self.coordinate(CoordinationRequest::InitProducer {
            transactional_id: Vec::from(transactional_id),
        })? {
            CoordinationResult::Producer(identity) => Ok(identity),
            _ => Err(BrokerError::UnexpectedReply),
        }
    }

    /// Enlists a partition in an ongoing transaction.
    pub fn add_transaction_partition(
        &self,
        transactional_id: &[u8],
        producer: ProducerIdentity,
        topic: &[u8],
        partition: i32,
    ) -> Result<(), BrokerError> {
        self.coordinate(CoordinationRequest::AddPartition {
            transactional_id: Vec::from(transactional_id),
            producer,
            topic: Vec::from(topic),
            partition,
        })
        .and_then(applied)
    }

    /// Checks the producer epoch and enlisted partition before appending.
    pub fn validate_transactional_produce(
        &self,
        transactional_id: &[u8],
        producer: ProducerIdentity,
        topic: &[u8],
        partition: i32,
    ) -> Result<(), BrokerError> {
        self.coordinate(CoordinationRequest::ValidateProduce {
            transactional_id: Vec::from(transactional_id),
            producer,
            topic: Vec::from(topic),
            partition,
        })
        .and_then(applied)
    }

    /// Validates a transactional append and then routes it to the owning
    /// partition shard. The append itself remains atomic at the partition;
    /// commit markers/read-committed filtering are a later log integration.
    pub fn produce_transactional(
        &self,
        transactional_id: &[u8],
        producer: ProducerIdentity,
        topic: &[u8],
        partition: i32,
        records: Vec<RecordData>,
    ) -> Result<i64, BrokerError> {
        self.validate_transactional_produce(transactional_id, producer, topic, partition)?;
        self.catalog.check_partition(topic, partition)?;
        let result =
            self.call(topic, partition, |reply| PartitionCommand::ProduceTransactional {
                topic: Vec::from(topic),
                partition,
                records,
                producer_id: producer.id,
                reply,
            })?;
        match result {
            PartitionResult::Produced {
                base_offset,
            } => Ok(base_offset),
            PartitionResult::Failed(error) => Err(BrokerError::Core(error)),
            _ => Err(BrokerError::UnexpectedReply),
        }
    }

    /// Enlists a consumer offset so it is committed atomically with the
    /// producer transaction.
    pub fn add_transaction_offset(
        &self,
        transactional_id: &[u8],
        producer: ProducerIdentity,
        offset: TransactionOffset,
    ) -> Result<(), BrokerError> {
        self.coordinate(CoordinationRequest::AddOffset {
            transactional_id: Vec::from(transactional_id),
            producer,
            offset,
        })
        .and_then(applied)
    }

    /// Commits or aborts an ongoing transaction and fences its identity.
    pub fn end_transaction(
        &self,
        transactional_id: &[u8],
        producer: ProducerIdentity,
        commit: bool,
    ) -> Result<(), BrokerError> {
        let result = self.coordinate(CoordinationRequest::EndTransaction {
            transactional_id: Vec::from(transactional_id),
            producer,
            commit,
        })?;
        let completion = match result {
            CoordinationResult::TransactionCompleted(completion) => completion,
            _ => return Err(BrokerError::UnexpectedReply),
        };
        for (topic, partition) in completion.partitions {
            let result =
                self.call(&topic, partition, |reply| PartitionCommand::ResolveTransaction {
                    producer_id: producer.id,
                    commit,
                    reply,
                })?;
            if !matches!(result, PartitionResult::Resolved) {
                return Err(BrokerError::UnexpectedReply);
            }
        }
        if commit {
            for offset in completion.offsets {
                self.commit_group_offset(
                    &offset.group_id,
                    offset.generation,
                    &offset.topic,
                    offset.partition,
                    offset.offset,
                )?;
            }
        }
        Ok(())
    }

    /// Joins a group and returns a deterministic assignment for every member.
    pub fn join_group(
        &self,
        group_id: &[u8],
        member_id: &[u8],
        subscriptions: Vec<Vec<u8>>,
    ) -> Result<(i32, Vec<GroupAssignment>), BrokerError> {
        let partitions = self
            .catalog
            .metadata()
            .into_iter()
            .flat_map(|topic| {
                (0..topic.partitions).map(move |partition| (topic.name.clone(), partition))
            })
            .collect();
        match self.coordinate(CoordinationRequest::JoinGroup {
            group_id: Vec::from(group_id),
            member_id: Vec::from(member_id),
            subscriptions,
            partitions,
        })? {
            CoordinationResult::Group {
                generation,
                assignments,
            } => Ok((generation, assignments)),
            _ => Err(BrokerError::UnexpectedReply),
        }
    }

    pub fn heartbeat_group(
        &self,
        group_id: &[u8],
        member_id: &[u8],
        generation: i32,
    ) -> Result<(), BrokerError> {
        self.coordinate(CoordinationRequest::Heartbeat {
            group_id: Vec::from(group_id),
            member_id: Vec::from(member_id),
            generation,
        })
        .and_then(applied)
    }

    pub fn leave_group(
        &self,
        group_id: &[u8],
        member_id: &[u8],
        generation: i32,
    ) -> Result<(), BrokerError> {
        self.coordinate(CoordinationRequest::LeaveGroup {
            group_id: Vec::from(group_id),
            member_id: Vec::from(member_id),
            generation,
        })
        .and_then(applied)
    }

    /// Removes a member using Kafka LeaveGroup v0 semantics (no generation).
    pub fn leave_group_any(&self, group_id: &[u8], member_id: &[u8]) -> Result<(), BrokerError> {
        self.coordinate(CoordinationRequest::LeaveGroupAny {
            group_id: Vec::from(group_id),
            member_id: Vec::from(member_id),
        })
        .and_then(applied)
    }

    pub fn commit_group_offset(
        &self,
        group_id: &[u8],
        generation: i32,
        topic: &[u8],
        partition: i32,
        offset: i64,
    ) -> Result<(), BrokerError> {
        self.coordinate(CoordinationRequest::CommitOffset {
            group_id: Vec::from(group_id),
            generation,
            topic: Vec::from(topic),
            partition,
            offset,
        })
        .and_then(applied)
    }

    pub fn group_offset(
        &self,
        group_id: &[u8],
        topic: &[u8],
        partition: i32,
    ) -> Result<Option<i64>, BrokerError> {
        match self.coordinate(CoordinationRequest::FetchOffset {
            group_id: Vec::from(group_id),
            topic: Vec::from(topic),
            partition,
        })? {
            CoordinationResult::Offset(offset) => Ok(offset),
            _ => Err(BrokerError::UnexpectedReply),
        }
    }

    /// Shard that owns `(topic, partition)`.
    ///
    /// The partition is expected to have been validated by the caller.
    pub fn shard_index(&self, topic: &[u8], partition: i32) -> usize {
        let mixed =
            splitmix64(fnv1a(topic) ^ (partition as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        (mixed % self.shards.len() as u64) as usize
    }

    /// Stops every shard and waits for its acknowledgement.
    ///
    /// Shutdown is idempotent; a second call is a no-op.
    pub fn shutdown(&self) -> Result<(), BrokerError> {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let capacity = queue_capacity(self.shards.len());
        let (ack_sender, mut ack_receiver) =
            channel(capacity).map_err(|_| BrokerError::InvalidConfig)?;
        for sender in &self.shards {
            send_with_retry(
                sender,
                PartitionCommand::Shutdown {
                    reply: ack_sender.clone(),
                },
                self.parker.as_ref(),
                self.park_timeout,
                self.max_retries,
            )?;
        }

        let mut acknowledgements = 0usize;
        while acknowledgements < self.shards.len() {
            match wait_reply(
                &mut ack_receiver,
                self.parker.as_ref(),
                self.park_timeout,
                self.max_retries,
            )? {
                PartitionResult::Stopped => acknowledgements += 1,
                _ => return Err(BrokerError::UnexpectedReply),
            }
        }
        Ok(())
    }

    fn call<F>(
        &self,
        topic: &[u8],
        partition: i32,
        command: F,
    ) -> Result<PartitionResult, BrokerError>
    where
        F: FnOnce(ShardSender<PartitionResult>) -> PartitionCommand,
    {
        let shard = self.shard_index(topic, partition);
        let (reply_sender, mut reply_receiver) =
            channel(queue_capacity(1)).map_err(|_| BrokerError::InvalidConfig)?;
        send_with_retry(
            &self.shards[shard],
            command(reply_sender),
            self.parker.as_ref(),
            self.park_timeout,
            self.max_retries,
        )?;
        wait_reply(&mut reply_receiver, self.parker.as_ref(), self.park_timeout, self.max_retries)
    }

    fn coordinate(&self, request: CoordinationRequest) -> Result<CoordinationResult, BrokerError> {
        let (reply_sender, mut reply_receiver) =
            channel(queue_capacity(1)).map_err(|_| BrokerError::InvalidConfig)?;
        send_coordination_with_retry(
            &self.shards[0],
            PartitionCommand::Coordinate {
                request,
                reply: reply_sender,
            },
            self.parker.as_ref(),
            self.park_timeout,
            self.max_retries,
        )?;
        match wait_reply(
            &mut reply_receiver,
            self.parker.as_ref(),
            self.park_timeout,
            self.max_retries,
        )? {
            PartitionResult::Coordination(result) => result.map_err(BrokerError::Coordination),
            _ => Err(BrokerError::UnexpectedReply),
        }
    }
}

fn applied(result: CoordinationResult) -> Result<(), BrokerError> {
    match result {
        CoordinationResult::Applied => Ok(()),
        _ => Err(BrokerError::UnexpectedReply),
    }
}

fn send_coordination_with_retry(
    sender: &ShardSender<PartitionCommand>,
    mut command: PartitionCommand,
    parker: &dyn ShardParker,
    park_timeout: Duration,
    max_retries: u32,
) -> Result<(), BrokerError> {
    for _ in 0..max_retries.max(1) {
        match sender.try_send(command) {
            Ok(()) => return Ok(()),
            Err(returned) => {
                command = returned;
                parker.park(Some(park_timeout));
            }
        }
    }
    Err(BrokerError::MailboxUnavailable)
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn send_with_retry(
    sender: &ShardSender<PartitionCommand>,
    mut command: PartitionCommand,
    parker: &dyn ShardParker,
    park_timeout: Duration,
    max_retries: u32,
) -> Result<(), BrokerError> {
    for _ in 0..max_retries.max(1) {
        match sender.try_send(command) {
            Ok(()) => return Ok(()),
            Err(returned) => {
                command = returned;
                parker.park(Some(park_timeout));
            }
        }
    }
    Err(BrokerError::MailboxUnavailable)
}

fn wait_reply(
    receiver: &mut ShardReceiver<PartitionResult>,
    parker: &dyn ShardParker,
    park_timeout: Duration,
    max_retries: u32,
) -> Result<PartitionResult, BrokerError> {
    for _ in 0..max_retries.max(1) {
        if let Some(result) = receiver.try_recv() {
            return Ok(result);
        }
        parker.park(Some(park_timeout));
    }
    Err(BrokerError::ReplyTimeout)
}

/// Usable capacity `items` corresponds to a ring size one larger, because the
/// bounded ring buffer distinguishes full from empty with a reserved slot.
fn queue_capacity(items: usize) -> usize {
    items + 1
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

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
    RecordData,
    TopicCatalog,
    TopicMetadata,
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
        self.catalog.check_partition(topic, partition)?;
        let result = self.call(topic, partition, |reply| PartitionCommand::Fetch {
            topic: Vec::from(topic),
            partition,
            offset,
            max_records,
            max_bytes,
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

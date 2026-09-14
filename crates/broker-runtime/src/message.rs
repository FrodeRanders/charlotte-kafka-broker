//! Typed commands and replies exchanged with a partition shard.
//!
//! Every message is an owned value. A command carries the reply sender it
//! expects; the shard answers on that channel and then unparks the caller.

use alloc::vec::Vec;

use broker_core::{
    CoordinationError,
    FetchWindow,
    GroupAssignment,
    LogError,
    ProducerIdentity,
    RecordData,
    TransactionCompletion,
    TransactionOffset,
};
use sitas_core::shard_runtime::ShardSender;

/// A request routed to the shard owning one partition.
pub(crate) enum PartitionCommand {
    Produce {
        topic: Vec<u8>,
        partition: i32,
        records: Vec<RecordData>,
        reply: ShardSender<PartitionResult>,
    },
    ProduceTransactional {
        topic: Vec<u8>,
        partition: i32,
        records: Vec<RecordData>,
        producer_id: i64,
        reply: ShardSender<PartitionResult>,
    },
    Fetch {
        topic: Vec<u8>,
        partition: i32,
        offset: i64,
        max_records: usize,
        max_bytes: usize,
        read_committed: bool,
        reply: ShardSender<PartitionResult>,
    },
    ListOffset {
        topic: Vec<u8>,
        partition: i32,
        earliest: bool,
        reply: ShardSender<PartitionResult>,
    },
    Shutdown {
        reply: ShardSender<PartitionResult>,
    },
    ResolveTransaction {
        producer_id: i64,
        commit: bool,
        reply: ShardSender<PartitionResult>,
    },
    /// Coordination state is hosted by shard zero so it remains single-writer
    /// without introducing a lock shared by every connection.
    Coordinate {
        request: CoordinationRequest,
        reply: ShardSender<PartitionResult>,
    },
}

/// Operations owned by the broker's coordination state machine.
pub(crate) enum CoordinationRequest {
    InitProducer {
        transactional_id: Vec<u8>,
    },
    AddPartition {
        transactional_id: Vec<u8>,
        producer: ProducerIdentity,
        topic: Vec<u8>,
        partition: i32,
    },
    ValidateProduce {
        transactional_id: Vec<u8>,
        producer: ProducerIdentity,
        topic: Vec<u8>,
        partition: i32,
    },
    EndTransaction {
        transactional_id: Vec<u8>,
        producer: ProducerIdentity,
        commit: bool,
    },
    AddOffset {
        transactional_id: Vec<u8>,
        producer: ProducerIdentity,
        offset: TransactionOffset,
    },
    JoinGroup {
        group_id: Vec<u8>,
        member_id: Vec<u8>,
        subscriptions: Vec<Vec<u8>>,
        partitions: Vec<(Vec<u8>, i32)>,
    },
    Heartbeat {
        group_id: Vec<u8>,
        member_id: Vec<u8>,
        generation: i32,
    },
    LeaveGroup {
        group_id: Vec<u8>,
        member_id: Vec<u8>,
        generation: i32,
    },
    LeaveGroupAny {
        group_id: Vec<u8>,
        member_id: Vec<u8>,
    },
    CommitOffset {
        group_id: Vec<u8>,
        generation: i32,
        topic: Vec<u8>,
        partition: i32,
        offset: i64,
    },
    FetchOffset {
        group_id: Vec<u8>,
        topic: Vec<u8>,
        partition: i32,
    },
}

/// A shard's answer to one command.
pub(crate) enum PartitionResult {
    Produced {
        base_offset: i64,
    },
    Fetched(FetchWindow),
    Offset(i64),
    Failed(LogError),
    Coordination(Result<CoordinationResult, CoordinationError>),
    Resolved,
    Stopped,
}

pub(crate) enum CoordinationResult {
    Producer(ProducerIdentity),
    Group {
        generation: i32,
        assignments: Vec<GroupAssignment>,
    },
    TransactionCompleted(TransactionCompletion),
    Offset(Option<i64>),
    Applied,
}

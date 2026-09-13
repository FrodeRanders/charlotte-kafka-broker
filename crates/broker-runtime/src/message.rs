//! Typed commands and replies exchanged with a partition shard.
//!
//! Every message is an owned value. A command carries the reply sender it
//! expects; the shard answers on that channel and then unparks the caller.

use alloc::vec::Vec;

use broker_core::{
    FetchWindow,
    LogError,
    RecordData,
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
    Fetch {
        topic: Vec<u8>,
        partition: i32,
        offset: i64,
        max_records: usize,
        max_bytes: usize,
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
}

/// A shard's answer to one command.
pub(crate) enum PartitionResult {
    Produced {
        base_offset: i64,
    },
    Fetched(FetchWindow),
    Offset(i64),
    Failed(LogError),
    Stopped,
}

//! The partition shard: the only place a partition log is mutated.
//!
//! Each physical shard owns a set of partitions and a message loop driven by
//! its own `ShardExecutor`. The loop validates topic/partition authority
//! against the catalog, applies the operation to the owning [`PartitionLog`],
//! and answers on the command's reply channel.

use alloc::{
    boxed::Box,
    collections::BTreeMap,
    sync::Arc,
    vec::Vec,
};
use core::time::Duration;

use broker_core::{
    PartitionLog,
    TopicCatalog,
};
use sitas_core::{
    placement::ShardPlacement,
    shard::ShardId,
    shard_executor::ShardExecutor,
    shard_runtime::{
        ShardParker,
        ShardReceiver,
        ShardRuntime,
        ShardSender,
    },
};

use crate::message::{
    PartitionCommand,
    PartitionResult,
};

/// Spawns one shard thread whose lifetime is driven by `receiver`.
pub(crate) fn spawn_partition_shard<R>(
    runtime: &R,
    shard_id: ShardId,
    catalog: Arc<TopicCatalog>,
    parker: Arc<dyn ShardParker>,
    receiver: ShardReceiver<PartitionCommand>,
    park_timeout: Duration,
) where
    R: ShardRuntime + ?Sized,
{
    let reactor = runtime.shard_reactor(shard_id);
    runtime.spawn_shard(
        shard_id,
        ShardPlacement::Sequential,
        Box::new(move || {
            let mut executor = ShardExecutor::new(reactor).with_idle_wait(Some(park_timeout));
            executor.spawn(partition_task(receiver, catalog, parker));
            executor.run();
        }),
    );
}

async fn partition_task(
    mut receiver: ShardReceiver<PartitionCommand>,
    catalog: Arc<TopicCatalog>,
    parker: Arc<dyn ShardParker>,
) {
    let mut logs: BTreeMap<(Vec<u8>, i32), PartitionLog> = BTreeMap::new();

    while let Some(command) = receiver.recv().await {
        match command {
            PartitionCommand::Produce {
                topic,
                partition,
                records,
                reply,
            } => {
                let result = if let Err(error) = catalog.check_partition(&topic, partition) {
                    PartitionResult::Failed(error)
                } else {
                    let log = logs.entry((topic, partition)).or_default();
                    match log.append_data(&records) {
                        Ok(base_offset) => PartitionResult::Produced {
                            base_offset,
                        },
                        Err(error) => PartitionResult::Failed(error),
                    }
                };
                respond(&reply, parker.as_ref(), result);
            }
            PartitionCommand::Fetch {
                topic,
                partition,
                offset,
                max_records,
                max_bytes,
                reply,
            } => {
                let result = if let Err(error) = catalog.check_partition(&topic, partition) {
                    PartitionResult::Failed(error)
                } else {
                    let log = logs.entry((topic, partition)).or_default();
                    match log.fetch(offset, max_records, max_bytes) {
                        Ok(window) => PartitionResult::Fetched(window),
                        Err(error) => PartitionResult::Failed(error),
                    }
                };
                respond(&reply, parker.as_ref(), result);
            }
            PartitionCommand::ListOffset {
                topic,
                partition,
                earliest,
                reply,
            } => {
                let result = if let Err(error) = catalog.check_partition(&topic, partition) {
                    PartitionResult::Failed(error)
                } else {
                    let log = logs.entry((topic, partition)).or_default();
                    PartitionResult::Offset(log.list_offset(earliest))
                };
                respond(&reply, parker.as_ref(), result);
            }
            PartitionCommand::Shutdown {
                reply,
            } => {
                respond(&reply, parker.as_ref(), PartitionResult::Stopped);
                return;
            }
        }
    }
}

/// Answers one command and releases the parked caller.
///
/// A reply that nobody waits for anymore (the caller timed out) is dropped
/// rather than retried; the caller's `ReplyTimeout` is the authoritative
/// failure signal.
fn respond(
    reply: &ShardSender<PartitionResult>,
    parker: &dyn ShardParker,
    result: PartitionResult,
) {
    let _ = reply.try_send(result);
    parker.unpark();
}

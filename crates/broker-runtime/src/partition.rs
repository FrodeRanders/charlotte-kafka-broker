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
    GroupCoordinator,
    PartitionLog,
    TopicCatalog,
    TransactionCoordinator,
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
    CoordinationRequest,
    CoordinationResult,
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
    partition_max_bytes: Option<usize>,
) where
    R: ShardRuntime + ?Sized,
{
    let reactor = runtime.shard_reactor(shard_id);
    runtime.spawn_shard(
        shard_id,
        ShardPlacement::Sequential,
        Box::new(move || {
            let mut executor = ShardExecutor::new(reactor).with_idle_wait(Some(park_timeout));
            executor.spawn(partition_task(receiver, catalog, parker, partition_max_bytes));
            executor.run();
        }),
    );
}

async fn partition_task(
    mut receiver: ShardReceiver<PartitionCommand>,
    catalog: Arc<TopicCatalog>,
    parker: Arc<dyn ShardParker>,
    partition_max_bytes: Option<usize>,
) {
    let mut logs: BTreeMap<(Vec<u8>, i32), PartitionLog> = BTreeMap::new();
    let mut transactions = TransactionCoordinator::new();
    let mut groups = GroupCoordinator::new();

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
                    let log = logs
                        .entry((topic, partition))
                        .or_insert_with(|| new_log(partition_max_bytes));
                    match log.append_data(&records) {
                        Ok(base_offset) => PartitionResult::Produced {
                            base_offset,
                        },
                        Err(error) => PartitionResult::Failed(error),
                    }
                };
                respond(&reply, parker.as_ref(), result);
            }
            PartitionCommand::ProduceTransactional {
                topic,
                partition,
                records,
                producer_id,
                reply,
            } => {
                let result = if let Err(error) = catalog.check_partition(&topic, partition) {
                    PartitionResult::Failed(error)
                } else {
                    let log = logs
                        .entry((topic, partition))
                        .or_insert_with(|| new_log(partition_max_bytes));
                    match log.append_transactional(&records, producer_id) {
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
                read_committed,
                reply,
            } => {
                let result = if let Err(error) = catalog.check_partition(&topic, partition) {
                    PartitionResult::Failed(error)
                } else {
                    let log = logs
                        .entry((topic, partition))
                        .or_insert_with(|| new_log(partition_max_bytes));
                    match log.fetch_with_isolation(offset, max_records, max_bytes, read_committed) {
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
                    let log = logs
                        .entry((topic, partition))
                        .or_insert_with(|| new_log(partition_max_bytes));
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
            PartitionCommand::Coordinate {
                request,
                reply,
            } => {
                let result = coordinate(request, &mut transactions, &mut groups);
                respond(&reply, parker.as_ref(), PartitionResult::Coordination(result));
            }
            PartitionCommand::ResolveTransaction {
                producer_id,
                commit,
                reply,
            } => {
                for log in logs.values_mut() {
                    log.commit_transaction(producer_id, commit);
                }
                respond(&reply, parker.as_ref(), PartitionResult::Resolved);
            }
        }
    }
}

fn coordinate(
    request: CoordinationRequest,
    transactions: &mut TransactionCoordinator,
    groups: &mut GroupCoordinator,
) -> Result<CoordinationResult, broker_core::CoordinationError> {
    match request {
        CoordinationRequest::InitProducer {
            transactional_id,
        } => transactions.init(&transactional_id).map(CoordinationResult::Producer),
        CoordinationRequest::AddPartition {
            transactional_id,
            producer,
            topic,
            partition,
        } => transactions
            .add_partition(&transactional_id, producer, &topic, partition)
            .map(|_| CoordinationResult::Applied),
        CoordinationRequest::ValidateProduce {
            transactional_id,
            producer,
            topic,
            partition,
        } => transactions
            .validate_produce(&transactional_id, producer, &topic, partition)
            .map(|_| CoordinationResult::Applied),
        CoordinationRequest::EndTransaction {
            transactional_id,
            producer,
            commit,
        } => transactions
            .end(&transactional_id, producer, commit)
            .map(CoordinationResult::TransactionCompleted),
        CoordinationRequest::AddOffset {
            transactional_id,
            producer,
            offset,
        } => transactions
            .add_offset(&transactional_id, producer, offset)
            .map(|_| CoordinationResult::Applied),
        CoordinationRequest::JoinGroup {
            group_id,
            member_id,
            subscriptions,
            partitions,
        } => groups.join(&group_id, &member_id, subscriptions, &partitions).map(
            |(generation, assignments)| CoordinationResult::Group {
                generation,
                assignments,
            },
        ),
        CoordinationRequest::Heartbeat {
            group_id,
            member_id,
            generation,
        } => {
            groups.heartbeat(&group_id, &member_id, generation).map(|_| CoordinationResult::Applied)
        }
        CoordinationRequest::LeaveGroup {
            group_id,
            member_id,
            generation,
        } => groups.leave(&group_id, &member_id, generation).map(|_| CoordinationResult::Applied),
        CoordinationRequest::LeaveGroupAny {
            group_id,
            member_id,
        } => groups.leave_any(&group_id, &member_id).map(|_| CoordinationResult::Applied),
        CoordinationRequest::CommitOffset {
            group_id,
            generation,
            topic,
            partition,
            offset,
        } => groups
            .commit_offset(&group_id, generation, &topic, partition, offset)
            .map(|_| CoordinationResult::Applied),
        CoordinationRequest::FetchOffset {
            group_id,
            topic,
            partition,
        } => groups.offset(&group_id, &topic, partition).map(CoordinationResult::Offset),
    }
}

/// Creates the partition log for one `(topic, partition)` owned by a shard.
fn new_log(partition_max_bytes: Option<usize>) -> PartitionLog {
    match partition_max_bytes {
        Some(max_bytes) => PartitionLog::with_max_bytes(max_bytes),
        None => PartitionLog::new(),
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

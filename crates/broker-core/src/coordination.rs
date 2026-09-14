//! Deterministic transaction and consumer-group coordination state.
//!
//! The coordinators deliberately contain no I/O or locking.  A runtime shard
//! owns one instance and serializes calls to it.  This keeps producer epochs,
//! group generations, and committed offsets single-writer state just like the
//! partition logs.

use alloc::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    vec::Vec,
};
use core::fmt;

/// A producer identity issued by the transaction coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerIdentity {
    pub id: i64,
    pub epoch: i16,
}

/// Transaction lifecycle visible to the coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
}

#[derive(Clone, Debug)]
struct Transaction {
    identity: ProducerIdentity,
    state: TransactionState,
    partitions: BTreeSet<(Vec<u8>, i32)>,
    offsets: Vec<TransactionOffset>,
}

/// A consumer offset enlisted in a producer transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionOffset {
    pub group_id: Vec<u8>,
    pub generation: i32,
    pub topic: Vec<u8>,
    pub partition: i32,
    pub offset: i64,
}

/// Work that must be resolved when a transaction ends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionCompletion {
    pub partitions: Vec<(Vec<u8>, i32)>,
    pub offsets: Vec<TransactionOffset>,
}

/// Errors raised by transaction and group coordination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinationError {
    InvalidId,
    UnknownTransaction,
    ProducerFenced,
    InvalidTransactionState,
    UnknownGroup,
    UnknownMember,
    IllegalGeneration,
}

impl fmt::Display for CoordinationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::InvalidId => "invalid coordinator id",
            Self::UnknownTransaction => "unknown transaction",
            Self::ProducerFenced => "producer fenced",
            Self::InvalidTransactionState => "invalid transaction state",
            Self::UnknownGroup => "unknown group",
            Self::UnknownMember => "unknown group member",
            Self::IllegalGeneration => "illegal group generation",
        };
        f.write_str(text)
    }
}

impl core::error::Error for CoordinationError {}

/// Single-writer transaction coordinator.
#[derive(Debug, Default)]
pub struct TransactionCoordinator {
    next_id: i64,
    transactions: BTreeMap<Vec<u8>, Transaction>,
}

impl TransactionCoordinator {
    pub const fn new() -> Self {
        Self {
            next_id: 1,
            transactions: BTreeMap::new(),
        }
    }

    /// Fences the previous producer for `transactional_id` and issues a new
    /// epoch. Re-initialising is therefore safe after a client restart.
    pub fn init(&mut self, transactional_id: &[u8]) -> Result<ProducerIdentity, CoordinationError> {
        if transactional_id.is_empty() {
            return Err(CoordinationError::InvalidId);
        }
        let identity = match self.transactions.get(transactional_id) {
            Some(previous) => ProducerIdentity {
                id: previous.identity.id,
                epoch: previous.identity.epoch.saturating_add(1),
            },
            None => {
                let id = self.next_id;
                self.next_id = self.next_id.saturating_add(1);
                ProducerIdentity {
                    id,
                    epoch: 0,
                }
            }
        };
        self.transactions.insert(
            Vec::from(transactional_id),
            Transaction {
                identity,
                state: TransactionState::Empty,
                partitions: BTreeSet::new(),
                offsets: Vec::new(),
            },
        );
        Ok(identity)
    }

    fn transaction_mut(
        &mut self,
        transactional_id: &[u8],
        identity: ProducerIdentity,
    ) -> Result<&mut Transaction, CoordinationError> {
        let transaction = self
            .transactions
            .get_mut(transactional_id)
            .ok_or(CoordinationError::UnknownTransaction)?;
        if transaction.identity != identity {
            return Err(CoordinationError::ProducerFenced);
        }
        Ok(transaction)
    }

    /// Adds a partition and starts the transaction if necessary.
    pub fn add_partition(
        &mut self,
        transactional_id: &[u8],
        identity: ProducerIdentity,
        topic: &[u8],
        partition: i32,
    ) -> Result<(), CoordinationError> {
        if topic.is_empty() || partition < 0 {
            return Err(CoordinationError::InvalidId);
        }
        let transaction = self.transaction_mut(transactional_id, identity)?;
        if matches!(
            transaction.state,
            TransactionState::PrepareCommit | TransactionState::PrepareAbort
        ) {
            return Err(CoordinationError::InvalidTransactionState);
        }
        transaction.state = TransactionState::Ongoing;
        transaction.partitions.insert((Vec::from(topic), partition));
        Ok(())
    }

    /// Validates a transactional produce against the current producer epoch.
    pub fn validate_produce(
        &mut self,
        transactional_id: &[u8],
        identity: ProducerIdentity,
        topic: &[u8],
        partition: i32,
    ) -> Result<(), CoordinationError> {
        let transaction = self.transaction_mut(transactional_id, identity)?;
        if transaction.state != TransactionState::Ongoing
            || !transaction.partitions.contains(&(Vec::from(topic), partition))
        {
            return Err(CoordinationError::InvalidTransactionState);
        }
        Ok(())
    }

    /// Enlists a consumer-group offset in the open transaction.
    pub fn add_offset(
        &mut self,
        transactional_id: &[u8],
        identity: ProducerIdentity,
        offset: TransactionOffset,
    ) -> Result<(), CoordinationError> {
        if offset.group_id.is_empty()
            || offset.topic.is_empty()
            || offset.generation < 0
            || offset.partition < 0
            || offset.offset < 0
        {
            return Err(CoordinationError::InvalidId);
        }
        let transaction = self.transaction_mut(transactional_id, identity)?;
        if transaction.state != TransactionState::Ongoing {
            return Err(CoordinationError::InvalidTransactionState);
        }
        transaction.offsets.push(offset);
        Ok(())
    }

    /// Completes a transaction while retaining its identity for fencing. The partition log remains
    /// the single owner of records; a later integration point adds commit markers
    /// to make read-committed fetches hide aborted batches.
    pub fn end(
        &mut self,
        transactional_id: &[u8],
        identity: ProducerIdentity,
        commit: bool,
    ) -> Result<TransactionCompletion, CoordinationError> {
        let transaction = self.transaction_mut(transactional_id, identity)?;
        if transaction.state != TransactionState::Ongoing {
            return Err(CoordinationError::InvalidTransactionState);
        }
        transaction.state = if commit {
            TransactionState::PrepareCommit
        } else {
            TransactionState::PrepareAbort
        };
        let completion = TransactionCompletion {
            partitions: transaction.partitions.iter().cloned().collect(),
            offsets: transaction.offsets.clone(),
        };
        transaction.partitions.clear();
        transaction.offsets.clear();
        transaction.state = TransactionState::Empty;
        Ok(completion)
    }
}

/// A member's assignment returned by a group join/synchronisation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupAssignment {
    pub member_id: Vec<u8>,
    pub partitions: Vec<(Vec<u8>, i32)>,
}

#[derive(Clone, Debug)]
struct GroupMember {
    subscriptions: Vec<Vec<u8>>,
    generation: i32,
    alive: bool,
}

#[derive(Clone, Debug)]
struct Group {
    generation: i32,
    members: BTreeMap<Vec<u8>, GroupMember>,
    assignments: Vec<GroupAssignment>,
    offsets: BTreeMap<(Vec<u8>, i32), i64>,
}

/// Single-writer consumer-group coordinator with deterministic round-robin
/// assignment and generation fencing.
#[derive(Debug, Default)]
pub struct GroupCoordinator {
    groups: BTreeMap<Vec<u8>, Group>,
}

impl GroupCoordinator {
    pub const fn new() -> Self {
        Self {
            groups: BTreeMap::new(),
        }
    }

    /// Joins or renews a member and returns the new generation assignment.
    pub fn join(
        &mut self,
        group_id: &[u8],
        member_id: &[u8],
        subscriptions: Vec<Vec<u8>>,
        partitions: &[(Vec<u8>, i32)],
    ) -> Result<(i32, Vec<GroupAssignment>), CoordinationError> {
        if group_id.is_empty() || member_id.is_empty() {
            return Err(CoordinationError::InvalidId);
        }
        let group = self.groups.entry(Vec::from(group_id)).or_insert_with(|| Group {
            generation: 0,
            members: BTreeMap::new(),
            assignments: Vec::new(),
            offsets: BTreeMap::new(),
        });
        group.generation = group.generation.saturating_add(1).max(1);
        group.members.insert(
            Vec::from(member_id),
            GroupMember {
                subscriptions,
                generation: group.generation,
                alive: true,
            },
        );
        let member_ids: Vec<Vec<u8>> = group.members.keys().cloned().collect();
        let mut assignments: Vec<GroupAssignment> = member_ids
            .iter()
            .map(|member_id| GroupAssignment {
                member_id: member_id.clone(),
                partitions: Vec::new(),
            })
            .collect();
        for (index, (topic, partition)) in partitions.iter().enumerate() {
            let eligible: Vec<usize> = group
                .members
                .values()
                .enumerate()
                .filter_map(|(member_index, member)| {
                    member.subscriptions.iter().any(|name| name == topic).then_some(member_index)
                })
                .collect();
            if let Some(member_index) = eligible.get(index % eligible.len().max(1)) {
                assignments[*member_index].partitions.push((topic.clone(), *partition));
            }
        }
        group.assignments = assignments.clone();
        Ok((group.generation, assignments))
    }

    /// Heartbeats fence stale generations and mark a member alive.
    pub fn heartbeat(
        &mut self,
        group_id: &[u8],
        member_id: &[u8],
        generation: i32,
    ) -> Result<(), CoordinationError> {
        let group = self.groups.get_mut(group_id).ok_or(CoordinationError::UnknownGroup)?;
        if generation != group.generation {
            return Err(CoordinationError::IllegalGeneration);
        }
        let member = group.members.get_mut(member_id).ok_or(CoordinationError::UnknownMember)?;
        if member.generation != generation {
            return Err(CoordinationError::IllegalGeneration);
        }
        member.alive = true;
        Ok(())
    }

    /// Removes a member and forces the next join to create a new generation.
    pub fn leave(
        &mut self,
        group_id: &[u8],
        member_id: &[u8],
        generation: i32,
    ) -> Result<(), CoordinationError> {
        let group = self.groups.get_mut(group_id).ok_or(CoordinationError::UnknownGroup)?;
        if generation != group.generation {
            return Err(CoordinationError::IllegalGeneration);
        }
        group.members.remove(member_id).ok_or(CoordinationError::UnknownMember)?;
        group.generation = group.generation.saturating_add(1).max(1);
        Ok(())
    }

    /// Removes a member when handling Kafka LeaveGroup v0, whose request does
    /// not carry a generation. The removal itself still advances generation.
    pub fn leave_any(
        &mut self,
        group_id: &[u8],
        member_id: &[u8],
    ) -> Result<(), CoordinationError> {
        let group = self.groups.get_mut(group_id).ok_or(CoordinationError::UnknownGroup)?;
        group.members.remove(member_id).ok_or(CoordinationError::UnknownMember)?;
        group.generation = group.generation.saturating_add(1).max(1);
        Ok(())
    }

    pub fn commit_offset(
        &mut self,
        group_id: &[u8],
        generation: i32,
        topic: &[u8],
        partition: i32,
        offset: i64,
    ) -> Result<(), CoordinationError> {
        let group = self.groups.get_mut(group_id).ok_or(CoordinationError::UnknownGroup)?;
        if generation != group.generation {
            return Err(CoordinationError::IllegalGeneration);
        }
        if partition < 0 || offset < 0 {
            return Err(CoordinationError::InvalidId);
        }
        group.offsets.insert((Vec::from(topic), partition), offset);
        Ok(())
    }

    pub fn offset(
        &self,
        group_id: &[u8],
        topic: &[u8],
        partition: i32,
    ) -> Result<Option<i64>, CoordinationError> {
        let group = self.groups.get(group_id).ok_or(CoordinationError::UnknownGroup)?;
        Ok(group.offsets.get(&(Vec::from(topic), partition)).copied())
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use super::*;

    #[test]
    fn producer_epoch_fences_previous_instance_and_requires_enlistment() {
        let mut coordinator = TransactionCoordinator::new();
        let first = coordinator.init(b"orders").expect("first identity");
        coordinator.add_partition(b"orders", first, b"events", 0).expect("enlist");
        assert_eq!(coordinator.validate_produce(b"orders", first, b"events", 0), Ok(()));
        let second = coordinator.init(b"orders").expect("fenced identity");
        assert_eq!(second.id, first.id);
        assert!(second.epoch > first.epoch);
        assert_eq!(
            coordinator.validate_produce(b"orders", first, b"events", 0),
            Err(CoordinationError::ProducerFenced)
        );
        assert_eq!(
            coordinator.validate_produce(b"orders", second, b"events", 0),
            Err(CoordinationError::InvalidTransactionState)
        );
        coordinator.add_partition(b"orders", second, b"events", 0).expect("re-enlist");
        let completion = coordinator.end(b"orders", second, true).expect("complete");
        assert_eq!(completion.partitions, vec![(b"events".to_vec(), 0)]);
        assert!(completion.offsets.is_empty());
        let third = coordinator.init(b"orders").expect("retain producer id");
        assert_eq!(third.id, second.id);
        assert!(third.epoch > second.epoch);
    }

    #[test]
    fn group_assignment_and_generation_fencing_are_deterministic() {
        let mut coordinator = GroupCoordinator::new();
        let partitions = vec![(b"events".to_vec(), 0), (b"events".to_vec(), 1)];
        let (generation, assignments) = coordinator
            .join(b"workers", b"a", vec![b"events".to_vec()], &partitions)
            .expect("join a");
        assert_eq!(generation, 1);
        assert_eq!(assignments[0].partitions.len(), 2);
        let (generation, assignments) = coordinator
            .join(b"workers", b"b", vec![b"events".to_vec()], &partitions)
            .expect("join b");
        assert_eq!(generation, 2);
        assert_eq!(assignments[0].partitions.len(), 1);
        assert_eq!(assignments[1].partitions.len(), 1);
        assert_eq!(
            coordinator.heartbeat(b"workers", b"a", 1),
            Err(CoordinationError::IllegalGeneration)
        );
        coordinator.commit_offset(b"workers", generation, b"events", 0, 1).expect("commit");
        assert_eq!(coordinator.offset(b"workers", b"events", 0), Ok(Some(1)));
    }
}

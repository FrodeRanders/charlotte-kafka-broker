//! In-memory topic catalog.
//!
//! The catalog stores only what the broker needs to validate routing and answer
//! Metadata requests: a topic name and its partition count. Placement,
//! replication, and durable catalog state belong to the runtime and cluster
//! layers.

use alloc::{
    collections::BTreeMap,
    vec::Vec,
};

use crate::error::LogError;

/// Topic description reported by metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicMetadata {
    /// Topic name.
    pub name: Vec<u8>,
    /// Number of partitions.
    pub partitions: i32,
}

/// Topic name to partition-count map.
#[derive(Debug, Default)]
pub struct TopicCatalog {
    topics: BTreeMap<Vec<u8>, i32>,
}

impl TopicCatalog {
    /// Creates an empty catalog.
    pub const fn new() -> Self {
        Self {
            topics: BTreeMap::new(),
        }
    }

    /// Adds a topic.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::InvalidTopic`] for an empty name or a non-positive
    /// partition count and [`LogError::DuplicateTopic`] if the name exists.
    pub fn create(&mut self, name: &[u8], partitions: i32) -> Result<(), LogError> {
        if name.is_empty() || partitions <= 0 {
            return Err(LogError::InvalidTopic);
        }
        if self.topics.contains_key(name) {
            return Err(LogError::DuplicateTopic);
        }
        self.topics.insert(Vec::from(name), partitions);
        Ok(())
    }

    /// Partition count for a topic, if present.
    pub fn partition_count(&self, name: &[u8]) -> Option<i32> {
        self.topics.get(name).copied()
    }

    /// Whether a topic exists.
    pub fn contains(&self, name: &[u8]) -> bool {
        self.topics.contains_key(name)
    }

    /// Number of topics.
    pub fn len(&self) -> usize {
        self.topics.len()
    }

    /// Whether the catalog has no topics.
    pub fn is_empty(&self) -> bool {
        self.topics.is_empty()
    }

    /// Validates a topic and partition index against the catalog.
    pub fn check_partition(&self, topic: &[u8], partition: i32) -> Result<(), LogError> {
        match self.partition_count(topic) {
            None => Err(LogError::UnknownTopic),
            Some(count) if partition < 0 || partition >= count => Err(LogError::UnknownPartition),
            Some(_) => Ok(()),
        }
    }

    /// Metadata for every topic, ordered by name.
    pub fn metadata(&self) -> Vec<TopicMetadata> {
        self.topics
            .iter()
            .map(|(name, partitions)| TopicMetadata {
                name: name.clone(),
                partitions: *partitions,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_rejects_empty_duplicate_and_zero_partition_topics() {
        let mut catalog = TopicCatalog::new();
        assert_eq!(catalog.create(b"", 1), Err(LogError::InvalidTopic));
        assert_eq!(catalog.create(b"events", 0), Err(LogError::InvalidTopic));
        assert_eq!(catalog.create(b"events", 3), Ok(()));
        assert_eq!(catalog.create(b"events", 3), Err(LogError::DuplicateTopic));
        assert!(catalog.contains(b"events"));
        assert_eq!(catalog.partition_count(b"events"), Some(3));
        assert_eq!(catalog.len(), 1);
    }

    #[test]
    fn check_partition_distinguishes_unknown_topic_and_partition() {
        let mut catalog = TopicCatalog::new();
        catalog.create(b"events", 2).expect("create");
        assert_eq!(catalog.check_partition(b"events", 0), Ok(()));
        assert_eq!(catalog.check_partition(b"events", 1), Ok(()));
        assert_eq!(catalog.check_partition(b"events", 2), Err(LogError::UnknownPartition));
        assert_eq!(catalog.check_partition(b"events", -1), Err(LogError::UnknownPartition));
        assert_eq!(catalog.check_partition(b"missing", 0), Err(LogError::UnknownTopic));
    }

    #[test]
    fn metadata_is_ordered_by_name() {
        let mut catalog = TopicCatalog::new();
        catalog.create(b"results", 1).expect("create results");
        catalog.create(b"events", 2).expect("create events");
        let metadata = catalog.metadata();
        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0].name, b"events");
        assert_eq!(metadata[0].partitions, 2);
        assert_eq!(metadata[1].name, b"results");
        assert_eq!(metadata[1].partitions, 1);
    }
}

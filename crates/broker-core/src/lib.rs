//! Deterministic, transport-free state for the CharlotteOS Kafka broker.
//!
//! This crate owns the partition log and topic catalog and nothing else: no
//! threads, channels, clocks, or I/O. That keeps record and offset semantics
//! testable in isolation and lets the same code run under `sitas-unix` on a
//! host and under `sitas-charlotte` at EL0.

#![no_std]

extern crate alloc;

pub mod coordination;
pub mod error;
pub mod log;
pub mod topic;

pub use coordination::{
    CoordinationError,
    GroupAssignment,
    GroupCoordinator,
    ProducerIdentity,
    TransactionCompletion,
    TransactionCoordinator,
    TransactionOffset,
    TransactionState,
};
pub use error::LogError;
pub use log::{
    FetchWindow,
    PartitionLog,
    Record,
    RecordData,
    RecordInput,
};
pub use topic::{
    TopicCatalog,
    TopicMetadata,
};

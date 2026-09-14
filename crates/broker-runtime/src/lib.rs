//! Sitas shard services for the CharlotteOS Kafka broker.
//!
//! [`Broker`] owns one mailbox per partition shard and routes produce, fetch,
//! list-offset, and metadata operations to the shard that owns the requested
//! partition. Only the owning shard mutates its logs; callers block on a
//! bounded reply channel and park between polls. Transaction and consumer
//! group state is single-writer coordination state hosted by shard zero and
//! exposed through typed methods on [`Broker`].
//!
//! This crate is `no_std + alloc`: on a host it runs over `sitas-unix`, and at
//! EL0 it runs over `sitas-charlotte`. It never touches sockets or storage
//! directly.

#![no_std]

extern crate alloc;

mod broker;
mod error;
mod message;
mod partition;
mod session;

pub use broker::{
    Broker,
    BrokerConfig,
    DEFAULT_MAILBOX_CAPACITY,
    DEFAULT_PARK_TIMEOUT,
    TopicSpec,
};
pub use broker_core::{
    CoordinationError,
    GroupAssignment,
    ProducerIdentity,
    TransactionOffset,
};
pub use error::BrokerError;
pub use session::{
    SessionId,
    SessionShardLayout,
};

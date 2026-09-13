//! Transport-free dispatch between the Kafka wire subset and broker shards.
//!
//! [`Engine`] decodes a request frame with `broker-wire`, calls the owning
//! shard through `broker-runtime`, and encodes the response. It owns no
//! sockets and no threads, so the same dispatch code can serve the host TCP
//! front end and, later, the CharlotteOS EL0 service.

#![no_std]

extern crate alloc;

mod engine;

pub use engine::{
    BrokerIdentity,
    DEFAULT_MAX_FETCH_BYTES,
    DEFAULT_MAX_FETCH_RECORDS,
    Engine,
    EngineConfig,
};

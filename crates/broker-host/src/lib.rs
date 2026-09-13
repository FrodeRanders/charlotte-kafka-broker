//! Host-side front end and conformance tests for the broker crates.
//!
//! [`server`] adds a plain TCP listener that feeds the engine on a host; the
//! conformance tests drive that listener with the real CharlotteOS Kafka
//! client codec. The service code under test remains the same `no_std` code
//! that will run over `sitas-charlotte` at EL0.

pub mod server;

pub use server::{
    ServerHandle,
    start,
};

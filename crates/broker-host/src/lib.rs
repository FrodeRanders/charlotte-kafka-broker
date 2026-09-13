//! Host-side test harness for the broker crates.
//!
//! This crate exists so `broker-runtime` can be exercised over the
//! `sitas-unix` backend with plain `cargo test`. The service code under test
//! is the same `no_std` code that will run over `sitas-charlotte` at EL0; the
//! tests live in `tests/`.

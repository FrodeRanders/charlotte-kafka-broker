//! Bounded server-side Kafka wire subset for the CharlotteOS broker.
//!
//! The supported request versions mirror the ones the CharlotteOS Kafka
//! connector already uses (`charlotte-kafka`), so the connector can talk to
//! this broker without a second dialect. See `docs/architecture.md` for the
//! version table and framing rules.
//!
//! This crate is transport-free: it turns a request frame into typed data and
//! typed results back into a response frame, and never touches a socket.

#![no_std]

extern crate alloc;

mod codec;
pub mod protocol;
pub mod record_batch;
pub mod request;
pub mod response;

pub use protocol::Error;
pub use record_batch::{
    crc32c,
    decode_record_batches,
    encode_record_batch,
};
pub use request::{
    FetchPartition,
    FetchTopic,
    Header,
    ListOffsetsPartition,
    ListOffsetsTopic,
    ProducePartition,
    ProduceTopic,
    Request,
    RequestBody,
    decode_request,
};
pub use response::{
    BrokerMetadata,
    FetchPartitionResult,
    FetchResponse,
    FetchTopicResult,
    ListOffsetsPartitionResult,
    ListOffsetsResponse,
    ListOffsetsTopicResult,
    MetadataResponse,
    PartitionMetadata,
    ProducePartitionResult,
    ProduceResponse,
    ProduceTopicResult,
    TopicMetadata,
    encode_api_versions,
    encode_fetch,
    encode_list_offsets,
    encode_metadata,
    encode_produce,
};

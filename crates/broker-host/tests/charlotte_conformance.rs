//! Conformance tests against the real CharlotteOS Kafka client codec.
//!
//! Every request is built by `charlotte-kafka` and every response is parsed by
//! `charlotte-kafka`, over the host TCP front end. This proves the broker
//! speaks the exact bounded versions the connector already uses instead of
//! only round-tripping through this repository's own decoder.

use std::{
    io::{
        Read,
        Write,
    },
    net::TcpStream,
    sync::Arc,
    time::Duration,
};

use broker_engine::{
    BrokerIdentity,
    Engine,
    EngineConfig,
};
use broker_host::{
    ServerHandle,
    start,
};
use broker_runtime::{
    Broker,
    BrokerConfig,
    TopicSpec,
};
use broker_wire::protocol::{
    OFFSET_OUT_OF_RANGE,
    UNSUPPORTED_VERSION,
};
use charlotte_kafka as client;
use sitas_unix::UnixRuntime;

const EVENTS: &[u8] = b"events";

struct Harness {
    _runtime: UnixRuntime,
    engine: Arc<Engine>,
    server: ServerHandle,
    stream: TcpStream,
}

fn harness() -> Harness {
    let runtime = UnixRuntime::new();
    let broker = Broker::start(
        &runtime,
        BrokerConfig::new(
            2,
            vec![TopicSpec {
                name: EVENTS.to_vec(),
                partitions: 2,
            }],
        ),
    )
    .expect("start broker");
    let engine = Arc::new(Engine::new(
        broker,
        EngineConfig::new(BrokerIdentity::new(1, b"127.0.0.1", 9092)),
    ));
    let server =
        start(Arc::clone(&engine), "127.0.0.1:0".parse().expect("address")).expect("start server");
    let stream = TcpStream::connect(server.local_addr()).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(5))).expect("read timeout");
    Harness {
        _runtime: runtime,
        engine,
        server,
        stream,
    }
}

impl Harness {
    fn round_trip(&mut self, request: &[u8]) -> Vec<u8> {
        self.stream.write_all(request).expect("write request");
        let mut prefix = [0u8; 4];
        self.stream.read_exact(&mut prefix).expect("read length");
        let length = i32::from_be_bytes(prefix);
        assert!(length > 0, "response length must be positive");
        let mut response = vec![0u8; 4 + length as usize];
        response[..4].copy_from_slice(&prefix);
        self.stream.read_exact(&mut response[4..]).expect("read response");
        response
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.engine.broker().shutdown();
        let _ = &self.server;
    }
}

fn record_batch(value: &[u8]) -> Vec<u8> {
    client::encode_record_batch(
        &[client::RecordInput {
            timestamp_ms: 1_000,
            key: Some(b"k"),
            value: Some(value),
        }],
        client::ProducerIdentity {
            producer_id: -1,
            producer_epoch: -1,
        },
        -1,
        false,
    )
    .expect("record batch")
}

#[test]
fn api_versions_conforms_to_the_client_codec() {
    let mut harness = harness();
    let request = client::api_versions_request(7, b"conformance").expect("request");
    let response = harness.round_trip(&request);
    let parsed = client::parse_api_versions(&response, 7).expect("parse");
    assert_eq!(parsed.error, client::NO_ERROR);
    for (api_key, version) in [
        (client::api::PRODUCE, client::version::PRODUCE),
        (client::api::FETCH, client::version::FETCH),
        (client::api::LIST_OFFSETS, client::version::LIST_OFFSETS),
        (client::api::METADATA, client::version::METADATA),
    ] {
        assert!(parsed.supports(api_key, version), "api {api_key} v{version} not advertised");
    }
    assert!(!parsed.supports(client::api::JOIN_GROUP, client::version::JOIN_GROUP));
}

#[test]
fn metadata_conforms_to_the_client_codec() {
    let mut harness = harness();
    let topics: &[&[u8]] = &[EVENTS, b"missing"];
    let request = client::metadata_request_many(11, b"conformance", topics).expect("request");
    let response = harness.round_trip(&request);
    let batch = client::parse_metadata_many(&response, 11).expect("parse");

    assert_eq!(batch.brokers.len(), 1);
    assert_eq!(batch.brokers[0].node_id, 1);
    assert_eq!(batch.brokers[0].host, "127.0.0.1");
    assert_eq!(batch.brokers[0].port, 9092);

    assert_eq!(batch.topics.len(), 2);
    let events = batch.topics.iter().find(|topic| topic.topic == EVENTS).expect("events");
    assert_eq!(events.error, client::NO_ERROR);
    assert_eq!(events.partitions.len(), 2);
    assert!(events.partitions.iter().all(|partition| partition.leader == 1));

    let missing = batch.topics.iter().find(|topic| topic.topic == b"missing").expect("missing");
    assert_eq!(missing.error, client::UNKNOWN_TOPIC_OR_PARTITION);
}

#[test]
fn produce_and_fetch_conform_to_the_client_codec() {
    let mut harness = harness();

    let batch = record_batch(b"first");
    let request = client::produce_request(1, b"conformance", None, EVENTS, 0, &batch, 30_000)
        .expect("request");
    let response = harness.round_trip(&request);
    let produced = client::parse_produce(&response, 1, EVENTS, 0).expect("parse");
    assert_eq!(produced.error, client::NO_ERROR);
    assert_eq!(produced.base_offset, 0);

    let fetch = client::Fetch {
        topic: EVENTS,
        partition: 0,
        offset: 0,
        max_wait_ms: 0,
        max_bytes: 65_536,
        read_committed: true,
    };
    let request = client::fetch_request(2, b"conformance", fetch).expect("request");
    let response = harness.round_trip(&request);
    let fetched = client::parse_fetch(&response, 2, EVENTS, 0).expect("parse");
    assert_eq!(fetched.error, client::NO_ERROR);
    assert_eq!(fetched.high_watermark, 1);
    assert_eq!(fetched.last_stable_offset, 1);
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].key.as_deref(), Some(b"k".as_slice()));
    assert_eq!(fetched.records[0].value.as_deref(), Some(b"first".as_slice()));
}

#[test]
fn list_offsets_conforms_to_the_client_codec() {
    let mut harness = harness();

    let request =
        client::list_offsets_request(3, b"conformance", EVENTS, 1, false).expect("request");
    let response = harness.round_trip(&request);
    assert_eq!(client::parse_list_offsets(&response, 3, EVENTS, 1).expect("parse"), 0);

    let batch = record_batch(b"second");
    let request = client::produce_request(4, b"conformance", None, EVENTS, 1, &batch, 30_000)
        .expect("request");
    let response = harness.round_trip(&request);
    assert_eq!(client::parse_produce(&response, 4, EVENTS, 1).expect("parse").base_offset, 0);

    let request =
        client::list_offsets_request(5, b"conformance", EVENTS, 1, false).expect("request");
    let response = harness.round_trip(&request);
    assert_eq!(client::parse_list_offsets(&response, 5, EVENTS, 1).expect("parse"), 1);

    let request =
        client::list_offsets_request(6, b"conformance", EVENTS, 1, true).expect("request");
    let response = harness.round_trip(&request);
    assert_eq!(client::parse_list_offsets(&response, 6, EVENTS, 1).expect("parse"), 0);
}

#[test]
fn per_partition_errors_are_reported_in_the_response() {
    let mut harness = harness();

    let batch = record_batch(b"x");
    let request = client::produce_request(1, b"conformance", None, EVENTS, 9, &batch, 30_000)
        .expect("request");
    let response = harness.round_trip(&request);
    let produced = client::parse_produce(&response, 1, EVENTS, 9).expect("parse");
    assert_eq!(produced.error, client::UNKNOWN_TOPIC_OR_PARTITION);

    let fetch = client::Fetch {
        topic: EVENTS,
        partition: 0,
        offset: 5,
        max_wait_ms: 0,
        max_bytes: 65_536,
        read_committed: false,
    };
    let request = client::fetch_request(2, b"conformance", fetch).expect("request");
    let response = harness.round_trip(&request);
    let fetched = client::parse_fetch(&response, 2, EVENTS, 0).expect("parse");
    assert_eq!(fetched.error, OFFSET_OUT_OF_RANGE);
}

#[test]
fn transactional_produce_is_rejected() {
    let mut harness = harness();
    let batch = record_batch(b"x");
    let request =
        client::produce_request(1, b"conformance", Some(b"txn"), EVENTS, 0, &batch, 30_000)
            .expect("request");
    let response = harness.round_trip(&request);
    let produced = client::parse_produce(&response, 1, EVENTS, 0).expect("parse");
    assert_eq!(produced.error, UNSUPPORTED_VERSION);
}

#[test]
fn malformed_request_closes_the_connection() {
    let mut harness = harness();

    let mut frame = client::api_versions_request(9, b"conformance").expect("request");
    frame[4..6].copy_from_slice(&0x7f00i16.to_be_bytes());
    harness.stream.write_all(&frame).expect("write");

    let mut byte = [0u8; 1];
    assert!(
        harness.stream.read_exact(&mut byte).is_err(),
        "server must close after an unsupported API key"
    );
}

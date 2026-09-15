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

fn transactional_record_batch(value: &[u8], producer: client::ProducerIdentity) -> Vec<u8> {
    client::encode_record_batch(
        &[client::RecordInput {
            timestamp_ms: 1_000,
            key: Some(b"k"),
            value: Some(value),
        }],
        producer,
        -1,
        true,
    )
    .expect("transactional record batch")
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
    assert!(parsed.supports(client::api::JOIN_GROUP, client::version::JOIN_GROUP));
    assert!(parsed.supports(client::api::SYNC_GROUP, client::version::SYNC_GROUP));
    assert!(parsed.supports(client::api::HEARTBEAT, client::version::HEARTBEAT));
    assert!(parsed.supports(client::api::LEAVE_GROUP, client::version::LEAVE_GROUP));
    assert!(parsed.supports(client::api::OFFSET_COMMIT, client::version::OFFSET_COMMIT));
    assert!(parsed.supports(client::api::OFFSET_FETCH, client::version::OFFSET_FETCH));
    assert!(parsed.supports(client::api::FIND_COORDINATOR, client::version::FIND_COORDINATOR));
    assert!(parsed.supports(client::api::INIT_PRODUCER_ID, client::version::INIT_PRODUCER_ID));
    assert!(
        parsed.supports(client::api::ADD_PARTITIONS_TO_TXN, client::version::ADD_PARTITIONS_TO_TXN)
    );
    assert!(parsed.supports(client::api::END_TXN, client::version::END_TXN));
}

#[test]
fn coordinator_and_transaction_identity_conform_to_client_codec() {
    let mut harness = harness();
    let request = client::find_coordinator_request(20, b"conformance", b"orders", true)
        .expect("find coordinator request");
    let response = harness.round_trip(&request);
    let coordinator = client::parse_find_coordinator(&response, 20).expect("parse coordinator");
    assert_eq!(coordinator.error, client::NO_ERROR);
    assert_eq!(coordinator.node_id, 1);
    assert_eq!(coordinator.host, "127.0.0.1");
    assert_eq!(coordinator.port, 9092);

    let request = client::init_producer_id_request(21, b"conformance", Some(b"orders"), 30_000)
        .expect("init producer request");
    let response = harness.round_trip(&request);
    let producer = client::parse_init_producer_id(&response, 21).expect("parse producer");
    assert!(producer.producer_id > 0);
    assert_eq!(producer.producer_epoch, 0);
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

    // A caught-up consumer fetches at the high watermark and must receive an
    // empty record set, not a null one: null record sets break kafka-python.
    let fetch = client::Fetch {
        topic: EVENTS,
        partition: 0,
        offset: 1,
        max_wait_ms: 0,
        max_bytes: 65_536,
        read_committed: true,
    };
    let request = client::fetch_request(3, b"conformance", fetch).expect("request");
    let response = harness.round_trip(&request);
    let fetched = client::parse_fetch(&response, 3, EVENTS, 0).expect("parse");
    assert_eq!(fetched.error, client::NO_ERROR);
    assert_eq!(fetched.high_watermark, 1);
    assert!(fetched.records.is_empty());
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
fn transactional_lifecycle_conforms_to_client_codec() {
    let mut harness = harness();
    let init = client::init_producer_id_request(30, b"conformance", Some(b"txn"), 30_000)
        .expect("init request");
    let identity =
        client::parse_init_producer_id(&harness.round_trip(&init), 30).expect("init response");
    let enlist =
        client::add_partitions_to_txn_request(31, b"conformance", b"txn", identity, EVENTS, 0)
            .expect("enlist request");
    assert!(client::parse_partition_error(&harness.round_trip(&enlist), 31, EVENTS, 0).is_ok());
    let batch = transactional_record_batch(b"transactional", identity);
    let produce =
        client::produce_request(32, b"conformance", Some(b"txn"), EVENTS, 0, &batch, 30_000)
            .expect("produce request");
    assert_eq!(
        client::parse_produce(&harness.round_trip(&produce), 32, EVENTS, 0)
            .expect("produce response")
            .error,
        client::NO_ERROR
    );
    let end =
        client::end_txn_request(33, b"conformance", b"txn", identity, true).expect("end request");
    assert!(client::parse_top_level_error(&harness.round_trip(&end), 33).is_ok());
}

#[test]
fn consumer_group_lifecycle_conforms_to_client_codec() {
    let mut harness = harness();
    let subscription = client::fixed_subscription(EVENTS, 0).expect("subscription");
    let join = client::join_group_request(
        40,
        b"conformance",
        b"workers",
        10_000,
        10_000,
        b"",
        &subscription,
    )
    .expect("join request");
    let joined = client::parse_join_group(&harness.round_trip(&join), 40).expect("join response");
    assert_eq!(joined.error, client::NO_ERROR);
    let assignment = client::fixed_assignment(EVENTS, Some(0)).expect("assignment");
    let sync = client::sync_group_request(
        41,
        b"conformance",
        b"workers",
        joined.generation,
        &joined.member_id,
        &[client::GroupAssignment {
            member_id: joined.member_id.clone(),
            assignment,
        }],
    )
    .expect("sync request");
    let (error, _) =
        client::parse_sync_group(&harness.round_trip(&sync), 41).expect("sync response");
    assert_eq!(error, client::NO_ERROR);
    let heartbeat = client::heartbeat_request(
        42,
        b"conformance",
        b"workers",
        joined.generation,
        &joined.member_id,
    )
    .expect("heartbeat request");
    assert_eq!(
        client::parse_group_error(&harness.round_trip(&heartbeat), 42).expect("heartbeat response"),
        client::NO_ERROR
    );
    let commit = client::offset_commit_request(
        43,
        b"conformance",
        client::OffsetCommit {
            group_id: b"workers",
            generation: joined.generation,
            member_id: &joined.member_id,
            topic: EVENTS,
            partition: 0,
            next_offset: 7,
        },
    )
    .expect("offset commit request");
    assert!(client::parse_offset_commit(&harness.round_trip(&commit), 43, EVENTS, 0).is_ok());
    let fetch = client::offset_fetch_request(44, b"conformance", b"workers", EVENTS, 0)
        .expect("offset fetch request");
    assert_eq!(
        client::parse_offset_fetch(&harness.round_trip(&fetch), 44, EVENTS, 0)
            .expect("offset fetch response"),
        Some(7)
    );
    let leave = client::leave_group_request(45, b"conformance", b"workers", &joined.member_id)
        .expect("leave request");
    assert_eq!(
        client::parse_group_error(&harness.round_trip(&leave), 45).expect("leave response"),
        client::NO_ERROR
    );
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

#[test]
fn unsupported_api_versions_version_returns_a_downgrade_response() {
    let mut harness = harness();

    // A modern client may probe a version we do not implement. Kafka expects
    // a v0 body with UNSUPPORTED_VERSION and the advertised list so the client
    // can retry with a version the broker accepts.
    let correlation: i32 = 21;
    let mut body = Vec::new();
    body.extend_from_slice(&client::api::API_VERSIONS.to_be_bytes());
    body.extend_from_slice(&3i16.to_be_bytes());
    body.extend_from_slice(&correlation.to_be_bytes());
    body.extend_from_slice(&(-1i16).to_be_bytes());
    body.push(0);
    let mut frame = Vec::new();
    frame.extend_from_slice(&(body.len() as i32).to_be_bytes());
    frame.extend_from_slice(&body);

    let response = harness.round_trip(&frame);
    let parsed = client::parse_api_versions(&response, correlation).expect("parse");
    assert_eq!(parsed.error, UNSUPPORTED_VERSION);
    assert!(parsed.versions.iter().any(|entry| {
        entry.api_key == client::api::PRODUCE
            && entry.min <= client::version::PRODUCE
            && client::version::PRODUCE <= entry.max
    }));
}

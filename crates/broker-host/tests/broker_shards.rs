use std::{
    sync::Arc,
    vec,
    vec::Vec,
};

use broker_core::{
    LogError,
    RecordData,
};
use broker_runtime::{
    Broker,
    BrokerConfig,
    BrokerError,
    TopicSpec,
};
use sitas_unix::UnixRuntime;

fn record(value: &[u8]) -> RecordData {
    RecordData {
        timestamp_ms: 1_000,
        key: None,
        value: Some(Vec::from(value)),
    }
}

fn events_config(shard_count: usize, partitions: i32) -> BrokerConfig {
    BrokerConfig::new(
        shard_count,
        vec![TopicSpec {
            name: b"events".to_vec(),
            partitions,
        }],
    )
}

#[test]
fn produce_and_fetch_across_shards() {
    let runtime = UnixRuntime::new();
    let broker = Broker::start(&runtime, events_config(3, 6)).expect("start broker");

    for partition in 0..6i32 {
        let value = [b'p', b'0' + partition as u8];
        assert_eq!(broker.produce(b"events", partition, vec![record(&value)]), Ok(0));
    }

    for partition in 0..6i32 {
        let window = broker.fetch(b"events", partition, 0, 10, 4096).expect("fetch");
        assert_eq!(window.high_watermark, 1);
        assert_eq!(window.records.len(), 1);
        assert_eq!(window.records[0].value.as_deref(), Some(&[b'p', b'0' + partition as u8][..]));
    }

    let owners: Vec<usize> =
        (0..6).map(|partition| broker.shard_index(b"events", partition)).collect();
    assert!(
        owners.iter().any(|owner| *owner != owners[0]),
        "partitions must not all share one shard"
    );

    broker.shutdown().expect("shutdown");
}

#[test]
fn fetch_honors_offsets_and_budgets() {
    let runtime = UnixRuntime::new();
    let broker = Broker::start(&runtime, events_config(2, 2)).expect("start broker");

    assert_eq!(broker.produce(b"events", 0, vec![record(b"a"), record(b"b"), record(b"c")]), Ok(0));
    assert_eq!(broker.produce(b"events", 0, vec![record(b"d")]), Ok(3));

    let window = broker.fetch(b"events", 0, 1, 10, 4096).expect("fetch");
    assert_eq!(window.high_watermark, 4);
    assert_eq!(window.records.len(), 3);
    assert_eq!(window.records[0].offset, 1);

    let window = broker.fetch(b"events", 0, 4, 10, 4096).expect("fetch at end");
    assert!(window.records.is_empty());
    assert_eq!(window.high_watermark, 4);

    let window = broker.fetch(b"events", 0, 0, 2, 4096).expect("fetch with record budget");
    assert_eq!(window.records.len(), 2);
}

#[test]
fn unknown_topics_and_partitions_are_rejected() {
    let runtime = UnixRuntime::new();
    let broker = Broker::start(&runtime, events_config(2, 3)).expect("start broker");

    assert_eq!(
        broker.produce(b"missing", 0, vec![record(b"x")]),
        Err(BrokerError::Core(LogError::UnknownTopic))
    );
    assert_eq!(
        broker.produce(b"events", 3, vec![record(b"x")]),
        Err(BrokerError::Core(LogError::UnknownPartition))
    );
    assert_eq!(
        broker.fetch(b"events", -1, 0, 10, 4096),
        Err(BrokerError::Core(LogError::UnknownPartition))
    );
    assert_eq!(
        broker.fetch(b"events", 0, 5, 10, 4096),
        Err(BrokerError::Core(LogError::OffsetOutOfRange))
    );
}

#[test]
fn list_offsets_track_the_ends() {
    let runtime = UnixRuntime::new();
    let broker = Broker::start(&runtime, events_config(2, 2)).expect("start broker");

    assert_eq!(broker.list_offset(b"events", 1, true), Ok(0));
    assert_eq!(broker.list_offset(b"events", 1, false), Ok(0));
    assert_eq!(broker.produce(b"events", 1, vec![record(b"a")]), Ok(0));
    assert_eq!(broker.list_offset(b"events", 1, true), Ok(0));
    assert_eq!(broker.list_offset(b"events", 1, false), Ok(1));
}

#[test]
fn metadata_reports_configured_topics() {
    let runtime = UnixRuntime::new();
    let broker = Broker::start(&runtime, events_config(2, 4)).expect("start broker");

    let metadata = broker.metadata();
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].name, b"events");
    assert_eq!(metadata[0].partitions, 4);
    assert_eq!(broker.partition_count(b"events"), Some(4));
}

#[test]
fn concurrent_producers_receive_distinct_offsets() {
    let runtime = UnixRuntime::new();
    let broker = Arc::new(Broker::start(&runtime, events_config(2, 1)).expect("start broker"));

    let mut producers = Vec::new();
    for _ in 0..4 {
        let broker = Arc::clone(&broker);
        producers.push(std::thread::spawn(move || {
            let mut offsets = Vec::new();
            for _ in 0..8 {
                offsets.push(broker.produce(b"events", 0, vec![record(b"x")]).expect("produce"));
            }
            offsets
        }));
    }

    let mut offsets: Vec<i64> = producers
        .into_iter()
        .flat_map(|producer| producer.join().expect("producer panicked"))
        .collect();
    offsets.sort_unstable();
    assert_eq!(offsets, (0..32).collect::<Vec<i64>>());

    broker.shutdown().expect("shutdown");
    broker.shutdown().expect("shutdown is idempotent");
}

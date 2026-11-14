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
    CoordinationError,
    TopicSpec,
    TransactionOffset,
};
use sitas_unix::UnixRuntime;

fn record(value: &[u8]) -> RecordData {
    RecordData {
        timestamp_ms: 1_000,
        key: None,
        value: Some(Vec::from(value)),
    }
}

#[test]
fn transactions_and_groups_are_fenced_by_owned_coordinator() {
    let runtime = UnixRuntime::new();
    let broker = Broker::start(&runtime, events_config(2, 2)).expect("start broker");

    let producer = broker.init_producer(b"orders").expect("init producer");
    broker.add_transaction_partition(b"orders", producer, b"events", 0).expect("enlist partition");
    broker
        .validate_transactional_produce(b"orders", producer, b"events", 0)
        .expect("validate produce");
    assert_eq!(
        broker.produce_transactional(b"orders", producer, b"events", 0, vec![record(b"tx")]),
        Ok(0)
    );
    assert_eq!(
        broker.validate_transactional_produce(b"orders", producer, b"events", 1),
        Err(BrokerError::Coordination(CoordinationError::InvalidTransactionState))
    );
    broker.end_transaction(b"orders", producer, true).expect("commit");
    let committed =
        broker.fetch_with_isolation(b"events", 0, 0, 10, 4096, true).expect("read committed");
    assert_eq!(committed.records.len(), 1);

    let producer = broker.init_producer(b"orders").expect("new epoch");
    broker.add_transaction_partition(b"orders", producer, b"events", 0).expect("enlist second");
    broker
        .produce_transactional(b"orders", producer, b"events", 0, vec![record(b"abort")])
        .expect("append second");
    assert_eq!(
        broker
            .fetch_with_isolation(b"events", 0, 0, 10, 4096, true)
            .expect("pending hidden")
            .records
            .len(),
        1
    );
    broker.end_transaction(b"orders", producer, false).expect("abort");

    let (generation, assignments) =
        broker.join_group(b"workers", b"member-a", vec![b"events".to_vec()]).expect("join group");
    assert_eq!(assignments[0].partitions.len(), 2);
    broker.commit_group_offset(b"workers", generation, b"events", 0, 1).expect("commit offset");
    assert_eq!(broker.group_offset(b"workers", b"events", 0), Ok(Some(1)));
    let producer = broker.init_producer(b"read-process-write").expect("init offset producer");
    broker
        .add_transaction_partition(b"read-process-write", producer, b"events", 1)
        .expect("enlist output");
    broker
        .add_transaction_offset(
            b"read-process-write",
            producer,
            TransactionOffset {
                group_id: b"workers".to_vec(),
                generation,
                topic: b"events".to_vec(),
                partition: 0,
                offset: 2,
            },
        )
        .expect("enlist offset");
    broker
        .produce_transactional(
            b"read-process-write",
            producer,
            b"events",
            1,
            vec![record(b"result")],
        )
        .expect("produce result");
    broker
        .end_transaction(b"read-process-write", producer, true)
        .expect("commit output and offset");
    assert_eq!(broker.group_offset(b"workers", b"events", 0), Ok(Some(2)));
    assert_eq!(
        broker.heartbeat_group(b"workers", b"member-a", generation - 1),
        Err(BrokerError::Coordination(CoordinationError::IllegalGeneration))
    );
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

#[test]
fn shutdown_acknowledgements_do_not_replace_worker_joins() {
    use core::time::Duration;
    use std::sync::{
        Barrier,
        atomic::{
            AtomicBool,
            AtomicUsize,
            Ordering,
        },
    };

    use sitas_core::{
        placement::ShardPlacement,
        shard::ShardId,
        shard_runtime::{
            ShardChannelResult,
            ShardParker,
            ShardRuntime,
        },
    };
    struct DelayedExit {
        inner: UnixRuntime,
        arrived: Arc<AtomicUsize>,
        release: Arc<Barrier>,
    }
    impl ShardRuntime for DelayedExit {
        type JoinHandle<T: Send + 'static> = <UnixRuntime as ShardRuntime>::JoinHandle<T>;
        type Reactor = <UnixRuntime as ShardRuntime>::Reactor;

        fn spawn_shard<T: Send + 'static>(
            &self,
            id: ShardId,
            placement: ShardPlacement,
            entry: Box<dyn FnOnce() -> T + Send>,
        ) -> Self::JoinHandle<T> {
            let arrived = self.arrived.clone();
            let release = self.release.clone();
            self.inner.spawn_shard(
                id,
                placement,
                Box::new(move || {
                    let result = entry();
                    arrived.fetch_add(1, Ordering::Release);
                    release.wait(); // The protocol acknowledgement has already been sent.
                    result
                }),
            )
        }

        fn channel<M: Send + 'static>(&self, capacity: usize) -> ShardChannelResult<M> {
            self.inner.channel(capacity)
        }

        fn sleep(&self, duration: Duration) {
            self.inner.sleep(duration);
        }

        fn parker(&self) -> Arc<dyn ShardParker> {
            self.inner.parker()
        }

        fn shard_reactor(&self, id: ShardId) -> Self::Reactor {
            self.inner.shard_reactor(id)
        }
    }
    let arrived = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Barrier::new(3));
    let runtime = DelayedExit {
        inner: UnixRuntime::new(),
        arrived: arrived.clone(),
        release: release.clone(),
    };
    let broker = Broker::start(&runtime, events_config(2, 2)).unwrap();
    let finished = Arc::new(AtomicBool::new(false));
    let observed = finished.clone();
    let shutdown = std::thread::spawn(move || {
        broker.shutdown().unwrap();
        observed.store(true, Ordering::Release);
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while arrived.load(Ordering::Acquire) != 2 {
        assert!(std::time::Instant::now() < deadline, "workers did not reach exit fence");
        std::thread::yield_now();
    }
    assert!(!finished.load(Ordering::Acquire), "acknowledgement was mistaken for exit");
    release.wait();
    shutdown.join().unwrap();
    assert!(finished.load(Ordering::Acquire));
}

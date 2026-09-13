# charlotte-kafka-broker

A shard-per-partition, Kafka-compatible log broker for
[CharlotteOS](https://github.com/FrodeRanders/charlotte-os), structured around
the [Sitas](https://github.com/FrodeRanders/sitas) shared-nothing runtime. This
is a research application, not a production broker. Its purpose is to exercise
the CharlotteOS machinery with a workload that has real concurrency, storage,
and distribution semantics instead of another single-threaded smoke service:

- **sharding:** one partition log is owned by exactly one Sitas shard; all
  cross-shard interaction is typed, owned messages;
- **asynchrony:** parked shards, bounded mailboxes, and completion-driven
  wakeups rather than polling;
- **persistence:** partition logs that will move from memory to a segmented
  format on the CharlotteOS block/object-store capabilities;
- **distribution:** partition leaders and replicas placed across cluster
  members, with the deployment, naming, and Raft services already present in
  CharlotteOS;
- **capability discipline:** the broker receives only the narrow transport,
  storage, and clock capabilities its role requires.

The broker is deliberately small: no query language, no general transaction
engine, and no POSIX compatibility layer. It appends records, assigns offsets,
serves fetches, and reports metadata. That is enough to demonstrate a real
distributed system on top of the kernel, and it is the substrate a later
database can reuse.

## Layers

```text
broker-wire     bounded Kafka subset codec (server side)
      |
broker-engine   request dispatch and Kafka error mapping
      |
broker-runtime  Sitas shard services: partition owners, routing, replies
      |
broker-core     transport-free partition log and topic catalog
```

`broker-core`, `broker-wire`, `broker-engine`, and `broker-runtime` are
`no_std + alloc` and never depend on a particular transport, runtime, or
storage backend. `broker-host` adds a plain TCP front end and drives the whole
stack with the real CharlotteOS Kafka client codec. The same service code is
intended to run under `sitas-unix` on a host for tests and under
`sitas-charlotte` at EL0.

## Restricted protocol subset

The wire subset mirrors the request versions already implemented by the
CharlotteOS Kafka connector (`charlotte-kafka`), so the connector can talk to
this broker without a separate protocol dialect.

| API | Key | Version | Request | Response |
|---|---|---|---|---|
| ApiVersions | 18 | 0 | empty | advertised subset |
| Metadata | 3 | 1 | topic list or all | brokers, controller, topics, partitions |
| Produce | 0 | 3 | record batch v2 per partition | error, base offset, append time |
| Fetch | 1 | 4 | offset, max wait/bytes, isolation | high watermark, LSO, record batch v2 |
| ListOffsets | 2 | 1 | timestamp (-2 earliest, -1 latest) | timestamp, offset |

Deliberately not implemented yet: consumer groups and coordinator (OffsetCommit,
OffsetFetch, FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup),
idempotent/transactional producer APIs (InitProducerId, AddPartitionsToTxn,
AddOffsetsToTxn, EndTxn, TxnOffsetCommit), SASL/SCRAM and mTLS listeners,
compression, record headers, and dynamic topic creation.

## Layout

| Crate | Purpose |
|---|---|
| `crates/broker-core` | Partition log, record model, topic catalog; pure and transport-free |
| `crates/broker-wire` | Bounded Kafka subset decode/encode, including record batch v2 |
| `crates/broker-engine` | Transport-free request dispatch and Kafka error mapping |
| `crates/broker-runtime` | Sitas shard services and the user-facing `Broker` API |
| `crates/broker-host` | Host TCP front end and `charlotte-kafka` conformance tests |

## Status

- [x] Workspace, pinned toolchain, host test lane
- [x] `broker-core`: in-memory append-only partition logs with offsets
- [x] `broker-runtime`: shard-per-partition services over typed mailboxes
- [x] `broker-wire`: restricted request decode and response encode
- [x] `broker-engine`: dispatch and per-partition error mapping
- [x] Host TCP front end and `charlotte-kafka` conformance tests
- [ ] EL0 service on CharlotteOS with a `tcpip` socket capability
- [ ] Segmented, durable partition logs over the block/object-store protocol
- [ ] Partition placement and Raft-replicated logs across cluster members

## Build and test

```sh
cargo fmt --all -- --check
cargo test
cargo clippy --all-targets -- -D warnings
cargo doc --no-deps
cargo run --example host_broker   # TCP broker on 127.0.0.1:9092
```

The conformance tests in `crates/broker-host/tests/charlotte_conformance.rs`
build every request with the pinned `charlotte-kafka` client codec and parse
every response with the same codec, so the broker is checked against the exact
wire versions the CharlotteOS connector uses.

The workspace pins the same nightly toolchain as CharlotteOS
(`nightly-2026-07-27`) and pins the Sitas and CharlotteOS revisions in the root
`Cargo.toml`. `broker-host` is the only crate that knows about a host
operating system; the other crates are fit for the CharlotteOS EL0 target.

## Integration with CharlotteOS

The EL0 binary will follow the `catten-user` pattern: its own target JSON and
linker script, `-Z build-std=core,alloc`, a signed ELF note, and a staged entry
in `target/embedded-services/aarch64-unknown-none/`. The kernel installs and
loads services by logical name from the object store, so the first EL0
integration requires one gated entry in `BOOTSTRAP_ELFS` (or the signed
deployment path). Runtime authority is expected to be a `tcpip` socket
capability plus a storage capability, not ambient device access.

The Kafka connector's current profile requires verified TLS. Since
`embedded-tls` provides no server side, the first host milestones use a
plaintext listener and an external test client; the TLS listener (or a reviewed
test-only plaintext connector profile) is an explicit open question in
[docs/architecture.md](docs/architecture.md).

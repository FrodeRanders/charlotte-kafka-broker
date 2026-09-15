# Architecture

## 1. Context

CharlotteOS already speaks Kafka as a client: `charlotte-kafka` implements the
bounded request versions, `kafka.elf` owns broker connections, credentials, and
producer/consumer state behind capability endpoints, and `kafka_step.elf`
provides a transactional runner. What is missing is a broker that runs *inside*
the operating system, on its own block and network services, so the cluster can
be the log substrate rather than merely a client of one.

This repository is that broker. It is intentionally a subset: a durable,
partitioned, replicated append log with Kafka wire compatibility for the paths
the CharlotteOS connector already uses.

## 2. Layer boundaries

```text
        Kafka clients
             |
   +---------v----------+
   |     broker-wire    |  bounded decode/encode for the supported subset
   +---------+----------+
             |
   +---------v----------+
   |   broker-engine    |  request dispatch + Kafka error mapping
   +---------+----------+
             |
   +---------v----------+
   |   broker-runtime   |  Sitas shards, routing, replies, lifecycle
   +---------+----------+
             |
   +---------v----------+
   |    broker-core     |  partition log + topic catalog (pure state)
   +---------+----------+
             |
    storage and transport capabilities supplied by CharlotteOS
```

- `broker-core` never performs I/O and never observes time. It is deterministic
  and unit-testable without a runtime.
- `broker-runtime` owns threads and channels through the `sitas-core`
  `ShardRuntime` seam. It does not know how a request arrived or where records
  will be stored.
- `broker-engine` maps decoded requests onto broker operations and maps
  per-item failures to Kafka error codes. It owns no sockets and no threads,
  so the host TCP front end and the future EL0 service share it.
- `broker-wire` maps bytes to typed requests and responses. It does not own
  shards or state.

## 3. Execution model

One partition log is owned by exactly one shard. A shard is one OS thread with
one `ShardExecutor` and one reactor on CharlotteOS. A shard may own several
partitions; no partition is shared between shards.

```text
caller shard                     partition shard
     |                                  |
     |  PartitionCommand::Produce       |
     |  { ..., reply: ShardSender }     |
     +--------------------------------->|
     |                            validate against catalog
     |                            append to owned PartitionLog
     |  PartitionResult::Produced       |
     |<---------------------------------+
     |        + parker.unpark()         |
```

Rules:

- commands and replies are owned values over bounded, typed
  `sitas-core` channels;
- a caller whose mailbox is full parks with a short timeout and retries
  instead of spinning;
- a shard sends the reply and then unparks the waiting caller; the caller still
  re-checks on every wake because wakes may be coalesced or spurious;
- every wait has a bounded retry budget so a dead shard produces an error
  instead of a hang;
- shutdown is an explicit `Shutdown` command with an acknowledgement, not a
  dropped channel;
- the Sitas ring channels reserve one slot to distinguish full from empty, so
  a channel sized for `n` messages is created with `n + 1` slots.

Placement is deterministic: `shard = hash(topic) + partition (mod shard_count)`.
It is a pure function of the topic name and partition index, so every caller
routes to the owner without shared routing state.

### Session ownership

Connection protocol state is a separate ownership concern from partition data.
The EL0 listener assigns each accepted connection a `SessionId` and a stable
session shard using a bounded deterministic layout. The session shard is the
owner of that session's decoder buffer and, as those features arrive, its
producer epoch, consumer cursor, and transaction handle. A session shard may
send commands to any partition shard; it never mutates a partition log owned by
another shard. The current EL0 implementation uses one handler thread per
connection and pins it to the assigned logical session shard, while the host
front end uses one native thread per connection. Deployments must budget enough
threads for the partition shards and expected concurrent sessions; the soak
runner uses the platform maximum of 64. Socket admission is also bounded by
the shared tcpip service. A temporary socket or thread shortage is logged and
retried at the EL0 accept boundary rather than treated as a broker-fatal
protocol error.

Session affinity must not be confused with logical Kafka identity. A TCP
connection is disposable; a reconnecting producer or transaction may retain a
logical identity and must advance a fencing epoch. Consumer-group membership is
owned by a coordinator keyed by `group_id`, and transaction state by a
coordinator keyed by `transactional_id`; neither is inferred from the session
shard. The coordinator is single-writer state hosted by shard zero and reached
through typed mailbox commands. Producer epochs fence reconnecting instances,
transactions require explicit partition enlistment, and group generations
fence stale heartbeats and offset commits.

This split preserves partition parallelism while making per-session ordering
and backpressure explicit. A request involving several partitions crosses
several bounded typed mailboxes and is completed by a session continuation.
The current coordinator provides deterministic round-robin assignments and
in-memory offsets. Transactional batches are tagged in the owning partition
log; read-committed fetches hide pending and aborted batches, while committed
batches retain their original offsets. Resolution is sent back to every
enlisted partition through typed commands. The current resolution path is an
in-memory single-process commit protocol; durable recovery and quorum-backed
two-phase completion are still required before production failover.

## 4. Partition log semantics

A `PartitionLog` is an ordered sequence of batches. Each batch is appended
atomically and receives a contiguous range starting at the current high
watermark.

- append: assigns monotonically increasing offsets and returns the base offset;
- fetch: returns records from the requested offset, bounded by record count and
  approximate byte count, plus the current high watermark and last stable
  offset. Read-committed fetches omit pending and aborted transaction batches;
- list offsets: earliest is the retained start offset, latest is the high
  watermark;
- out-of-range reads are an error, not an empty result;
- a partition may be bounded by an approximate retained-byte budget. When the
  budget is exceeded, the oldest batches are dropped and the retained start
  advances; a reader below the start receives `OFFSET_OUT_OF_RANGE` and must
  resynchronize. The newest batch is always retained;
- readers of records always receive owned copies; no reference into shard state
  escapes.

Compaction and timestamp indexes are out of scope for the first milestones.
Transactional resolution is currently in-memory and coordinated through the
runtime; Kafka wire transaction APIs and durable recovery remain future work.
Retention is an in-memory byte
budget only; durable segment deletion arrives with the storage milestone.

## 5. Wire subset

The framing is the Kafka legacy request header version 1 (big-endian, length
prefixed) and the response header followed by the correlation id. Flexible
(tagged) versions are not used by this subset.

| API | Key | Version | Implemented |
|---|---|---|---|
| ApiVersions | 18 | 0 | advertises the table below |
| Metadata | 3 | 1 | `brokers`, `controller_id`, per-topic partitions |
| Produce | 0 | 3 | record batch v2, one topic/partition per entry |
| Fetch | 1 | 4 | high watermark, last stable offset, record batch v2 |
| ListOffsets | 2 | 1 | earliest (-2) and latest (-1) |
| FindCoordinator | 10 | 1 | returns this broker as the coordinator |
| InitProducerId | 22 | 0 | allocates/fences transactional producer epochs |
| AddPartitionsToTxn | 24 | 0 | enlists partitions for a producer transaction |
| EndTxn | 26 | 0 | commits or aborts enlisted partitions |
| JoinGroup | 11 | 1 | joins a deterministic consumer assignment |
| SyncGroup | 14 | 0 | returns the leader-provided assignment |
| Heartbeat | 12 | 0 | generation-fenced membership heartbeat |
| LeaveGroup | 13 | 0 | removes a group member |
| OffsetCommit | 8 | 2 | commits a generation-owned offset |
| OffsetFetch | 9 | 1 | retrieves a committed group offset |

The advertised ApiVersions response deliberately omits wire APIs that are not
implemented. Coordinator discovery, producer identity allocation, partition
enlistment, transactional produce, commit/abort, and the basic consumer-group
lifecycle are available on the wire. Group assignment is deterministic and
generation fenced; timeout-based eviction, cooperative rebalancing, and
durable coordinator recovery remain future work.

Record batches use magic 2 and CRC32C. Produce requests may carry several record
batches in one partition entry; each decoded batch is appended as one atomic
unit. Fetch responses re-encode stored records into a batch whose base offset is
the first returned record, preserving Kafka offset semantics.

### Dispatch and error mapping

`broker-engine` handles one decoded request at a time and answers with one
response frame. A failure that applies to one topic or partition becomes a
Kafka error code in that entry; the connection stays open. Decode and encode
failures fail the frame, and the front end closes the connection because a
malformed request has no trustworthy correlation id.

| Failure | Kafka error code |
|---|---|
| Unknown topic or partition | `UNKNOWN_TOPIC_OR_PARTITION` (3) |
| Fetch offset beyond the high watermark | `OFFSET_OUT_OF_RANGE` (1) |
| Transactional produce (`transactional_id` set) | `UNSUPPORTED_VERSION` (35) |
| ListOffsets timestamp other than -2 or -1 | `UNSUPPORTED_VERSION` (35) |
| Any other runtime or mailbox failure | `UNKNOWN_SERVER_ERROR` (-1) |

Fetch honors the smaller of the per-partition and request byte caps, bounded by
the engine's own maximum. `max_wait_ms` and `min_bytes` are accepted and
ignored: the broker answers with whatever is available immediately.

## 6. Storage direction

The first implementation keeps logs in memory so protocol and shard semantics
can be tested deterministically. The durable step keeps the same `PartitionLog`
interface and stores batches in segments on a storage capability:

- segment files are created in order and sealed at a bounded size;
- the high watermark is recoverable by scanning the last segment;
- flush and FUA behavior follow the object-store or block protocol contract;
- recovery validates CRCs before resuming appends;
- the broker receives only the storage capability named by its deployment
  descriptor, never a raw device.

`broker-core` is written so the storage backend stays on the runtime side of
the boundary; the log owns ordering and offsets, not file layout.

## 7. Distribution direction

The CharlotteOS cluster already provides discovery, a replicated name service,
Raft (`catten-graft`), and signed placement. The intended progression is:

1. a partition is hosted by one shard on one node;
2. a replica set is described by a signed deployment descriptor;
3. followers fetch from the leader and acknowledge the high watermark;
4. leader election uses the existing Raft service or a per-partition
   Leader/Follower state machine, and clients are redirected by `NOT_LEADER`
   metadata error codes;
5. producers observe `acks=-1` only after the replica quorum acknowledges.

Until replication exists, the broker reports itself as the leader of every
partition and stores `NO_ERROR` data locally.

## 8. CharlotteOS integration

This repository is an out-of-tree application. It produces a signed ELF and
the inputs for a signed deployment descriptor, then hands them to CharlotteOS
deployment tooling; it never edits or stages files into the OS tree. The full
boundary, ownership matrix, and stage commands are in
[development-model.md](development-model.md).

- The dispatch logic is `broker-engine`, so only the socket loop differs
  between the host front end and the EL0 service.
- A deployed instance receives only a bootstrap call to `grantctl` plus a
  read-only descriptor capability. It acquires its named grants and publishes
  its endpoint through `grant_client`; it receives no ambient name or
  transport authority.
- The expected grants for the EL0 broker are a `tcpip` socket capability and a
  storage capability. No MMIO, interrupt, or DMA authority belongs to the
  broker. Exact grant names and rights are fixed with the EL0 service.
- TLS is an open question: the CharlotteOS connector requires verified TLS and
  `embedded-tls` is client-only. The host milestones use plaintext with an
  external test client; the in-guest connector path needs either a server TLS
  implementation or a reviewed test-only plaintext profile.

## 9. Open questions

- Which record batch versions beyond v2 should the subset admit, if any?
- Is a dedicated per-partition Raft group acceptable, or should partitioning
  state live in one replicated controller?
- How should `acks=-1` be surfaced before a replica quorum exists: fail fast or
  append locally with a degraded acknowledgement?

The host conformance question is settled:
`crates/broker-host/tests/charlotte_conformance.rs` uses the pinned
`charlotte-kafka` request builders and response parsers as the oracle over the
TCP front end. The EL0 service should run the same dispatch (the engine) under
the same conformance expectations.

## 10. Milestones

- **M0 - foundation (done):** workspace, deterministic partition log,
  shard-per-partition runtime over typed mailboxes, bounded wire subset codec,
  host tests on the `sitas-unix` backend.
- **M1 - host broker (done):** transport-free dispatch engine, plain TCP front
  end, and conformance against `charlotte-kafka` request builders and response
  parsers.
- **M2 - EL0 and deployment (done):** `broker.elf` built through the out-of-tree
  packaging flow, deployed with a signed `CDEPLOY5` descriptor, and exercised
  from the host by the pinned client codec.
- **M3 - durable logs:** segmented storage with crash-recovery checks.
- **M4 - replicated partitions:** followers, high watermark replication, and
  leader failover against the existing distributed fixtures.

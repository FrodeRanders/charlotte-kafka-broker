# AGENTS.md

## Project purpose

This repository builds a research Kafka-subset broker for CharlotteOS on top of
the Sitas shard-per-core runtime. It is an application repository: it exists to
exercise CharlotteOS storage, networking, naming, deployment, and capability
machinery with a workload that has genuine concurrency and distribution
semantics. It is not a production broker and not a place to grow general
infrastructure frameworks.

Read [docs/architecture.md](docs/architecture.md) before changing protocol,
sharding, storage, or lifecycle behavior.

## Architectural invariants

Preserve these unless the change is explicitly about revising the architecture
and updates `docs/architecture.md` at the same time.

1. Only the owning shard may mutate a partition log or other service state.
2. Cross-shard interaction is explicit typed messages carrying owned values.
3. Do not hide service state behind `Arc<Mutex<..>>` as the normal model.
4. `broker-core` and `broker-runtime` are `no_std + alloc` and do not depend on
   a transport, host OS, or storage backend.
5. `broker-core` is pure state: no clocks, channels, threads, or I/O.
6. The wire subset is bounded and versioned. Adding an API or version requires
   updating the table in `docs/architecture.md` first.
7. Long waits park on the runtime parker; no busy-spin loops.
8. Every reply channel is bounded and every wait has a bounded retry budget.
9. Values that cross a shard boundary are owned; borrowed record data never
   outlives the command that carried it.
10. The dependencies are pinned by revision. Update the pins deliberately and
    in their own change.

## Coding rules

- Use edition 2024 and the pinned toolchain in `rust-toolchain.toml`.
- Keep the dependency footprint minimal; do not add a third-party async
  runtime, actor framework, or serialization framework.
- Prefer typed command enums and typed APIs over unstructured mailboxes.
- Follow the rustfmt configuration in the repository; do not hand-format.
- Keep public APIs documented; `cargo doc --no-deps` must succeed.
- Record batch and protocol code mirrors the bounded versions in
  `charlotte-kafka`; when that crate changes its version table, reconcile here.

## Testing and validation

Run the narrowest relevant tests first, then broaden:

```bash
cargo fmt --all -- --check
cargo test
cargo clippy --all-targets -- -D warnings
cargo doc --no-deps
```

When touching `broker-core`, cover append/fetch boundaries, offset exhaustion,
unknown topics/partitions, and empty or oversized appends. When touching
`broker-runtime`, cover success, full mailboxes, shard shutdown, and reply
timeouts where practical.

## Documentation responsibilities

Update `docs/architecture.md` when a change affects:

- the supported protocol subset or version table;
- shard ownership or placement rules;
- record and offset semantics;
- the storage layout or recovery behavior;
- the CharlotteOS integration boundary (bundle, signing, capabilities);
- non-goals or milestone status.

## Non-goals

Do not implement unless the current task explicitly asks for it:

- consumer groups or a group coordinator;
- transactions, idempotent producers, or producer fencing;
- compression or record headers;
- dynamic topic creation and admin APIs;
- TLS termination or SASL/SCRAM listeners;
- POSIX or SQL surfaces;
- a general-purpose storage engine or query layer.

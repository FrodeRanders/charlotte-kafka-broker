# AGENTS.md

## Project purpose

This repository builds a research Kafka-subset broker for CharlotteOS on top of
the Sitas shard-per-core runtime. It is an application repository: it exists to
exercise CharlotteOS storage, networking, naming, deployment, and capability
machinery with a workload that has genuine concurrency and distribution
semantics. It is not a production broker and not a place to grow general
infrastructure frameworks.

The layers are `broker-wire` (bounded codec), `broker-engine` (dispatch and
error mapping), `broker-runtime` (Sitas shard services), and `broker-core`
(partition log and catalog). `broker-host` is the only host-specific crate: it
provides the TCP front end and the conformance tests. `broker-el0` is the
`no_std` image built for the CharlotteOS EL0 target; it is excluded from
default workspace builds and is built with `tools/build-elf.sh`.

Read [docs/architecture.md](docs/architecture.md) before changing protocol,
sharding, storage, or lifecycle behavior, and
[docs/development-model.md](docs/development-model.md) before changing how the
application is built, signed, packaged, or deployed.

## Repository boundary

This repository consumes CharlotteOS; it does not develop it. The two
repositories meet at versioned crates, the launch/descriptor ABI, and the
deployment handoff. Preserve that separation:

1. Never edit a CharlotteOS checkout from this repository. Paths under
   `CHARLOTTE_OS_DIR` are read-only inputs. OS gaps are fixed in the OS
   repository and adopted here by bumping the pinned revision.
2. Never stage artifacts into a CharlotteOS service bundle or rely on
   `BOOTSTRAP_ELFS`. Bootstrap embedding is an OS bring-up mechanism; this
   application deploys through a signed `CDEPLOY5` descriptor.
3. The host development loop must work without a CharlotteOS checkout. Only
   cargo and the pinned crate revisions may be required for `cargo test`.
4. The product of this repository is a signed ELF plus descriptor inputs
   (placement, limits, grants). The operator or CI uploads the immutable
   bytes; CharlotteOS admits, places, and launches them.
5. Never commit private keys, deployment material, or built ELFs.
6. Update [docs/development-model.md](docs/development-model.md) in the same
   change as any new platform dependency, tooling invocation, or boundary
   rule.

## Architectural invariants

Preserve these unless the change is explicitly about revising the architecture
and updates `docs/architecture.md` at the same time.

1. Only the owning shard may mutate a partition log or other service state.
2. Cross-shard interaction is explicit typed messages carrying owned values.
3. Do not hide service state behind `Arc<Mutex<..>>` as the normal model.
4. `broker-core`, `broker-wire`, `broker-engine`, and `broker-runtime` are
   `no_std + alloc` and do not depend on a transport, host OS, or storage
   backend.
5. `broker-core` is pure state: no clocks, channels, threads, or I/O.
6. The wire subset is bounded and versioned. Adding an API or version requires
   updating the table in `docs/architecture.md` and keeping the
   `charlotte-kafka` conformance suite in `broker-host` green.
7. Long waits park on the runtime parker; no busy-spin loops.
8. Every reply channel is bounded and every wait has a bounded retry budget.
9. Values that cross a shard boundary are owned; borrowed record data never
   outlives the command that carried it.
10. The dependencies are pinned by revision. `charlotte.lock` pins the same
    CharlotteOS revision as the root `Cargo.toml`; update them deliberately and
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

The EL0 image is validated with the platform tools, not by host cargo:

```bash
tools/charlotte-sdk.sh use-os <charlotte-os-dir>   # or fetch / unpack
tools/build-elf.sh
tools/package.sh sign
```

With Docker and QEMU available, the end-to-end deployment check is:

```bash
CHARLOTTE_OS_DIR=<charlotte-os-dir> tools/qemu-smoke.sh
```

The independent Python load client lives under `tools/soak/`; it must stay
independent of the broker implementation (kafka-python as a black-box client)
and is not part of any crate. Its runner validates the deployed image under
sustained load:

```bash
CHARLOTTE_OS_DIR=<charlotte-os-dir> tools/soak/run_soak.sh --duration 60 --rate 5
```

When touching `broker-core`, cover append/fetch boundaries, offset exhaustion,
unknown topics/partitions, and empty or oversized appends. When touching
`broker-runtime`, cover success, full mailboxes, shard shutdown, and reply
timeouts where practical. When touching `broker-wire` or `broker-engine`,
extend `crates/broker-host/tests/charlotte_conformance.rs` so the change is
validated by the real client codec, not only by this repository's decoder.

## Documentation responsibilities

Update `docs/architecture.md` when a change affects:

- the supported protocol subset or version table;
- shard ownership or placement rules;
- record and offset semantics;
- the storage layout or recovery behavior;
- non-goals or milestone status.

Update `docs/development-model.md` when a change affects:

- the app/platform ownership boundary;
- pinned revisions, toolchain, or the platform contract;
- any develop, compile, package, sign, upload, or deploy step;
- an OS-side gap or the way the application works around it;
- the artifact identity, resource limits, or grant list.

## Non-goals

Do not implement unless the current task explicitly asks for it:

- consumer groups or a group coordinator;
- transactions, idempotent producers, or producer fencing;
- compression or record headers;
- dynamic topic creation and admin APIs;
- TLS termination or SASL/SCRAM listeners;
- POSIX or SQL surfaces;
- a general-purpose storage engine or query layer.

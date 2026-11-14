# Development model: consuming the CharlotteOS SDK

This repository is an application example. Broker core, wire handling, dispatch
and partition state belong here. Runtime owners, launch grants, protocol clients,
ELF targets, signing formats and deployment admission belong to CharlotteOS.

## Resolve the SDK once

`charlotte.lock` pins the OS revision, Sitas revision, toolchain and schema 2.
All workspace platform dependencies are relative paths into one generated source
SDK under `.charlotte/sdk/charlotte-app-sdk`. Cargo never mixes Git and SDK
instances of the same capability or runtime types. The SDK includes
`charlotte-app`, `catten-rt`, `charlotte-launch`, the Kafka client codec and both
Sitas host/native backends. `Cargo.lock` pins external registry dependencies.

```sh
tools/charlotte-sdk.sh unpack /path/to/charlotte-app-sdk.tar.gz
# Or export from a matching, read-only developer checkout:
tools/charlotte-sdk.sh use-os ../charlotte-os
# Or fetch the pinned OS/Sitas sources and export them into application state:
tools/charlotte-sdk.sh fetch
tools/charlotte-sdk.sh verify
```

A supplied `CHARLOTTE_SDK_SHA256` or `.sha256` sidecar is checked before unpack.
The resolver verifies schema, revisions, toolchain and every file hash before
using the SDK. These consistency checks are not signed release provenance.
A deliberate pin change requires resolving a matching SDK again. `worktree`
creates an independent local clone at the pin when the source checkout differs;
it never registers a worktree in the input repository. The SDK exporter accepts
`CHARLOTTE_SITAS_DIR` for its read-only Sitas input.

`.charlotte/` contains generated SDK, signer and runner state and is ignored.
Exporting and compiling do not modify the OS checkout. The signer is copied to
application-owned `.charlotte/signer` before Cargo can update a lockfile or build
output, preserving the SDK's verified inputs. The Rust toolchain is
`nightly-2026-07-27`; target builds also need `rust-src` and `llvm-tools`.

## Host development

After SDK resolution, the host loop needs no OS checkout or VM:

```sh
cargo test
cargo test -p broker-el0
python3 -m unittest discover -s tools/tests
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
cargo doc --no-deps
cargo run --example host_broker
```

The workspace explicitly excludes the SDK packages. `cargo fmt -- --check`
checks all six application crates; `--all` would also traverse local path
dependencies and apply application formatting to the verified SDK inputs.

The portable crates remain `no_std + alloc`. Conformance uses the SDK's actual
`charlotte-kafka` request builders and response decoders over a host TCP server.
The EL0 host tests cover framing and configuration parsing without invoking
native syscalls. A delayed-exit regression confirms that partition shutdown
acknowledgements are not mistaken for completed thread joins.

## Compile and sign

The demo keeps its existing Cargo workspace. Its small wrapper invokes the
SDK's public manifest/binary build interface, independently of the SDK starter
project layout:

```sh
tools/build-elf.sh --arch aarch64
# Or --arch x86_64
CHARLOTTE_SIGN_KEY_FILE=/path/to/restricted/key tools/package.sh sign
```

`tools/build-elf.sh` writes `target/elf/broker.elf`, uses the SDK's target/linker
inputs and verifies stripped ELF LOAD segments. The EL0 binary is `broker-el0`;
the artifact name is `broker`. `BROKER_ADVERTISE_HOST` and
`BROKER_ADVERTISE_PORT` set the endpoint returned in Kafka metadata at build
time. For a host forward, advertise its host-visible address and port.

Without an explicit key path, packaging stages a mode-0600 copy of the SDK's
publicly known development key. Version, rollback and provenance remain explicit
application policy; the current demo uses release/rollback 1 and no provenance.
No key bytes, signed artifacts or deployment material are committed.

The product is a signed CLS2 ELF, its digest and descriptor inputs. Upload and
admission use the existing operator/CI workflow. This repository never stages
its ELF into an OS bootstrap bundle. A signed deployment needs `tcpip=client`
and `broker=publish`, eight stack pages per thread, and at least three threads
(main network loop plus two partition workers). The fixture reserves 16 threads
and a 5000 ms shutdown grace. No raw MMIO, IRQ or DMA grant belongs to this app.

## Deployment and operation

Use the OS's `deployment-sign` and `deployment-notify` tools with the existing
management mTLS inputs; `CDEPLOY6` binds architecture/runtime ABI, exact signed
ELF digest, object key, placement, resource limits and grants. Signing an old ELF
with a new signer does not upgrade its runtime ABI.

The EL0 adapter uses `Application`, borrows the signed launch descriptor and
acquires only its declared `tcpip` connection. It binds its first listener before
publishing readiness under its signed artifact name. There is no public-name
lookup fallback. Its bounded CQ loop retains pending calls and socket borrows
inside the application scope. Shutdown releases network scopes, confirms
partition thread exits, and then returns `AppExit::Shutdown` to the SDK wrapper.
The Kafka protocol subset, partition placement and in-memory state are unchanged.

The existing `tools/package.sh sign` output lists the descriptor/sign/notify
handoff. For the complete disposable local workflow:

```sh
CHARLOTTE_OS_DIR=../charlotte-os tools/qemu-smoke.sh
```

The runner creates an application-owned OS/Sitas mirror and recreates its
disposable NVMe fixture for every run (`--fresh-storage`), then builds/signs the app,
uploads it to the test store, submits signed admission and observes exact-run
readiness. A host client checks produce/fetch/offsets. After the probe window,
a signed node shutdown must show the broker's worker-join marker and verified
poweroff. The operator's source checkout and persistent appliance data are not
used as writable test state. Reusing a previous smoke disk would also replay
its committed deployment and node-shutdown catalog state; it is a restart test,
not a fresh deployment fixture. This fixture requires Docker, QEMU and OS build
inputs, independently of the host test loop.

`tools/soak/` remains a separate kafka-python load client. Its current
`--host` and guest modes still use bounded in-memory retention; this ergonomics
change does not qualify long guest soaks, durable storage, replication, TLS or
additional Kafka wire APIs. Ordinary session limits and synchronous business
mailbox waits remain visible in the architecture guide.

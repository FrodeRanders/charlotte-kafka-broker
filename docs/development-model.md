# Development model: the CharlotteOS boundary

This repository is an out-of-tree CharlotteOS application and a worked example
of how to develop one. It consumes CharlotteOS as a platform. It does not
develop CharlotteOS itself, and the two repositories meet at a small, explicit
contract.

The development view from here is:

```text
develop  ->  compile  ->  package/sign  ->  upload  ->  deploy  ->  operate
 host lane    EL0 ELF      CLS2 note         S3 store     CDEPLOY5    status
 tests        + strip      + SHA-256         (CI/ops)     descriptor  + grants
```

This repository owns develop, compile, package/sign, and the descriptor inputs.
Upload is an operator or CI step. Deploy and operate belong to CharlotteOS
tooling; the only handoff is a signed ELF, its digest, and the descriptor
parameters.

## 1. Ownership

| Concern | Owner |
|---|---|
| Kernel, launch ABI, capability grants, `grantctl`, `deployd`, node `agent`, S3 connector | CharlotteOS |
| Target spec, link script, EL0 toolchain, `cluster-sign`, trust anchors, dev keys | CharlotteOS |
| Sitas runtime and its CharlotteOS backend | Sitas, integrated by CharlotteOS |
| Broker behavior, wire subset, shard layout, storage format | This repository |
| Artifact name, version, rollback policy, provenance, resource limits, grant list, object key | This repository |
| Upload of immutable bytes to the central store | Operator or CI |
| Descriptor signing key, admission, placement, launch, readiness fencing | CharlotteOS |

Hard rules:

1. This repository never edits a CharlotteOS checkout. OS changes are made in
   the OS repository and adopted here by bumping the pinned revision.
2. OS tooling is invoked read-only through `CHARLOTTE_OS_DIR`. Nothing under
   that directory is written.
3. `target/embedded-services` staging and `BOOTSTRAP_ELFS` entries are OS
   bring-up mechanisms. They are not this application's packaging path.
4. The host development loop works without a CharlotteOS checkout. Only cargo
   and the pinned crate revisions are required.
5. No private keys or deployment material are committed here. The development
   key is a publicly known CharlotteOS test key.
6. A change that adds a platform dependency updates the pinned revisions, the
   suite that proves it (conformance for wire/ABI), and this document.

## 2. The platform contract

| Contract element | Source | Pinned by |
|---|---|---|
| CharlotteOS crates (`catten-rt`, `charlotte-launch`, protocols, `charlotte-kafka`) | OS repository | `rev` in the root `Cargo.toml` |
| Sitas runtime | Sitas repository | `rev` in the root `Cargo.toml` |
| Platform tooling (builder, SDK export, signer) | OS repository | `charlotte.lock` |
| Toolchain | OS repository | `rust-toolchain.toml` and `charlotte.lock` |
| Machine image layout | `crates/catten-services/aarch64-unknown-none.json` and `link.x` | OS checkout revision |
| ELF signing note | CLS2, `charlotte-launch/src/signature_note.rs` | OS checkout revision |
| Deployment descriptor | `CDEPLOY5`/`CRELEASE`, `charlotte-launch/src/deployment.rs` | OS checkout revision |
| Runtime authority | Launch page v2, typed manifest, capability vector, `grantctl` | OS checkout revision |
| Wire versions | `charlotte-kafka` version table | OS checkout revision |

The pinned OS revision appears in the root `Cargo.toml` git dependencies and in
`charlotte.lock`. `tools/charlotte-sdk.sh` verifies a checkout or an exported
SDK against the lock before use, so the crate pin and the platform tooling
cannot silently diverge.

Authoritative OS references: `docs/guides/userspace-development.md`,
`docs/reference/deployment-ingress.md`,
`docs/reference/capability-grant-controller.md`, and
`docs/architecture/deployment-secrets-and-operations.md` in the CharlotteOS
repository.

## 3. Stages

### 3.0 Resolve the platform tooling

Two equivalent sources provide the platform build and signing tools:

- a pinned CharlotteOS checkout (development):
  `tools/charlotte-sdk.sh use-os ../charlotte-os` verifies the revision in
  `charlotte.lock` and uses its `scripts/build-external-elf.sh` and
  `tools/cluster-sign`;
- an exported SDK tarball (new projects, CI): CharlotteOS produces one with
  `scripts/export-app-sdk.sh`; `tools/charlotte-sdk.sh unpack <tarball>` checks
  its SHA-256, unpacks it under `.charlotte/sdk`, and uses its build wrapper,
  platform definitions, vendored `cluster-sign` workspace, and development
  keys. `tools/charlotte-sdk.sh fetch` sparse-clones the pinned revision when
  neither of the above exists.

`tools/charlotte-sdk.sh build-signer` builds `cluster-sign` with the pinned
toolchain and records its path in `.charlotte/platform.env`. Everything under
`.charlotte/` is generated and git-ignored, and a CharlotteOS checkout is never
written.

### 3.1 Develop (host lane, no OS checkout)

The primary loop. All portable crates are `no_std` and run under `sitas-unix`
on the host:

```sh
cargo test
cargo run --example host_broker
```

Wire and ABI behavior is validated against the pinned `charlotte-kafka` client
codec in `crates/broker-host/tests/charlotte_conformance.rs`. That suite is the
arbiter for protocol changes; this repository's own decoder is never the only
witness.

The host front end (`broker-host`) is development infrastructure. It is never
packaged, signed, or deployed to CharlotteOS; only the EL0 artifact is.

### 3.2 Compile (EL0 lane, resolved platform)

`tools/build-elf.sh` calls the platform builder
(`scripts/build-external-elf.sh` from a checkout, or the SDK copy) with the
application manifest and output path. The platform builder:

1. generates a build-local target specification whose pre-link argument points
   at the platform `link.x` (the shipped spec references it by relative path,
   so it cannot be used unmodified from another working directory);
2. runs the pinned toolchain with `-Z json-target-spec` and
   `-Z build-std=core,alloc,compiler_builtins`, and sets the platform build
   environment;
3. strips the result with `llvm-objcopy` and rejects writable executable or
   page-overlapping LOAD segments.

Output: `target/elf/broker.elf`. The EL0 crate is excluded from default
workspace builds because it is `no_std`; the host test loop never requires an
OS checkout.

### 3.3 Package and sign (OS tool, application parameters)

`tools/package.sh sign` signs the EL0 image with the resolved `cluster-sign`
and prints the artifact name, SHA-256, and the deployment handoff:

```sh
tools/package.sh sign
```

The artifact identity (`broker`, class `service`, version 1, rollback 1,
flags 0) is application policy. The development key is used unless
`CHARLOTTE_SIGN_KEY_HEX` names another key; production uses the offline cluster
key and is an operator action. The OS-side `artifact-policy.tsv` is an in-tree
build convenience and is not used by out-of-tree artifacts.

The equivalent manual invocation is:

```sh
CLUSTER_SIGN="$CHARLOTTE_CLUSTER_SIGN"   # from tools/charlotte-sdk.sh env
KEY_HEX="$(grep -v '^#' "$CHARLOTTE_KEYS_DIR/dev-key.hex" | tr -d '[:space:]')"
"$CLUSTER_SIGN" elf-sign target/elf/broker.elf broker "$KEY_HEX" service 1 1 0 -
DIGEST="$("$CLUSTER_SIGN" sha256 target/elf/broker.elf)"
```

`elf-sign` arguments are name, private key, class, version, rollback counter,
flags, and provenance digest.

### 3.4 Upload (operator or CI)

The signed bytes are immutable. Upload them to the central S3-compatible store
under the object key that the descriptor will name:

```sh
# Operator/CI upload; the development fixture uses a local RustFS:
rc cp target/elf/broker.elf local/<bucket>/deployments/broker-1.elf
```

CharlotteOS does not yet ship a host upload tool or a CI pipeline for external
artifacts. Until it does, this step belongs to the operator.

### 3.5 Deploy (CharlotteOS tools)

Create the signed `CDEPLOY5` descriptor. The application owns the content:
exact digest, object key, placement, per-thread stack pages, maximum active
threads, shutdown grace, and the named grants. CharlotteOS owns the signature
and admission:

```sh
"$CLUSTER_SIGN" deployment-sign deploy/broker.cdep broker \
  deployments/broker-1.elf "$DIGEST" 0 "$(date +%s)" 8 8 5000 "$KEY_HEX" \
  --replicas=3 --spread-replicas --anti-affinity-group=7 \
  tcpip=client storage=client

"$CLUSTER_SIGN" deployment-notify deploy/broker.cdep \
  127.0.0.1:${CATTEN_DEPLOY_HOST_PORT:-8081}
"$CLUSTER_SIGN" deployment-status broker \
  127.0.0.1:${CATTEN_DEPLOY_HOST_PORT:-8081} 120
```

Node key `0` addresses any eligible node. For an ordered multi-component
change, use `release-sign` and `release-apply` instead; admission is atomic
while rollout policy remains future work. `deployd` listens on guest TCP 7444
and the QEMU runners forward host port 8081 by default.

The EL0 broker needs at least three threads: the bootstrap thread plus one per
partition shard. The descriptor's `max_threads` must cover them and
`stack_pages_per_thread` applies to each. Its grant list is `tcpip=client` and
`broker=publish`; the publish grant name must equal the descriptor artifact
name for the node agent to observe readiness.

A deployed application receives only a bootstrap call to `grantctl` plus a
read-only descriptor capability; the launch manifest is empty. It acquires its
named grants and publishes its endpoint through `grant_client` under the
artifact name in the descriptor.

### 3.6 Operate

`deployment-status` reports the exact committed generation. Broker readiness
and observability surface through the descriptor's published endpoint; live
replacement is generation-fenced by the OS. The application must treat every
generation as disposable and never keep state that the descriptor does not
place.

## 4. What must change in CharlotteOS

These are OS-side or operations work items, not patches from this repository:

| Gap | Handling |
|---|---|
| No host upload tool or external-artifact CI job | Operator uploads for now; a future OS/ops pipeline can absorb it |
| Bootstrap S3 connector profile must exist before the first notify | Cluster provisioning step, recorded in the OS deployment guide |
| Application must acquire and publish through `grantctl` | Implemented here in M2; the OS provides the contract |
| No coordinated rollback or rolling replacement | Use `CRELEASE` atomic admission; do not assume rollback exists |
| Any new grant kind, connector binding, or protocol version | Changed in the OS repository first; then bump the pin here |

The node `agent` already handles full artifact names, S3 fetch, digest and CLS2
verification, placement, and scoped launch; the port to a generic broker is an
application-side exercise, not an OS change.

## 5. Working example

Implemented:

- `crates/broker-el0`: the `#![no_std]` image. It starts one broker over a
  `CharlotteReactor`, acquires only its granted `tcpip` connection, publishes
  readiness under its artifact name, listens on port 9092, and serves Kafka
  frames from accepted connections through `broker-engine`;
- `tools/charlotte-sdk.sh`: platform resolution by checkout, sparse fetch, or
  SDK tarball, plus `cluster-sign` build;
- `tools/build-elf.sh` and `tools/package.sh`: the compile and sign stages;
- CharlotteOS `scripts/build-external-elf.sh` and `scripts/export-app-sdk.sh`:
  the platform builder and the SDK packaging.

Still open:

- EL0 execution under QEMU with a `CDEPLOY5` descriptor;
- connector interop through the deployment ingress and readiness observation;
- segmented durable logs and replicated partitions.

The host development loop requires no OS checkout, which is the point of the
boundary.

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
| Toolchain | OS repository | `rust-toolchain.toml` |
| Machine image layout | `crates/catten-services/aarch64-unknown-none.json` and `link.x` | OS checkout revision |
| ELF signing note | CLS2, `charlotte-launch/src/signature_note.rs` | OS checkout revision |
| Deployment descriptor | `CDEPLOY5`/`CRELEASE`, `charlotte-launch/src/deployment.rs` | OS checkout revision |
| Runtime authority | Launch page v2, typed manifest, capability vector, `grantctl` | OS checkout revision |
| Wire versions | `charlotte-kafka` version table | OS checkout revision |

The pinned OS revision is the one referenced by the root `Cargo.toml` git
dependency. The EL0 build (M2) verifies that the `CHARLOTTE_OS_DIR` checkout is
at that revision before using its target spec and signing tool.

Authoritative OS references: `docs/guides/userspace-development.md`,
`docs/reference/deployment-ingress.md`,
`docs/reference/capability-grant-controller.md`, and
`docs/architecture/deployment-secrets-and-operations.md` in the CharlotteOS
repository.

## 3. Stages

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

### 3.2 Compile (EL0 lane, read-only OS checkout)

The application supplies `crates/broker-el0` (M2). CharlotteOS supplies the
platform definition: target specification, linker script, toolchain, and the
`-Z build-std=core,alloc` invocation pattern. `tools/build-elf.sh` (M2) will:

1. require `CHARLOTTE_OS_DIR` and verify its revision against the pin;
2. generate a build-local target spec whose pre-link argument points at the OS
   `link.x` (the shipped spec references it by relative path, so it cannot be
   used unmodified from another working directory);
3. build the EL0 binary, strip it with `llvm-objcopy`, and reject writable
   executable or page-overlapping LOAD segments;

Output: `target/elf/broker.elf`, an unsigned ELF with a verified machine
layout.

### 3.3 Package and sign (OS tool, application parameters)

This repository owns the artifact identity and policy; CharlotteOS owns the
signing format and key map. Sign with the pinned `cluster-sign`:

```sh
CHARLOTTE_OS_DIR=/path/to/charlotte-os
CLUSTER_SIGN="$CHARLOTTE_OS_DIR/target/debug/cluster-sign"

# Build the OS tool without inheriting this repository's working directory;
# the OS root build configuration pins its own platform build-std setup.
(cd /tmp && cargo build --quiet \
  --manifest-path "$CHARLOTTE_OS_DIR/tools/cluster-sign/Cargo.toml")

KEY_HEX="$(grep -v '^#' "$CHARLOTTE_OS_DIR/tools/cluster-sign/dev-key.hex" | tr -d '[:space:]')"

"$CLUSTER_SIGN" elf-sign target/elf/broker.elf broker "$KEY_HEX" service 1 1 0 -
DIGEST="$("$CLUSTER_SIGN" sha256 target/elf/broker.elf)"
```

`elf-sign` arguments are name, private key, class, version, rollback counter,
flags, and provenance digest. The development key is the publicly known test
key; production uses the offline cluster key and is an operator action. The
OS-side `artifact-policy.tsv` is an in-tree build convenience and is not used
by out-of-tree artifacts.

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

## 5. Working example at M2

The first EL0 milestone adds:

- `crates/broker-el0`: the `#![no_std]` binary using `catten-rt` and
  `sitas-charlotte`, holding no authority beyond its bootstrap grant;
- `tools/build-elf.sh`: the compile stage above;
- `deploy/broker.cdep` inputs and a packaging script that runs stages 3.3 and
  3.5 through `CHARLOTTE_OS_DIR`;
- conformance and readiness checks driven from the host against a QEMU cluster.

Everything before that is host-testable and requires no OS checkout, which is
the point of the boundary.

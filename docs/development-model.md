# Development model: the CharlotteOS boundary

This repository is an out-of-tree CharlotteOS application and a worked example
of how to develop one. It consumes CharlotteOS as a platform.

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
| CharlotteOS crates (`catten-rt`, `charlotte-launch`, protocols, `charlotte-kafka`) | OS repository | immutable platform tag in the root `Cargo.toml` |
| Sitas runtime | Sitas repository | repository tag in the root `Cargo.toml` |
| Platform tooling (builder, SDK export, signer) | OS repository | `charlotte.lock` |
| Toolchain | OS repository | `rust-toolchain.toml` and `charlotte.lock` |
| Machine image layout | `crates/catten-services/aarch64-unknown-none.json` and `link.x` | OS checkout revision |
| ELF signing note | CLS2, `charlotte-launch/src/signature_note.rs` | OS checkout revision |
| Deployment descriptor | `CDEPLOY5`/`CRELEASE`, `charlotte-launch/src/deployment.rs` | OS checkout revision |
| Runtime authority | Launch page v2, typed manifest, capability vector, `grantctl` | OS checkout revision |
| Wire versions | `charlotte-kafka` version table | OS checkout revision |

The platform is consumed through an immutable git tag
(`app-platform-81d5064a`), with the exact revision also recorded in
`charlotte.lock`; the committed `Cargo.lock` resolves the tag to its commit.
`tools/charlotte-sdk.sh` verifies a checkout or an exported SDK against the
lock before use, so the crate pin and the platform tooling cannot silently
diverge. Tags are never moved; a platform change creates a new tag.

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
  `tools/cluster-sign`. When the local checkout has moved past the pinned
  revision, `tools/charlotte-sdk.sh worktree ../charlotte-os` creates a
  detached worktree at the immutable platform tag from that checkout, leaving
  `main` untouched, and resolves the worktree instead;
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

### 3.5.1 Manual RustFS and VMware deployment

The following is the long-form version of the automated deployment fixture. It
keeps RustFS, signing, upload, admission, and load generation visible as
separate operator actions. It assumes a single x86-64 VMware guest on a
bridged LAN (a host-only network also works when it supplies DHCP and routes
the host to the guest). Replace the addresses with values from the local LAN:

```sh
export CHARLOTTE_OS_DIR="$PWD/../charlotte-os"
export VM_IP=192.168.1.42             # DHCP lease of the CharlotteOS guest
export RUSTFS_HOST_IP=192.168.1.20    # address of the Docker host
export RUSTFS_HOSTNAME=rustfs.test    # DNS name in the certificate/profile
```

The hostname must resolve from the guest to `RUSTFS_HOST_IP`; create a LAN DNS
record (or use the organisation's existing RustFS name). TLS verification is
name-based, so do not put an arbitrary IP in the profile while retaining a
certificate for a different name.

1. Build and boot a fresh VMware appliance. In Fusion/Workstation select a
   bridged adapter, or a host-only adapter with DHCP, before powering it on:

   ```sh
   cd "$CHARLOTTE_OS_DIR"
   scripts/build-vmware-x86_64.sh release --replace
   # Open os-images/vmware/CharlotteOS.vmwarevm/CharlotteOS.vmx and boot it.
   # Obtain VM_IP from the LAN DHCP lease table or the operator's inventory.
   ```

   CharlotteOS then starts its ordinary DHCP, discovery, cluster, storage,
   deployment, TCP/IP, and time services. VMware NAT is suitable for local
   qualification, but bridged/host-only networking is the direct-address path
   used here; no host-port forwarding is required.

2. Create a short-lived TLS RustFS fixture on the Docker host. The compose file
   binds localhost by default; `CATTEN_RUSTFS_BIND_ADDRESS` publishes it on the
   LAN so the guest can reach it. Keep the private CA and server key outside
   version control:

   ```sh
   export RUSTFS_DIR="$CHARLOTTE_OS_DIR/target/manual-rustfs"
   export CATTEN_RUSTFS_CERT_DIR="$RUSTFS_DIR/certs"
   export CATTEN_RUSTFS_BIND_ADDRESS="$RUSTFS_HOST_IP"
   export CATTEN_RUSTFS_PORT=19000
   mkdir -p "$CATTEN_RUSTFS_CERT_DIR"

   openssl ecparam -name prime256v1 -genkey -noout \
     -out "$CATTEN_RUSTFS_CERT_DIR/ca.key"
   openssl req -x509 -new -sha256 -days 2 \
     -key "$CATTEN_RUSTFS_CERT_DIR/ca.key" \
     -subj "/CN=CharlotteOS manual RustFS CA" \
     -addext "basicConstraints=critical,CA:TRUE" \
     -addext "keyUsage=critical,keyCertSign,cRLSign" \
     -out "$CATTEN_RUSTFS_CERT_DIR/ca.crt"
   openssl ecparam -name prime256v1 -genkey -noout \
     -out "$CATTEN_RUSTFS_CERT_DIR/rustfs_key.pem"
   openssl req -new -sha256 \
     -key "$CATTEN_RUSTFS_CERT_DIR/rustfs_key.pem" \
     -subj "/CN=$RUSTFS_HOSTNAME" \
     -out "$CATTEN_RUSTFS_CERT_DIR/rustfs.csr"
   printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyAgreement\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:%s\n' \
     "$RUSTFS_HOSTNAME" > "$RUSTFS_DIR/server-ext.cnf"
   openssl x509 -req -sha256 -days 2 \
     -in "$CATTEN_RUSTFS_CERT_DIR/rustfs.csr" \
     -CA "$CATTEN_RUSTFS_CERT_DIR/ca.crt" \
     -CAkey "$CATTEN_RUSTFS_CERT_DIR/ca.key" -CAcreateserial \
     -extfile "$RUSTFS_DIR/server-ext.cnf" \
     -out "$CATTEN_RUSTFS_CERT_DIR/rustfs_cert.pem"
   openssl x509 -in "$CATTEN_RUSTFS_CERT_DIR/ca.crt" -outform DER \
     -out "$CATTEN_RUSTFS_CERT_DIR/ca.der"
   chmod 0644 "$CATTEN_RUSTFS_CERT_DIR/ca.crt" \
     "$CATTEN_RUSTFS_CERT_DIR/ca.der" \
     "$CATTEN_RUSTFS_CERT_DIR/rustfs_cert.pem" \
     "$CATTEN_RUSTFS_CERT_DIR/rustfs_key.pem"

   export RUSTFS_COMPOSE="$CHARLOTTE_OS_DIR/docker/rustfs-s3-test/compose.yaml"
   docker compose -f "$RUSTFS_COMPOSE" down --volumes --remove-orphans
   docker compose -f "$RUSTFS_COMPOSE" up -d --wait rustfs
   docker compose -f "$RUSTFS_COMPOSE" run --rm init
   ```

   The fixture creates bucket `charlotte-test` and uses access key
   `charlotte-test-access` with secret
   `charlotte-test-secret-2026`. These are development credentials only.

3. Build the broker with the guest's directly reachable address, add the CLS2
   signature, and upload the exact signed bytes to RustFS. The upload runs in
   the RustFS CLI container, so it does not require `rc` on the host:

   ```sh
   cd ../charlotte-kafka-broker
   tools/charlotte-sdk.sh use-os "$CHARLOTTE_OS_DIR"
   tools/charlotte-sdk.sh build-signer
   BROKER_ADVERTISE_HOST="$VM_IP" BROKER_ADVERTISE_PORT=9092 \
     tools/build-elf.sh --arch x86_64
   tools/package.sh sign

   docker compose -f "$RUSTFS_COMPOSE" run --rm --no-deps \
     -v "$PWD/target/elf/broker.elf:/tmp/broker.elf:ro" \
     --entrypoint /bin/sh init -ec \
     "rc alias set local https://$RUSTFS_HOSTNAME:9000 \\
        charlotte-test-access charlotte-test-secret-2026 && \\
        rc cp /tmp/broker.elf local/charlotte-test/deployments/broker.elf"
   ```

   `tools/package.sh sign` signs the final ELF before the upload. Never upload
   an ELF, then sign or strip it: the descriptor digest must match the bytes
   that the agent retrieves.

4. Sign a descriptor and tell the cluster to admit it. The node key `0` lets a
   singleton placement choose the eligible node (the only node in this setup):

   ```sh
   . .charlotte/platform.env
   KEY_HEX="$(grep -v '^#' "$CHARLOTTE_KEYS_DIR/dev-key.hex" | tr -d '[:space:]')"
   DIGEST="$($CHARLOTTE_CLUSTER_SIGN sha256 target/elf/broker.elf)"
   SEQUENCE="$(date +%s)"
   mkdir -p target/manual-deployment

   "$CHARLOTTE_CLUSTER_SIGN" deployment-sign \
     target/manual-deployment/broker.cdep broker \
     deployments/broker.elf "$DIGEST" 0 "$SEQUENCE" 8 64 5000 \
     "$KEY_HEX" tcpip=client broker=publish
   "$CHARLOTTE_CLUSTER_SIGN" deployment-notify \
     target/manual-deployment/broker.cdep "$VM_IP:7444"
   "$CHARLOTTE_CLUSTER_SIGN" deployment-status broker "$VM_IP:7444" 180
   ```

   `deployment-status` must report the exact generation as ready before the
   broker is used. The signed descriptor contains no S3 credentials; it names
   only the opaque object key. The node's separately provisioned `s3` connector
   fetches and verifies the object.

5. Drive the broker directly over the VMware LAN address. The normal soak
   wrapper is QEMU-specific, so invoke its independent client explicitly:

   ```sh
   python3 -m venv tools/soak/.venv
   tools/soak/.venv/bin/python -m pip install -r tools/soak/requirements.txt
   tools/soak/.venv/bin/python tools/soak/soak_client.py \
     --bootstrap "$VM_IP:9092" --duration 600 --rate 20 --producers 4
   ```

There is one current platform prerequisite for this otherwise complete
sequence: a stock VMware appliance does not yet expose an operator-facing
command for provisioning the bootstrap `s3` connector profile. The QEMU
`--deployment-ingress-test` fixture injects that profile through test-only
launch code; a normal VMware boot therefore logs `S3 GET unavailable:
connector is not registered` until the profile is provisioned. The profile must
contain the RustFS LAN endpoint, matching `RUSTFS_HOSTNAME`, port 19000, the CA
DER from `ca.der`, bucket `charlotte-test`, and the fixture credentials, with
GET rights. Adding a sealed/bootstrap profile provisioning command is the
remaining step needed to make the VMware procedure fully turnkey; the build,
upload, signing, and cluster-admission steps above are already manual and
supported.

### 3.6 Operate

`deployment-status` reports the exact committed generation. Broker readiness
and observability surface through the descriptor's published endpoint; live
replacement is generation-fenced by the OS. The application must treat every
generation as disposable and never keep state that the descriptor does not
place.

For sustained load, `tools/soak/run_soak.sh` drives the independent Python
client against either the host front end or a deployed QEMU image:

```sh
tools/soak/run_soak.sh --host --duration 60 --rate 20
CHARLOTTE_OS_DIR=../charlotte-os tools/soak/run_soak.sh --duration 43200 --rate 20
# Leaner QEMU measurement: optimized kernel and no packet capture.
CHARLOTTE_OS_DIR=../charlotte-os tools/soak/run_soak.sh \
  --kernel-profile release --no-net-dump --duration 600 --rate 100 --producers 8
```

The QEMU path builds the image with the client-reachable advertised address,
deploys it, keeps the guest alive for the requested duration, and verifies log
continuity, value integrity, and per-producer sequence monotonicity. `--rate` is
the aggregate offered load and `--producers` (default 4) sizes the connection
pool: a synchronous producer offers at most one record per broker round trip,
so a rate is reachable only when the pool covers the guest's request latency.
The guest's `tcpip` service owns a heap-sized smoltcp socket set shared by local
services. The default policy requests 64 slots and charges each authenticated
principal at most 64 sockets and 2 MiB of buffers. A TCP socket in FIN-WAIT
or TIME-WAIT remains charged until the stack reaches a final state, so a burst
of reconnects can temporarily exhaust the per-principal budget even when the
number of active sessions is lower. The soak runner deploys the broker with a
64-thread budget and preserves the guest on failure; the broker logs socket
admission failures and retries instead of aborting the address space.
`--arch aarch64|x86_64|auto`
selects the guest and EL0 image architecture and defaults to the host, so an
Intel machine exercises the native x86_64 path and Apple Silicon the aarch64
one. Each partition is bounded to a small retained-byte budget so the 4 MiB EL0
heap survives an overnight run; the client resynchronizes and reports a gap if
it ever falls behind retention. The runner creates and repairs its own Python
virtualenv on first use (`CHARLOTTE_SOAK_PYTHON` selects the interpreter), so a
host only needs `python3`, Docker, and QEMU.

If the load client fails, the runner deliberately leaves the guest alive and
prints the wrapper PID, QEMU PID file, serial log, packet capture, QEMU monitor
socket, and GDB port. This makes the failure state inspectable instead of
destroying it in the shell exit trap. The runner's calculated safety timeout
still provides an eventual upper bound. Unattended automation that prefers the
old teardown behavior passes `--cleanup-on-failure`; `--keep` continues to mean
that a successful run waits for the guest's hold period instead of cleaning it
up immediately. The QEMU runner uses a debug kernel and packet capture by
default. `--kernel-profile release` (or
`CATTEN_SOAK_KERNEL_PROFILE=release`) selects the optimized kernel, while
`--no-net-dump` (or `CATTEN_SOAK_NET_DUMP=0`) disables packet capture;
`--net-dump` explicitly enables it.

The CharlotteOS repository also builds a VMware Fusion/Workstation appliance
(`scripts/build-vmware-x86_64.sh`). It boots the ordinary single-node service
configuration, so it is suitable for exercising the same deployment and broker
workload. With the adapter attached to a bridged (or suitable host-only)
network, CharlotteOS obtains its address from the LAN's DHCP service and the
host reaches deployment port 7444 and the broker port directly; no QEMU-style
host forwarding is involved. The supplied VMX defaults to VMware NAT for
portable first-boot qualification, so change that attachment when direct LAN
reachability is wanted. The remaining soak-runner work is lifecycle and
address handoff: starting the VM, learning the DHCP lease (or accepting an
operator-supplied guest address), and passing `<guest-ip>:7444` to the signed
deployment client and `<guest-ip>:9092` to the load client. RustFS must likewise
be reachable from the guest. Until that adapter exists, use QEMU for the
repeatable automated deployment flow and the VMware appliance for manual LAN
qualification.

The EL0 broker emits a low-rate progress heartbeat rather than logging every
Kafka request. Its cumulative `frames`, `handled`, and `sent` counters localize
a stall to receive, shard dispatch/encoding, or socket send. The `last` field
is `<connection>:<stage>` and uses stages 1 accepted, 2 bytes received,
3 dispatching, 4 handled, 5 sending, 6 sent, 7 receive error, 8 engine error,
9 send error, and 10 closed.
The accompanying API key/version and correlation ID identify the last complete
request frame. CharlotteOS's `tcpip` heartbeat reports the counts of listening,
connecting, established, closing, and closed TCP sockets together with pending
and ready receives, so the broker and transport views can be correlated.

The EL0 listener also records a stable `SessionId` for each accepted
connection, assigns it to one of the logical session shards, and logs that
assignment. The current handler remains one thread per connection, but its
placement is no longer hard-coded to shard 0. Session placement owns protocol
state only; partition logs continue to be routed by `(topic, partition)`.

Transaction and consumer-group coordination follows the same ownership rule.
The runtime coordinator is a single writer: `transactional_id` owns producer
epochs and enlisted partitions, while `group_id` owns members, generations,
assignments, and committed offsets. Applications use typed coordinator calls;
they do not share locks or mutate partition state directly. A stale producer
epoch or group generation is rejected, making reconnect and rebalance
behaviour explicit. Transactional records are tagged in their owning log and
read-committed consumers hide pending or aborted batches. The state is
currently in memory and a transaction may enlist consumer offsets; committed
transactions advance those offsets while aborted transactions leave them
unchanged. The basic Kafka transaction and group lifecycle is available on the
wire; the state will be promoted to the cluster's durable control plane before
group failover and production recovery are enabled.

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
  frames from accepted connections through `broker-engine`. It builds for both
  AArch64 and x86_64;
- `crates/broker-host/examples/remote_smoke.rs`: the host client that drives
  the deployed image with the pinned `charlotte-kafka` codec;
- `tools/charlotte-sdk.sh`: platform resolution by checkout, sparse fetch, or
  SDK tarball, plus `cluster-sign` build;
- `tools/build-elf.sh` and `tools/package.sh`: the compile and sign stages;
- `tools/qemu-smoke.sh`: deploy the signed image into a single-guest cluster
  through the signed `CDEPLOY5` path and run the host smoke client;
- `tools/soak/`: the independent kafka-python load client and its runner,
  including the build-time advertised endpoint that lets a host client follow
  metadata through the QEMU forward;
- bounded in-memory retention per partition so long runs fit the EL0 heap;
- CharlotteOS `scripts/build-external-elf.sh`, `scripts/export-app-sdk.sh`,
  and the parameterized `--deployment-ingress-test` fixture.

The validated run builds the image, signs it, uploads it to the central store,
notifies `deployd`, lets the node agent fetch, verify, and scoped-launch it,
observes readiness, and then exercises produce, fetch, and list-offsets from
the host through a SLIRP forward.

Still open:

- in-guest connector interop, blocked on the TLS-listener decision;
- segmented durable logs and replicated partitions.

The host development loop requires no OS checkout, which is the point of the
boundary.

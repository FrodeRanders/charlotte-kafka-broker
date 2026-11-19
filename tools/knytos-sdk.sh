#!/usr/bin/env bash
# Resolve one immutable Knytos source SDK. All generated state is local.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE="$ROOT/.knytos"
ENV_FILE="$STATE/platform.env"
. "$ROOT/knytos.lock"

check_revision() {
    [ "$(git -C "$1" rev-parse HEAD)" = "$OS_REVISION" ] || {
        echo "error: platform differs from knytos.lock; adopt a reviewed pin first" >&2; exit 1;
    }
}
write_env() {
    mkdir -p "$STATE"
    local sdk="$STATE/sdk/knytos-app-sdk"
    {
        printf 'KNYTOS_PLATFORM_KIND=%q\n' "$1"
        printf 'KNYTOS_PLATFORM_ROOT=%q\n' "$2"
        printf 'KNYTOS_SDK_ROOT=%q\n' "$sdk"
        printf 'KNYTOS_BUILD_ELF=%q\n' "$sdk/build-external-elf.sh"
        printf 'KNYTOS_SIGNER_MANIFEST=%q\n' "$STATE/signer/Cargo.toml"
        printf 'KNYTOS_SIGNER_TARGET=%q\n' "$STATE/signer-target"
        printf 'KNYTOS_KEYS_DIR=%q\n' "$sdk/keys"
        printf 'KNYTOS_REVISION=%q\n' "$OS_REVISION"
        printf 'KNYTOS_TOOLCHAIN=%q\n' "$TOOLCHAIN"
    } > "$ENV_FILE"
}
verify_sdk() {
    python3 - "$1" "$OS_REVISION" "$SITAS_REVISION" "$TOOLCHAIN" "$SDK_SCHEMA" <<'PY'
from pathlib import Path
import hashlib,json,sys
sdk=Path(sys.argv[1]); lock=json.loads((sdk/'sdk.json').read_text())
if (lock['os_revision'],lock['sitas_revision'],lock['toolchain'],str(lock['schema']))!=tuple(sys.argv[2:]): raise SystemExit('SDK differs from knytos.lock')
for name,digest in lock['files'].items():
    if hashlib.sha256((sdk/name).read_bytes()).hexdigest()!=digest: raise SystemExit(f'SDK input differs: {name}')
for name in ['knytos-app','knytos-rt','knytos-launch','knytos-kafka','sitas-core','sitas-knytos','sitas-unix']:
    if not (sdk/'crates'/name/'Cargo.toml').is_file(): raise SystemExit(f'missing SDK crate: {name}')
PY
}
unpack() {
    python3 - "$1" <<'PYHASH'
import hashlib,os,sys
from pathlib import Path
archive=Path(sys.argv[1]); sidecar=Path(str(archive)+'.sha256')
expected=os.environ.get('KNYTOS_SDK_SHA256')
if not expected and sidecar.exists(): expected=sidecar.read_text().split()[0]
if expected and hashlib.sha256(archive.read_bytes()).hexdigest()!=expected: raise SystemExit('SDK tarball sha256 mismatch')
PYHASH
    mkdir -p "$STATE"
    local stage
    stage="$(mktemp -d "$STATE/sdk-next.XXXXXX")"
    tar -xzf "$1" -C "$stage"
    verify_sdk "$stage/knytos-app-sdk"
    # Only generated SDK input is replaced; signer output lives elsewhere.
    rm -rf "$STATE/sdk"
    mv "$stage" "$STATE/sdk"
    write_env sdk "$STATE/sdk/knytos-app-sdk"
}
use_os() {
    local source
    source="$(cd "$1" && pwd)"
    check_revision "$source"
    mkdir -p "$STATE"
    local tarball="$STATE/sdk-$OS_REVISION.tar.gz"
    GIT_OPTIONAL_LOCKS=0 "$source/scripts/export-app-sdk.sh" --output "$tarball"
    unpack "$tarball"
    write_env os "$source"
}
fetch() {
    local root="$STATE/platform-source"
    local os="$root/private/knytos"
    local sitas="$root/gautelis/sitas"
    mkdir -p "$root/private" "$root/gautelis"
    if [ ! -d "$os/.git" ]; then git clone --filter=blob:none --no-checkout "$OS_REPOSITORY" "$os"; fi
    git -C "$os" fetch origin "$OS_REVISION"
    git -C "$os" sparse-checkout init --cone
    git -C "$os" sparse-checkout set scripts tools/cluster-sign .github/workflows \
        crates/knytos-services crates/knytos-rt crates/knytos-syscall crates/knytos-launch \
        crates/knytos-lifecycle crates/knytos-app crates/knytos-app-sitas \
        crates/knytos-kafka crates/knytos-protocol-grant crates/knytos-protocol-kafka crates/knytos-protocol-s3
    git -C "$os" checkout --detach "$OS_REVISION"
    if [ ! -d "$sitas/.git" ]; then git clone --filter=blob:none --no-checkout "$SITAS_REPOSITORY" "$sitas"; fi
    git -C "$sitas" fetch origin "$SITAS_REVISION"
    git -C "$sitas" checkout --detach "$SITAS_REVISION"
    use_os "$os"
}
worktree() {
    local source="$1" destination="${2:-$STATE/worktree-$OS_REVISION}"
    [ ! -e "$destination" ] || { echo "error: destination exists" >&2; exit 1; }
    # Independent clone: no registration or other writes in the input checkout.
    git clone --no-checkout --no-hardlinks "$source" "$destination"
    git -C "$destination" checkout --detach "$OS_REVISION"
    # Export uses the caller's read-only Sitas source at the pinned revision.
    local sitas_source="$source/../../gautelis/sitas"
    local tarball="$STATE/sdk-$OS_REVISION.tar.gz"
    GIT_OPTIONAL_LOCKS=0 KNYTOS_SITAS_DIR="$sitas_source" "$destination/scripts/export-app-sdk.sh" --output "$tarball"
    unpack "$tarball"
}
runner_os() {
    local source="$1" root="$STATE/runner-$OS_REVISION"
    source="$(cd "$source" && pwd)"
    local os="$root/private/knytos" sitas="$root/gautelis/sitas"
    mkdir -p "$root/private" "$root/gautelis"
    if [ ! -d "$os/.git" ]; then
        git clone --no-checkout --no-hardlinks "$source" "$os" >&2
        git -C "$os" checkout --detach "$OS_REVISION" >&2
    fi
    check_revision "$os"
    if [ ! -d "$sitas/.git" ]; then
        local input="${KNYTOS_SITAS_DIR:-$source/../../gautelis/sitas}"
        [ -d "$input/.git" ] || input="$SITAS_REPOSITORY"
        git clone --no-checkout --no-hardlinks "$input" "$sitas" >&2
        git -C "$sitas" checkout --detach "$SITAS_REVISION" >&2
    fi
    [ "$(git -C "$sitas" rev-parse HEAD)" = "$SITAS_REVISION" ] || exit 1
    printf '%s\n' "$os"
}
load() {
    [ -f "$ENV_FILE" ] || { echo "error: resolve an SDK with use-os, fetch or unpack" >&2; exit 1; }
    . "$ENV_FILE"
    [ "$KNYTOS_REVISION" = "$OS_REVISION" ] && [ "$KNYTOS_TOOLCHAIN" = "$TOOLCHAIN" ] || {
        echo "error: stale platform resolution" >&2; exit 1;
    }
    verify_sdk "$KNYTOS_SDK_ROOT"
}
build_signer() {
    load
    local fingerprint
    fingerprint="$(python3 - "$KNYTOS_SDK_ROOT/sdk.json" <<'PY'
import hashlib,sys
print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())
PY
)"
    if [ ! -f "$STATE/signer-input" ] || [ "$(cat "$STATE/signer-input")" != "$fingerprint" ]; then
        rm -rf "$STATE/signer"
        cp -R "$KNYTOS_SDK_ROOT/signer" "$STATE/signer"
        printf '%s\n' "$fingerprint" > "$STATE/signer-input"
    fi
    (cd "$ROOT" && cargo "+$TOOLCHAIN" build --manifest-path "$KNYTOS_SIGNER_MANIFEST" --target-dir "$KNYTOS_SIGNER_TARGET")
    printf 'KNYTOS_CLUSTER_SIGN=%q\n' "$KNYTOS_SIGNER_TARGET/debug/cluster-sign" >> "$ENV_FILE"
}
command="${1:-}"
shift || true
case "$command" in
    use-os) use_os "$@" ;;
    unpack) unpack "$@" ;;
    fetch) fetch ;;
    worktree) worktree "$@" ;;
    runner-os) runner_os "$@" ;;
    build-signer) build_signer ;;
    verify) load; echo 'SDK inputs verified' ;;
    env) load; sed 's/^/export /' "$ENV_FILE" ;;
    status) load; printf 'SDK: %s\nOS: %s\nSitas: %s\n' "$KNYTOS_SDK_ROOT" "$OS_REVISION" "$SITAS_REVISION" ;;
    *) echo 'usage: knytos-sdk.sh use-os DIR | unpack ARCHIVE | fetch | worktree SOURCE [DEST] | runner-os SOURCE | build-signer | verify | env | status' >&2; exit 2 ;;
esac

#!/usr/bin/env bash
#
# charlotte-sdk.sh -- resolve the pinned CharlotteOS platform tooling.
#
# A new or existing project gets its build and signing tools from either a
# CharlotteOS checkout (read-only), an exported application SDK tarball, or a
# sparse fetch of the pinned revision. All commands write the resolution to
# .charlotte/platform.env, which tools/platform.sh loads.
#
# Commands:
#   fetch                    Sparse-clone the pinned revision and use it.
#   use-os DIR     Use an existing CharlotteOS checkout.
#   worktree SOURCE [DIR]    Clone SOURCE locally at the pinned revision (SOURCE stays read-only).
#   runner-os SOURCE         Prepare a private OS/Sitas mirror for QEMU runners.
#   unpack TARBALL           Unpack an exported application SDK tarball.
#   build-signer             Build cluster-sign from the resolved signer tree.
#   env                      Print shell exports for the resolved platform.
#   status                   Show the resolved platform.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE="$ROOT/.charlotte"
ENV_FILE="$STATE/platform.env"
LOCK_FILE="$ROOT/charlotte.lock"

if [ ! -f "$LOCK_FILE" ]; then
    echo "error: missing $LOCK_FILE" >&2
    exit 1
fi
# shellcheck source=/dev/null
. "$LOCK_FILE"

write_env() {
    mkdir -p "$STATE"
    {
        echo "CHARLOTTE_PLATFORM_KIND=$1"
        echo "CHARLOTTE_PLATFORM_ROOT=$2"
        echo "CHARLOTTE_BUILD_ELF=$3"
        echo "CHARLOTTE_SIGNER_MANIFEST=$4"
        echo "CHARLOTTE_SIGNER_TARGET=$5"
        echo "CHARLOTTE_KEYS_DIR=$6"
        echo "CHARLOTTE_OS_REVISION=$OS_REVISION"
        echo "CHARLOTTE_TOOLCHAIN=$TOOLCHAIN"
    } > "$ENV_FILE"
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

check_os_revision() {
    local dir="$1"
    local revision
    revision="$(git -C "$dir" rev-parse HEAD 2>/dev/null || echo unknown)"
    if [ "$revision" != "$OS_REVISION" ]; then
        echo "error: $dir is at $revision but charlotte.lock pins $OS_REVISION" >&2
        echo "       use worktree SOURCE to resolve a private clone at the pin" >&2
        exit 1
    fi
}

cmd_use_os() {
    if [ "$#" -ne 1 ]; then
        echo "usage: $0 use-os <charlotte-os-dir>" >&2
        exit 2
    fi
    local dir="$1"
    dir="$(cd "$dir" && pwd)"
    [ -f "$dir/scripts/build-external-elf.sh" ] || {
        echo "error: $dir is not a CharlotteOS checkout (no scripts/build-external-elf.sh)" >&2
        exit 1
    }
    check_os_revision "$dir"
    # Export the signer into application-owned state: cargo must never create
    # a lockfile or target output in the read-only platform checkout.
    local tarball="$STATE/charlotte-app-sdk-$OS_REVISION.tar.gz"
    "$dir/scripts/export-app-sdk.sh" --output "$tarball"
    cmd_unpack "$tarball"
    local sdk="$STATE/sdk/charlotte-app-sdk"
    write_env os "$dir" "$dir/scripts/build-external-elf.sh" \
        "$sdk/signer/Cargo.toml" "$STATE/signer-target" "$sdk/keys"
    echo ">>> platform: CharlotteOS checkout at $dir"
}

cmd_worktree() {
    local source="${1:-}"
    local worktree="${2:-$STATE/worktree-$OS_REVISION}"
    if [ -z "$source" ]; then
        echo "usage: $0 worktree <charlotte-os-dir> [worktree-dir]" >&2
        exit 2
    fi
    source="$(cd "$source" && pwd)"
    if ! git -C "$source" rev-parse --git-dir >/dev/null 2>&1; then
        echo "error: $source is not a git checkout" >&2
        exit 1
    fi
    if [ -e "$worktree" ]; then
        echo "error: $worktree already exists; remove it or pass another path" >&2
        exit 1
    fi
    # A git worktree would write registration metadata into SOURCE/.git.
    # Use an independent local clone to keep the platform input read-only.
    git clone --no-checkout --no-hardlinks "$source" "$worktree"
    git -C "$worktree" checkout --detach "$OS_REVISION"
    cmd_use_os "$worktree"
}

# QEMU platform runners build the OS and write fixture state. Run them only
# from an application-owned mirror, retaining the OS's Sitas sibling layout.
cmd_runner_os() {
    local source="${1:?usage: runner-os SOURCE}"
    source="$(cd "$source" && pwd)"
    local root="$STATE/runner-$OS_REVISION"
    local os_dir="$root/private/charlotte-os"
    local sitas_dir="$root/gautelis/sitas"
    local sitas_source="${CHARLOTTE_SITAS_DIR:-$source/../../gautelis/sitas}"
    mkdir -p "$root/private" "$root/gautelis"
    if [ ! -d "$os_dir/.git" ]; then
        git clone --no-checkout --no-hardlinks "$source" "$os_dir" >&2
        git -C "$os_dir" checkout --detach "$OS_REVISION" >&2
    fi
    check_os_revision "$os_dir"
    if [ ! -d "$sitas_dir/.git" ]; then
        if [ ! -d "$sitas_source/.git" ]; then
            sitas_source="$SITAS_REPOSITORY"
        fi
        git clone --no-checkout --no-hardlinks "$sitas_source" "$sitas_dir" >&2
        git -C "$sitas_dir" checkout --detach "$SITAS_REVISION" >&2
    fi
    [ "$(git -C "$sitas_dir" rev-parse HEAD)" = "$SITAS_REVISION" ] || {
        echo "error: runner Sitas checkout differs from charlotte.lock" >&2
        exit 1
    }
    printf '%s\n' "$os_dir"
}

cmd_unpack() {
    local tarball="${1:-}"
    if [ -z "$tarball" ]; then
        echo "usage: $0 unpack <sdk.tar.gz>" >&2
        exit 2
    fi
    [ -f "$tarball" ] || { echo "error: no such tarball: $tarball" >&2; exit 1; }
    if [ -n "${CHARLOTTE_SDK_SHA256:-}" ]; then
        local actual
        actual="$(sha256_of "$tarball")"
        if [ "$actual" != "$CHARLOTTE_SDK_SHA256" ]; then
            echo "error: SDK tarball sha256 mismatch" >&2
            echo "       expected $CHARLOTTE_SDK_SHA256" >&2
            echo "       actual   $actual" >&2
            exit 1
        fi
    elif [ -f "$tarball.sha256" ]; then
        local expected
        expected="$(awk '{print $1}' "$tarball.sha256")"
        local actual
        actual="$(sha256_of "$tarball")"
        if [ "$actual" != "$expected" ]; then
            echo "error: SDK tarball sha256 mismatch against $tarball.sha256" >&2
            exit 1
        fi
    fi

    rm -rf "$STATE/sdk"
    mkdir -p "$STATE/sdk"
    tar -xzf "$tarball" -C "$STATE/sdk"
    local sdk="$STATE/sdk/charlotte-app-sdk"
    [ -f "$sdk/build-external-elf.sh" ] || {
        echo "error: $tarball does not contain charlotte-app-sdk/build-external-elf.sh" >&2
        exit 1
    }
    local sdk_revision
    sdk_revision="$(sed -n 's/^os_revision=//p' "$sdk/VERSION" | head -1)"
    if [ "$sdk_revision" != "$OS_REVISION" ]; then
        echo "error: SDK is from $sdk_revision but charlotte.lock pins $OS_REVISION" >&2
        exit 1
    fi
    write_env sdk "$sdk" "$sdk/build-external-elf.sh" \
        "$sdk/signer/Cargo.toml" "$sdk/signer/target" "$sdk/keys"
    echo ">>> platform: application SDK from $tarball"
}

cmd_fetch() {
    local os_dir="$STATE/charlotte-os"
    if [ ! -d "$os_dir/.git" ]; then
        echo ">>> sparse clone of $OS_REPOSITORY"
        git clone --filter=blob:none --no-checkout "$OS_REPOSITORY" "$os_dir"
    fi
    git -C "$os_dir" sparse-checkout init --cone
    git -C "$os_dir" sparse-checkout set \
        crates/catten-services \
        crates/charlotte-launch \
        tools/cluster-sign \
        scripts
    git -C "$os_dir" fetch origin "$OS_REVISION"
    echo ">>> checking out $OS_REVISION"
    git -C "$os_dir" checkout --detach "$OS_REVISION"

    # A sparse checkout cannot build cluster-sign directly: the OS workspace
    # manifest lists members that were not fetched. Export the SDK, which has
    # its own small workspace, unpack it, and use that.
    local tarball="$STATE/charlotte-app-sdk-$OS_REVISION.tar.gz"
    "$os_dir/scripts/export-app-sdk.sh" --output "$tarball"
    cmd_unpack "$tarball"
}

load_env() {
    if [ -f "$ENV_FILE" ]; then
        # shellcheck source=/dev/null
        . "$ENV_FILE"
        if [ "${CHARLOTTE_OS_REVISION:-}" != "$OS_REVISION" ] ||
           [ "${CHARLOTTE_TOOLCHAIN:-}" != "$TOOLCHAIN" ]; then
            echo "error: stale platform resolution; run use-os, fetch, or unpack again" >&2
            exit 1
        fi
    fi
}

cmd_build_signer() {
    load_env
    if [ -z "${CHARLOTTE_SIGNER_MANIFEST:-}" ]; then
        echo "error: resolve a platform first (fetch, use-os, or unpack)" >&2
        exit 1
    fi
    local binary="$CHARLOTTE_SIGNER_TARGET/debug/cluster-sign"
    echo ">>> building cluster-sign with $TOOLCHAIN"
    (cd /tmp && cargo "+$TOOLCHAIN" build --manifest-path "$CHARLOTTE_SIGNER_MANIFEST" --target-dir "$CHARLOTTE_SIGNER_TARGET")
    if [ ! -x "$binary" ]; then
        echo "error: expected signer binary at $binary" >&2
        exit 1
    fi
    grep -v '^CHARLOTTE_CLUSTER_SIGN=' "$ENV_FILE" > "$ENV_FILE.tmp" || true
    mv "$ENV_FILE.tmp" "$ENV_FILE"
    echo "CHARLOTTE_CLUSTER_SIGN=$binary" >> "$ENV_FILE"
    echo ">>> signer: $binary"
}

cmd_env() {
    load_env
    for name in CHARLOTTE_PLATFORM_KIND CHARLOTTE_PLATFORM_ROOT CHARLOTTE_BUILD_ELF \
        CHARLOTTE_SIGNER_MANIFEST CHARLOTTE_SIGNER_TARGET CHARLOTTE_KEYS_DIR \
        CHARLOTTE_OS_REVISION CHARLOTTE_TOOLCHAIN CHARLOTTE_CLUSTER_SIGN; do
        if [ -n "${!name:-}" ]; then
            printf 'export %s=%q\n' "$name" "${!name}"
        fi
    done
}

cmd_status() {
    load_env
    if [ -z "${CHARLOTTE_PLATFORM_ROOT:-}" ]; then
        echo "no platform resolved; run fetch, use-os, or unpack"
        return
    fi
    echo "kind:     $CHARLOTTE_PLATFORM_KIND"
    echo "root:     $CHARLOTTE_PLATFORM_ROOT"
    echo "builder:  $CHARLOTTE_BUILD_ELF"
    echo "signer:   ${CHARLOTTE_CLUSTER_SIGN:-not built ($CHARLOTTE_SIGNER_MANIFEST)}"
    echo "revision: $CHARLOTTE_OS_REVISION"
    echo "toolchain: $CHARLOTTE_TOOLCHAIN"
}

command="${1:-}"
shift || true
case "$command" in
    fetch) cmd_fetch "$@" ;;
    use-os) cmd_use_os "$@" ;;
    worktree) cmd_worktree "$@" ;;
    runner-os) cmd_runner_os "$@" ;;
    unpack) cmd_unpack "$@" ;;
    build-signer) cmd_build_signer "$@" ;;
    env) cmd_env "$@" ;;
    status) cmd_status "$@" ;;
    *)
        echo "usage: $0 fetch | use-os DIR | worktree SOURCE [DIR] | runner-os SOURCE | unpack TARBALL | build-signer | env | status" >&2
        exit 2
        ;;
esac

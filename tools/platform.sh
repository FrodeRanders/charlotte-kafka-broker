#!/usr/bin/env bash
# Shared platform resolution for broker build, package, and deploy scripts.
set -euo pipefail

BROKER_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BROKER_STATE="$BROKER_ROOT/.charlotte"

if [ ! -f "$BROKER_STATE/platform.env" ]; then
    echo "error: no CharlotteOS platform resolved yet" >&2
    echo "       run one of:" >&2
    echo "         tools/charlotte-sdk.sh fetch" >&2
    echo "         tools/charlotte-sdk.sh use-os <charlotte-os-dir>" >&2
    echo "         tools/charlotte-sdk.sh unpack <sdk.tar.gz>" >&2
    exit 1
fi

# shellcheck source=/dev/null
. "$BROKER_STATE/platform.env"

if [ -z "${CHARLOTTE_BUILD_ELF:-}" ]; then
    echo "error: $BROKER_STATE/platform.env does not name a platform builder" >&2
    exit 1
fi

# Refuse stale generated resolution after a deliberate platform pin change.
# shellcheck source=/dev/null
. "$BROKER_ROOT/charlotte.lock"
if [ "${CHARLOTTE_OS_REVISION:-}" != "$OS_REVISION" ] || \
   [ "${CHARLOTTE_TOOLCHAIN:-}" != "$TOOLCHAIN" ]; then
    echo "error: stale .charlotte/platform.env; resolve the pinned platform again" >&2
    exit 1
fi

# The pinned OS creates a unique run directory and prints the exact signed
# descriptor path. Follow that run's result, never a stale result from a prior
# deployment. Both broker runners deploy the logical artifact "broker".
broker_deployment_ready() {
    local os_dir="$1"
    local log="$2"
    [ -f "$log" ] || return 1
    local descriptor
    descriptor="$(sed -n 's/^signed deployment \(.*\/broker\.cdep\): artifact=.*/\1/p' "$log" | tail -1)"
    case "$descriptor" in
        "$os_dir"/target/deployment-ingress-test/run.*/broker.cdep) ;;
        *) return 1 ;;
    esac
    local result="${descriptor%/*}/result"
    [ -f "$result" ] && grep -Fxq ready "$result"
}

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

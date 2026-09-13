#!/usr/bin/env bash
#
# package.sh -- sign the EL0 broker image and print the deployment handoff.
#
# The artifact name, version, rollback counter, and provenance belong to this
# repository; the signing format and keys belong to CharlotteOS. Deployment
# (deployment-sign/deployment-notify) is performed by the operator or CI using
# the printed inputs and the commands in docs/development-model.md.
#
# Usage:
#   tools/package.sh sign [--name NAME] [--elf PATH]
set -euo pipefail

# shellcheck source=/dev/null
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/platform.sh"

MODE="${1:-sign}"
shift || true

NAME="${BROKER_ARTIFACT_NAME:-broker}"
ELF="${BROKER_ELF:-$BROKER_ROOT/target/elf/$NAME.elf}"
while [ "$#" -gt 0 ]; do
    case "$1" in
        --name) NAME="$2"; shift 2 ;;
        --elf) ELF="$2"; shift 2 ;;
        *) echo "usage: $0 sign [--name NAME] [--elf PATH]" >&2; exit 2 ;;
    esac
done

if [ "$MODE" != "sign" ]; then
    echo "usage: $0 sign [--name NAME] [--elf PATH]" >&2
    exit 2
fi
[ -f "$ELF" ] || {
    echo "error: $ELF does not exist; run tools/build-elf.sh first" >&2
    exit 1
}

"$BROKER_ROOT/tools/charlotte-sdk.sh" build-signer
# shellcheck source=/dev/null
. "$BROKER_STATE/platform.env"

if [ -n "${CHARLOTTE_SIGN_KEY_HEX:-}" ]; then
    KEY_HEX="$CHARLOTTE_SIGN_KEY_HEX"
else
    KEY_FILE="$CHARLOTTE_KEYS_DIR/dev-key.hex"
    [ -f "$KEY_FILE" ] || {
        echo "error: no $KEY_FILE; set CHARLOTTE_SIGN_KEY_HEX" >&2
        exit 1
    }
    KEY_HEX="$(grep -v '^#' "$KEY_FILE" | tr -d '[:space:]')"
fi

"$CHARLOTTE_CLUSTER_SIGN" elf-sign "$ELF" "$NAME" "$KEY_HEX" service 1 1 0 -
DIGEST="$("$CHARLOTTE_CLUSTER_SIGN" sha256 "$ELF")"

echo ">>> signed artifact: $ELF"
echo ">>> artifact name:   $NAME"
echo ">>> artifact sha256: $DIGEST"
cat <<EOF
>>> deployment handoff (see docs/development-model.md):
    $CHARLOTTE_CLUSTER_SIGN deployment-sign <descriptor.cdep> $NAME \\
        <object-key> $DIGEST <node-key> \$(date +%s) <stack-pages> <max-threads> <grace> \\
        <private-key-hex> [placement] tcpip=client $NAME=publish
    $CHARLOTTE_CLUSTER_SIGN deployment-notify <descriptor.cdep> 127.0.0.1:\${CATTEN_DEPLOY_HOST_PORT:-8081}
    $CHARLOTTE_CLUSTER_SIGN deployment-status $NAME 127.0.0.1:\${CATTEN_DEPLOY_HOST_PORT:-8081} 120
EOF

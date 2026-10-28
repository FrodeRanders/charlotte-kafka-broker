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

if [ -n "${CHARLOTTE_SIGN_KEY_HEX:-}" ]; then
    echo "error: CHARLOTTE_SIGN_KEY_HEX is retired; use CHARLOTTE_SIGN_KEY_FILE" >&2
    exit 1
fi

"$BROKER_ROOT/tools/charlotte-sdk.sh" build-signer
# shellcheck source=/dev/null
. "$BROKER_STATE/platform.env"

KEY_FILE="${CHARLOTTE_SIGN_KEY_FILE:-$CHARLOTTE_KEYS_DIR/dev-key.hex}"
[ -f "$KEY_FILE" ] || {
    echo "error: no $KEY_FILE; set CHARLOTTE_SIGN_KEY_FILE to a restricted private-key file" >&2
    exit 1
}

# The SDK's publicly known development key is shipped mode 0644. Stage a
# restricted copy locally; private production keys are passed by path as-is.
if [ -z "${CHARLOTTE_SIGN_KEY_FILE:-}" ]; then
    mkdir -p "$BROKER_STATE/keys"
    install -m 0600 "$KEY_FILE" "$BROKER_STATE/keys/dev-key.hex"
    KEY_FILE="$BROKER_STATE/keys/dev-key.hex"
fi

"$CHARLOTTE_CLUSTER_SIGN" elf-sign "$ELF" "$NAME" "$KEY_FILE" service 1 1 0 -
DIGEST="$("$CHARLOTTE_CLUSTER_SIGN" sha256 "$ELF")"

echo ">>> signed artifact: $ELF"
echo ">>> artifact name:   $NAME"
echo ">>> artifact sha256: $DIGEST"
cat <<EOF
>>> CDEPLOY6 deployment handoff (requires management mTLS; see docs/development-model.md):
    $CHARLOTTE_CLUSTER_SIGN deployment-sign <descriptor.cdep> $NAME \\
        <object-key> $ELF <node-key> \$(date +%s) <stack-pages> <max-threads> <grace> \\
        <private-key-file> [placement] tcpip=client $NAME=publish
    $CHARLOTTE_CLUSTER_SIGN deployment-notify <descriptor.cdep> 127.0.0.1:\${CATTEN_DEPLOY_HOST_PORT:-8081}
    $CHARLOTTE_CLUSTER_SIGN deployment-status $NAME 127.0.0.1:\${CATTEN_DEPLOY_HOST_PORT:-8081} 120
EOF

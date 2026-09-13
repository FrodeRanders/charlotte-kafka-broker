#!/usr/bin/env bash
#
# qemu-smoke.sh -- deploy the signed EL0 broker into a single-guest QEMU
# cluster and run the host Kafka-subset smoke client against it.
#
# Requires Docker (the RustFS fixture), QEMU, and a full CharlotteOS checkout
# with the parameterized deployment fixture. Set CHARLOTTE_OS_DIR when the
# resolved platform is an exported SDK.
#
# Usage:
#   CHARLOTTE_OS_DIR=../charlotte-os tools/qemu-smoke.sh
#
# Environment:
#   CATTEN_APP_HOST_PORT   host port forwarded to guest 9092 (default 19099)
#   CATTEN_QEMU_TIMEOUT    runner timeout in seconds (default 480)
set -euo pipefail

# shellcheck source=/dev/null
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/platform.sh"

OS_DIR="${CHARLOTTE_OS_DIR:-}"
if [ -z "$OS_DIR" ]; then
    if [ "${CHARLOTTE_PLATFORM_KIND:-}" = "os" ]; then
        OS_DIR="$CHARLOTTE_PLATFORM_ROOT"
    else
        echo "error: set CHARLOTTE_OS_DIR to a full CharlotteOS checkout to run QEMU" >&2
        exit 1
    fi
fi
if [ ! -x "$OS_DIR/scripts/run-aarch64.sh" ]; then
    echo "error: no QEMU runner at $OS_DIR/scripts/run-aarch64.sh" >&2
    exit 1
fi

APP_HOST_PORT="${CATTEN_APP_HOST_PORT:-19099}"
TIMEOUT="${CATTEN_QEMU_TIMEOUT:-480}"
LOG="$BROKER_ROOT/target/qemu-smoke.log"
RESULT_FILE="$OS_DIR/target/deployment-ingress-test/result"

"$BROKER_ROOT/tools/build-elf.sh"
"$BROKER_ROOT/tools/package.sh" sign
rm -f "$RESULT_FILE"

echo ">>> booting one guest and deploying broker (log: $LOG)"
CATTEN_DEPLOY_NAME=broker \
CATTEN_DEPLOY_ELF="$BROKER_ROOT/target/elf/broker.elf" \
CATTEN_DEPLOY_OBJECT_KEY=deployments/broker.elf \
CATTEN_DEPLOY_STACK_PAGES=8 \
CATTEN_DEPLOY_MAX_THREADS=16 \
CATTEN_DEPLOY_GRACE_MS=5000 \
CATTEN_DEPLOY_GRANTS="tcpip=client broker=publish" \
CATTEN_APP_HOST_PORT="$APP_HOST_PORT" \
CATTEN_APP_GUEST_PORT=9092 \
CATTEN_APP_HOLD_SECONDS="${CATTEN_APP_HOLD_SECONDS:-90}" \
"$OS_DIR/scripts/run-aarch64.sh" debug --deployment-ingress-test --timeout "$TIMEOUT" \
    >"$LOG" 2>&1 &
RUNNER_PID=$!

cleanup() {
    pkill -P "$RUNNER_PID" 2>/dev/null || true
    kill "$RUNNER_PID" 2>/dev/null || true
    wait "$RUNNER_PID" 2>/dev/null || true
}
trap cleanup EXIT

deadline=$((SECONDS + TIMEOUT))
while [ ! -f "$RESULT_FILE" ]; do
    if ! kill -0 "$RUNNER_PID" 2>/dev/null; then
        echo "error: QEMU runner exited before readiness; tail of $LOG:" >&2
        tail -40 "$LOG" >&2 || true
        exit 1
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "error: deployment did not become ready before $TIMEOUT s; tail of $LOG:" >&2
        tail -40 "$LOG" >&2 || true
        exit 1
    fi
    sleep 2
done

echo ">>> broker is ready; running remote smoke on 127.0.0.1:$APP_HOST_PORT"
if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$APP_HOST_PORT" -sTCP:LISTEN 2>/dev/null || echo ">>> warning: nothing is listening on host port $APP_HOST_PORT"
fi
sleep 2
cargo run --quiet --example remote_smoke -- "127.0.0.1:$APP_HOST_PORT"

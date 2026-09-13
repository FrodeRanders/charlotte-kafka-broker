#!/usr/bin/env bash
#
# run_soak.sh -- run the independent Python load client against the broker.
#
# Two modes:
#   --host   start the host TCP front end on 127.0.0.1:9092 and load it;
#   --qemu   build and sign the EL0 image with an advertised address pointing
#            at the host forward, deploy it into a single-guest cluster, and
#            load it until the requested duration elapses.
#
# The QEMU guest bounds each partition to a small retained-byte budget, so the
# client is expected to run for hours without exhausting the 4 MiB EL0 heap.
# Point --bootstrap at a forwarded address for a foreign cluster.
#
# Usage:
#   tools/soak/run_soak.sh --host  --duration 30 --rate 20
#   CHARLOTTE_OS_DIR=../charlotte-os tools/soak/run_soak.sh --duration 43200 --rate 20
set -euo pipefail

# shellcheck source=/dev/null
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/../" && pwd)/platform.sh"

VENV="$BROKER_ROOT/tools/soak/.venv"
CLIENT="$BROKER_ROOT/tools/soak/soak_client.py"
REQUIREMENTS="$BROKER_ROOT/tools/soak/requirements.txt"

MODE="qemu"
DURATION="3600"
RATE="20"
MAX_ERRORS="100"
BOOTSTRAP=""
NO_BUILD="0"
KEEP="0"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --host) MODE="host"; shift ;;
        --qemu) MODE="qemu"; shift ;;
        --duration) DURATION="$2"; shift 2 ;;
        --rate) RATE="$2"; shift 2 ;;
        --max-errors) MAX_ERRORS="$2"; shift 2 ;;
        --bootstrap) BOOTSTRAP="$2"; shift 2 ;;
        --no-build) NO_BUILD="1"; shift ;;
        --keep) KEEP="1"; shift ;;
        *) echo "usage: $0 [--host|--qemu] [--duration S] [--rate N] [--max-errors N] [--bootstrap HOST:PORT] [--no-build] [--keep]" >&2; exit 2 ;;
    esac
done

mkdir -p "$BROKER_ROOT/target"
if [ ! -x "$VENV/bin/python" ]; then
    echo ">>> creating Python load-client environment"
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install --quiet -r "$REQUIREMENTS"
fi

run_client() {
    local bootstrap="$1"
    local log="$2"
    echo ">>> load client: bootstrap=$bootstrap duration=${DURATION}s rate=$RATE/s log=$log"
    "$VENV/bin/python" "$CLIENT" \
        --bootstrap "$bootstrap" \
        --duration "$DURATION" \
        --rate "$RATE" \
        --max-errors "$MAX_ERRORS" 2>&1 | tee "$log"
}

if [ "$MODE" = "host" ]; then
    BOOTSTRAP="${BOOTSTRAP:-127.0.0.1:9092}"
    LOG="$BROKER_ROOT/target/soak-host.log"
    echo ">>> starting host broker on $BOOTSTRAP"
    cargo run --quiet --release --example host_broker >"$BROKER_ROOT/target/host_broker.log" 2>&1 &
    HOST_PID=$!
    cleanup() {
        kill "$HOST_PID" 2>/dev/null || true
        wait "$HOST_PID" 2>/dev/null || true
    }
    trap cleanup EXIT
    port="${BOOTSTRAP##*:}"
    for _ in $(seq 1 120); do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then break; fi
        sleep 0.5
    done
    run_client "$BOOTSTRAP" "$LOG"
    exit 0
fi

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
case "$DURATION" in
    ''|*[!0-9]*) echo "error: --duration must be a positive integer in QEMU mode" >&2; exit 2 ;;
esac
if [ "$DURATION" -le 0 ]; then
    echo "error: --duration must be positive in QEMU mode" >&2
    exit 2
fi

APP_HOST_PORT="${CATTEN_APP_HOST_PORT:-19099}"
BOOTSTRAP="${BOOTSTRAP:-127.0.0.1:$APP_HOST_PORT}"
HOLD=$((DURATION + 120))
TIMEOUT=$((HOLD + 600))
LOG="$BROKER_ROOT/target/soak-qemu.log"
CLIENT_LOG="$BROKER_ROOT/target/soak-client.log"
RESULT_FILE="$OS_DIR/target/deployment-ingress-test/result"

if [ "$NO_BUILD" != "1" ]; then
    echo ">>> building broker-el0 with advertised endpoint 127.0.0.1:$APP_HOST_PORT"
    BROKER_ADVERTISE_HOST=127.0.0.1 BROKER_ADVERTISE_PORT="$APP_HOST_PORT" \
        "$BROKER_ROOT/tools/build-elf.sh"
    "$BROKER_ROOT/tools/package.sh" sign
fi
rm -f "$RESULT_FILE"

echo ">>> booting guest; hold=${HOLD}s timeout=${TIMEOUT}s (log: $LOG)"
CATTEN_DEPLOY_NAME=broker \
CATTEN_DEPLOY_ELF="$BROKER_ROOT/target/elf/broker.elf" \
CATTEN_DEPLOY_OBJECT_KEY=deployments/broker.elf \
CATTEN_DEPLOY_STACK_PAGES=8 \
CATTEN_DEPLOY_MAX_THREADS=16 \
CATTEN_DEPLOY_GRACE_MS=5000 \
CATTEN_DEPLOY_GRANTS="tcpip=client broker=publish" \
CATTEN_APP_HOST_PORT="$APP_HOST_PORT" \
CATTEN_APP_GUEST_PORT=9092 \
CATTEN_APP_HOLD_SECONDS="$HOLD" \
"$OS_DIR/scripts/run-aarch64.sh" debug --deployment-ingress-test --timeout "$TIMEOUT" \
    >"$LOG" 2>&1 &
RUNNER_PID=$!

cleanup() {
    pkill -P "$RUNNER_PID" 2>/dev/null || true
    kill "$RUNNER_PID" 2>/dev/null || true
    wait "$RUNNER_PID" 2>/dev/null || true
}
trap cleanup EXIT

deadline=$((SECONDS + 900))
while [ ! -f "$RESULT_FILE" ]; do
    if ! kill -0 "$RUNNER_PID" 2>/dev/null; then
        echo "error: QEMU runner exited before readiness; tail of $LOG:" >&2
        tail -40 "$LOG" >&2 || true
        exit 1
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "error: broker did not become ready; tail of $LOG:" >&2
        tail -40 "$LOG" >&2 || true
        exit 1
    fi
    sleep 2
done

run_client "$BOOTSTRAP" "$CLIENT_LOG"
if [ "$KEEP" = "1" ]; then
    echo ">>> --keep set; guest stays up until the runner timeout"
    trap - EXIT
    wait "$RUNNER_PID" 2>/dev/null || true
fi

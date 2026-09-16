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
# The QEMU guest architecture follows the host by default (x86_64 on Intel,
# aarch64 on Apple Silicon and ARM), and --arch overrides it.
#
# The QEMU guest bounds each partition to a small retained-byte budget, so the
# client is expected to run for hours without exhausting the 4 MiB EL0 heap.
# Point --bootstrap at a forwarded address for a foreign cluster.
#
# Usage:
#   tools/soak/run_soak.sh --host  --duration 30 --rate 20
#   CHARLOTTE_OS_DIR=../charlotte-os tools/soak/run_soak.sh --duration 43200 --rate 100 --producers 8
#   CHARLOTTE_OS_DIR=../charlotte-os tools/soak/run_soak.sh --kernel-profile release --no-net-dump
#
# --rate is the aggregate offered load and --producers sizes the connection
# pool. A synchronous producer offers at most one record per broker round trip,
# so high rates need enough concurrent producers to cover the guest's latency.
# The QEMU guest derives its socket-set capacity from the tcpip heap. The
# default image currently has 64 slots and a 64-socket per-principal quota;
# closing TCP sockets may remain charged until smoltcp reaches a final state.
# QEMU runs use a debug kernel and packet capture by default; use
# `--kernel-profile release` and `--no-net-dump` for a leaner measurement.
#
# A QEMU guest is preserved by default when the load client fails, so the
# running kernel can be inspected. Use --cleanup-on-failure in unattended CI.
set -euo pipefail

# shellcheck source=/dev/null
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/../" && pwd)/platform.sh"

VENV="$BROKER_ROOT/tools/soak/.venv"
CLIENT="$BROKER_ROOT/tools/soak/soak_client.py"
REQUIREMENTS="$BROKER_ROOT/tools/soak/requirements.txt"

MODE="qemu"
ARCH="auto"
DURATION="3600"
RATE="20"
PRODUCERS="4"
MAX_ERRORS="100"
BOOTSTRAP=""
NO_BUILD="0"
KEEP="0"
CLEANUP_ON_FAILURE="0"
KERNEL_PROFILE="${CATTEN_SOAK_KERNEL_PROFILE:-debug}"
NET_DUMP="${CATTEN_SOAK_NET_DUMP:-1}"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --host) MODE="host"; shift ;;
        --qemu) MODE="qemu"; shift ;;
        --arch) ARCH="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --rate) RATE="$2"; shift 2 ;;
        --producers) PRODUCERS="$2"; shift 2 ;;
        --max-errors) MAX_ERRORS="$2"; shift 2 ;;
        --bootstrap) BOOTSTRAP="$2"; shift 2 ;;
        --no-build) NO_BUILD="1"; shift ;;
        --keep) KEEP="1"; shift ;;
        --cleanup-on-failure) CLEANUP_ON_FAILURE="1"; shift ;;
        --kernel-profile)
            [ "$#" -ge 2 ] || { echo "Missing value for --kernel-profile" >&2; exit 2; }
            KERNEL_PROFILE="$2"; shift 2 ;;
        --net-dump) NET_DUMP="1"; shift ;;
        --no-net-dump) NET_DUMP="0"; shift ;;
        *)
            echo "usage: $0 [--host|--qemu] [--arch aarch64|x86_64|auto] [--duration S] [--rate N] [--producers N] [--max-errors N] [--bootstrap HOST:PORT] [--kernel-profile debug|release] [--net-dump|--no-net-dump] [--no-build] [--keep] [--cleanup-on-failure]" >&2
            exit 2
            ;;
    esac
done

case "$KERNEL_PROFILE" in
    debug|release) ;;
    *) echo "error: --kernel-profile must be debug or release" >&2; exit 2 ;;
esac
case "$NET_DUMP" in
    0|1) ;;
    *) echo "error: packet capture setting must be 0 or 1" >&2; exit 2 ;;
esac

mkdir -p "$BROKER_ROOT/target"

ensure_client_env() {
    local python="${CHARLOTTE_SOAK_PYTHON:-python3}"
    if ! command -v "$python" >/dev/null 2>&1; then
        echo "error: $python not found; set CHARLOTTE_SOAK_PYTHON to a Python 3 interpreter" >&2
        exit 1
    fi

    # Treat a directory without a marker file or a working interpreter as a
    # partial environment left by an interrupted create or install.
    local recreate=0
    if [ ! -f "$VENV/pyvenv.cfg" ] || [ ! -x "$VENV/bin/python" ]; then
        recreate=1
    elif ! "$VENV/bin/python" -c "import sys" >/dev/null 2>&1; then
        recreate=1
    fi
    if [ "$recreate" = "1" ]; then
        rm -rf "$VENV"
        echo ">>> creating Python load-client environment with $python"
        if ! "$python" -m venv "$VENV"; then
            echo "error: creating a virtualenv failed" >&2
            echo "       Debian/Ubuntu: apt install python3-venv" >&2
            echo "       or set CHARLOTTE_SOAK_PYTHON to another Python 3" >&2
            exit 1
        fi
    fi

    if ! "$VENV/bin/python" -m pip --version >/dev/null 2>&1; then
        echo ">>> bootstrapping pip in $VENV"
        if ! "$VENV/bin/python" -m ensurepip --upgrade >/dev/null 2>&1 \
            && ! "$VENV/bin/python" -m ensurepip --upgrade --default-pip >/dev/null 2>&1; then
            echo "error: pip is unavailable in $VENV and ensurepip failed" >&2
            echo "       Debian/Ubuntu: apt install python3-venv" >&2
            echo "       or set CHARLOTTE_SOAK_PYTHON to another Python 3" >&2
            exit 1
        fi
    fi

    # A venv can exist while the dependency install failed or was interrupted;
    # verify the import and repair instead of trusting the interpreter.
    if ! "$VENV/bin/python" -c "import kafka" >/dev/null 2>&1; then
        echo ">>> installing Python load-client dependencies"
        "$VENV/bin/python" -m pip install --quiet --upgrade pip >/dev/null 2>&1 || true
        if ! "$VENV/bin/python" -m pip install --quiet -r "$REQUIREMENTS"; then
            echo "error: installing $REQUIREMENTS failed; check network access" >&2
            exit 1
        fi
    fi
    if ! "$VENV/bin/python" -c "import kafka" >/dev/null 2>&1; then
        echo "error: kafka-python is still not importable from $VENV" >&2
        exit 1
    fi
    echo ">>> load client: $("$VENV/bin/python" -c 'import kafka; print("kafka-python", kafka.__version__)')"
}

ensure_client_env

run_client() {
    local bootstrap="$1"
    local log="$2"
    echo ">>> load client: bootstrap=$bootstrap duration=${DURATION}s producers=$PRODUCERS rate=$RATE/s log=$log"
    "$VENV/bin/python" "$CLIENT" \
        --bootstrap "$bootstrap" \
        --duration "$DURATION" \
        --rate "$RATE" \
        --producers "$PRODUCERS" \
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
if [ "$ARCH" = "auto" ]; then
    case "$(uname -m)" in
        x86_64|amd64) ARCH="x86_64" ;;
        arm64|aarch64) ARCH="aarch64" ;;
        *) ARCH="aarch64" ;;
    esac
fi
case "$ARCH" in
    aarch64|x86_64) ;;
    *) echo "error: --arch must be aarch64, x86_64, or auto" >&2; exit 2 ;;
esac
RUNNER="$OS_DIR/scripts/run-$ARCH.sh"
if [ ! -x "$RUNNER" ]; then
    echo "error: no QEMU runner at $RUNNER" >&2
    exit 1
fi
if [ "$ARCH" = "x86_64" ] && ! grep -q "CATTEN_DEPLOY_NAME" "$RUNNER"; then
    echo "error: $RUNNER does not yet support the external-artifact deployment fixture" >&2
    echo "       rerun with --arch aarch64 until the x86_64 fixture lands" >&2
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
RUNNER_PID_FILE="$BROKER_ROOT/target/soak-runner.pid"
QEMU_PID_FILE="$BROKER_ROOT/target/soak-qemu.pid"
GDB_PORT="${CATTEN_SOAK_GDB_PORT:-1234}"
MONITOR_SOCKET="/tmp/charlotte-monitor.sock"
PCAP_FILE="/tmp/charlotte-net.pcap"
SERIAL_LOG="/tmp/charlotte-serial.log"
if [ "$ARCH" = "x86_64" ]; then
    SERIAL_LOG="/tmp/charlotte-x86-serial.log"
fi
KERNEL_BINARY="$OS_DIR/target/${ARCH}-unknown-none-catten/${KERNEL_PROFILE}/catten"

if [ "$NO_BUILD" != "1" ]; then
    echo ">>> building broker-el0 for $ARCH with advertised endpoint 127.0.0.1:$APP_HOST_PORT"
    BROKER_ADVERTISE_HOST=127.0.0.1 BROKER_ADVERTISE_PORT="$APP_HOST_PORT" \
        "$BROKER_ROOT/tools/build-elf.sh" --arch "$ARCH"
    "$BROKER_ROOT/tools/package.sh" sign
fi
rm -f "$RESULT_FILE" "$RUNNER_PID_FILE" "$QEMU_PID_FILE" "$MONITOR_SOCKET" "$PCAP_FILE"

echo ">>> booting $ARCH guest; kernel=${KERNEL_PROFILE} net_dump=${NET_DUMP} hold=${HOLD}s timeout=${TIMEOUT}s (log: $LOG)"
CATTEN_DEPLOY_NAME=broker \
CATTEN_DEPLOY_ELF="$BROKER_ROOT/target/elf/broker.elf" \
CATTEN_DEPLOY_OBJECT_KEY=deployments/broker.elf \
CATTEN_DEPLOY_STACK_PAGES=8 \
CATTEN_DEPLOY_MAX_THREADS=64 \
CATTEN_DEPLOY_GRACE_MS=5000 \
CATTEN_DEPLOY_GRANTS="tcpip=client broker=publish" \
CATTEN_APP_HOST_PORT="$APP_HOST_PORT" \
CATTEN_APP_GUEST_PORT=9092 \
CATTEN_APP_HOLD_SECONDS="$HOLD" \
CATTEN_QEMU_PID_FILE="$QEMU_PID_FILE" \
CATTEN_QEMU_MONITOR=1 \
CATTEN_QEMU_NET_DUMP="$NET_DUMP" \
CATTEN_QEMU_DEBUG_STUB=1 \
"$RUNNER" "$KERNEL_PROFILE" --deployment-ingress-test --gdb-port "$GDB_PORT" \
    --timeout "$TIMEOUT" \
    >"$LOG" 2>&1 &
RUNNER_PID=$!
printf '%s\n' "$RUNNER_PID" >"$RUNNER_PID_FILE"

cleanup() {
    pkill -P "$RUNNER_PID" 2>/dev/null || true
    kill "$RUNNER_PID" 2>/dev/null || true
    wait "$RUNNER_PID" 2>/dev/null || true
}
trap cleanup EXIT

preserve_guest() {
    local reason="${1:-soak failure}"
    trap - EXIT
    disown "$RUNNER_PID" 2>/dev/null || true
    echo ">>> $reason; leaving the CharlotteOS guest running for diagnosis" >&2
    echo ">>> runner PID: $RUNNER_PID (recorded in $RUNNER_PID_FILE)" >&2
    if [ -s "$QEMU_PID_FILE" ]; then
        echo ">>> QEMU PID: $(tr -d '[:space:]' <"$QEMU_PID_FILE") (recorded in $QEMU_PID_FILE)" >&2
    else
        echo ">>> QEMU PID will be recorded in $QEMU_PID_FILE" >&2
    fi
    echo ">>> serial: $SERIAL_LOG" >&2
    if [ "$NET_DUMP" = "1" ]; then
        echo ">>> packet capture: $PCAP_FILE" >&2
    fi
    echo ">>> QEMU monitor: $MONITOR_SOCKET" >&2
    echo ">>> debugger: lldb $KERNEL_BINARY -o 'gdb-remote $GDB_PORT'" >&2
    echo ">>> the runner retains its ${TIMEOUT}s safety timeout" >&2
}

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
        if [ "$CLEANUP_ON_FAILURE" != "1" ]; then
            preserve_guest "broker readiness deadline expired"
        fi
        exit 1
    fi
    sleep 2
done

set +e
run_client "$BOOTSTRAP" "$CLIENT_LOG"
CLIENT_STATUS=$?
set -e
if [ "$CLIENT_STATUS" -ne 0 ]; then
    if [ "$CLEANUP_ON_FAILURE" = "1" ]; then
        exit "$CLIENT_STATUS"
    fi
    preserve_guest "load client failed (status $CLIENT_STATUS)"
    exit "$CLIENT_STATUS"
fi
if [ "$KEEP" = "1" ]; then
    echo ">>> --keep set; guest stays up until the runner timeout"
    trap - EXIT
    wait "$RUNNER_PID" 2>/dev/null || true
fi

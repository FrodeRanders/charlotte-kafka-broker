#!/usr/bin/env bash
#
# build-elf.sh -- build the EL0 broker image through the pinned platform.
#
# All platform inputs come from the resolved Knytos checkout or
# application SDK; this script only names the application binary and output.
set -euo pipefail

# shellcheck source=/dev/null
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/platform.sh"

exec "$KNYTOS_BUILD_ELF" \
    --manifest "$BROKER_ROOT/crates/broker-el0/Cargo.toml" \
    --bin broker-el0 \
    --target-dir "$BROKER_ROOT/target/knytos" \
    --output "$BROKER_ROOT/target/elf/broker.elf" \
    "$@"

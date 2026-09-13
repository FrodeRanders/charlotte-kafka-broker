#!/usr/bin/env bash
#
# build-elf.sh -- build the EL0 broker image through the pinned platform.
#
# All platform inputs come from the resolved CharlotteOS checkout or
# application SDK; this script only names the application binary and output.
set -euo pipefail

# shellcheck source=/dev/null
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/platform.sh"

exec "$CHARLOTTE_BUILD_ELF" \
    --manifest "$BROKER_ROOT/crates/broker-el0/Cargo.toml" \
    --bin broker-el0 \
    --target-dir "$BROKER_ROOT/target/charlotte" \
    --output "$BROKER_ROOT/target/elf/broker.elf" \
    "$@"

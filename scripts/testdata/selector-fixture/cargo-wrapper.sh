#!/usr/bin/env bash

set -euo pipefail

: "${REAL_CARGO:?REAL_CARGO must name the real cargo executable}"
: "${SELECTOR_ARG_LOG:?SELECTOR_ARG_LOG must name the argument log}"

printf '%s\n' "$@" >"$SELECTOR_ARG_LOG"
exec "$REAL_CARGO" "$@"

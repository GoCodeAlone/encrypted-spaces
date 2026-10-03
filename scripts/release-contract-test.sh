#!/usr/bin/env bash

set -euo pipefail

if (( $# < 1 )); then
    printf 'usage: %s <test-prefix> [-- <harness-args...>]\n' "$0" >&2
    exit 2
fi

prefix="$1"
shift

if (( $# > 0 )) && [[ "$1" == "--" ]]; then
    shift
fi

manifest_path="tools/release-contract/Cargo.toml"
if [[ ! -f "$manifest_path" ]]; then
    printf 'error: release-contract manifest not found: %s\n' "$manifest_path" >&2
    exit 1
fi

cargo_bin="${CARGO:-cargo}"
list_output="$("$cargo_bin" test --locked --manifest-path "$manifest_path" "$prefix" -- --list "$@")"
printf '%s\n' "$list_output"

test_count="$(printf '%s\n' "$list_output" | awk -v prefix="$prefix" '
    /: test$/ {
        name = $0
        sub(/: test$/, "", name)
        count_parts = split(name, parts, "::")
        if (index(parts[count_parts], prefix) == 1) {
            count++
        }
    }
    END { print count + 0 }
')"
if (( test_count == 0 )); then
    printf 'error: release-contract test prefix %q matched no test-name components\n' \
        "$prefix" >&2
    exit 1
fi

"$cargo_bin" test --locked --manifest-path "$manifest_path" "$prefix" -- "$@"

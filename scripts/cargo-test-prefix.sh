#!/usr/bin/env bash

set -euo pipefail

if (( $# < 2 )); then
    printf 'usage: %s <package> <test-prefix> [-- <harness-args...>]\n' "$0" >&2
    exit 2
fi

package="$1"
prefix="$2"
shift 2

if (( $# > 0 )) && [[ "$1" == "--" ]]; then
    shift
fi

cargo_bin="${CARGO:-cargo}"
list_output="$("$cargo_bin" test --locked -p "$package" "$prefix" -- --list "$@")"
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
    printf 'error: test prefix %q matched no test-name components in package %q\n' \
        "$prefix" "$package" >&2
    exit 1
fi

"$cargo_bin" test --locked -p "$package" "$prefix" -- "$@"

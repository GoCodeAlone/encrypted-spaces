#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture="$repo_root/scripts/testdata/selector-fixture"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

(
    cd "$fixture"
    SELECTOR_FIXTURE_MARKER="$scratch/known-prefix" \
        bash "$repo_root/scripts/cargo-test-prefix.sh" \
        selector-fixture selector_known_
)
test -f "$scratch/known-prefix"

if (
    cd "$fixture"
    bash "$repo_root/scripts/cargo-test-prefix.sh" \
        selector-fixture selector_missing_
); then
    printf 'error: zero-match selector unexpectedly succeeded\n' >&2
    exit 1
fi

if (
    cd "$fixture"
    bash "$repo_root/scripts/cargo-test-prefix.sh" \
        selector-fixture selector_suffix_
); then
    printf 'error: substring-only selector unexpectedly succeeded\n' >&2
    exit 1
fi

if (
    cd "$fixture"
    bash "$repo_root/scripts/cargo-test-prefix.sh" \
        selector-fixture selector_known_ -- --exact
); then
    printf 'error: harness-filtered zero-test selector unexpectedly succeeded\n' >&2
    exit 1
fi

(
    cd "$fixture"
    bash "$repo_root/scripts/cargo-test-prefix.sh" \
        selector-fixture selector_harness_argument_probe -- --nocapture
)

real_cargo="$(command -v cargo)"
(
    cd "$fixture"
    REAL_CARGO="$real_cargo" \
        SELECTOR_ARG_LOG="$scratch/release-contract-args" \
        CARGO="$fixture/cargo-wrapper.sh" \
        bash "$repo_root/scripts/release-contract-test.sh" \
        release_contract_selector_ -- --nocapture
)

grep -Fx -- '--locked' "$scratch/release-contract-args" >/dev/null
grep -Fx -- '--manifest-path' "$scratch/release-contract-args" >/dev/null
grep -Fx -- 'tools/release-contract/Cargo.toml' "$scratch/release-contract-args" >/dev/null
grep -Fx -- '--nocapture' "$scratch/release-contract-args" >/dev/null

if (
    cd "$fixture"
    bash "$repo_root/scripts/release-contract-test.sh" \
        release_contract_suffix_
); then
    printf 'error: release-contract substring-only selector unexpectedly succeeded\n' >&2
    exit 1
fi

if (
    cd "$fixture"
    bash "$repo_root/scripts/release-contract-test.sh" \
        release_contract_selector_ -- --exact
); then
    printf 'error: release-contract harness-filtered zero-test selector unexpectedly succeeded\n' >&2
    exit 1
fi

printf 'PASS: cargo test selectors require nonzero matches and preserve locked harness arguments\n'

# Upstream Delta and Release Ledger

This ledger records the intentional delta from upstream commit
`4cda0ae87698135aa672990e6e68cf7873847426`.

## Applied Changes

- `800495f` pins `kdl` to `=6.5.0` and repairs the matching lock entries for
  Rust `1.94.1`.
- Adds `encrypted-spaces-bridge`, a typed versioned JSONL stdio boundary with
  bounded frames and secret-redacted protocol errors.
- Adds boundary tests for the bridge lifecycle, data, membership, cancellation,
  framing, process exit, and release contract surfaces.
- Adds a public-repository release contract workflow. It does not publish and
  `RELEASE_READY=false` is intentional.

## Pending Release Work

- All SDK/backend runtime behavior. Every declared operation currently returns
  `NOT_IMPLEMENTED`.
- Real Linux/macOS amd64/arm64 builds, checksums, and provenance attestations.
- The Apache `NOTICE` attribution file. The release workflow keeps this as a
  failing legal-release gate until supplied.

# Upstream Delta and Release Ledger

This ledger records the intentional delta from upstream commit
`4cda0ae87698135aa672990e6e68cf7873847426`.

## Applied Changes

- `800495f` pins `kdl` to `=6.5.0` and repairs the matching lock entries for
  Rust `1.94.1`.
- Adds `encrypted-spaces-bridge`, a typed versioned JSONL stdio boundary with
  bounded frames, secret-redacted errors, process-owned trust configuration,
  cancellable waits, and SDK-backed space, table, list, text, file, and member
  operations.
- Adds an owned schema-bytes SDK input for runtime-loaded KDL without leaking
  configuration to obtain a static lifetime.
- Adds launched backend/bridge conformance tests covering restart restoration,
  multi-process membership, verified synchronization, revocation, and every
  bridge data primitive.
- Adds native Linux/macOS amd64/arm64 release automation with real RISC Zero
  guest builds, checksums, in-toto provenance, Apache attribution, and tag-only
  GitHub release publication.

# Encrypted Collaboration Spaces

A cryptographic framework for building collaborative applications over an
untrusted server, and a working prototype implementation.

The cryptographic design is described in full in the
[whitepaper](https://encryptedspaces.org/whitepapers/encrypted-spaces.pdf). For a
high-level overview of the project, see
[encryptedspaces.org](https://encryptedspaces.org).

## ⚠️⚠️⚠️ DO NOT USE IN PRODUCTION ⚠️⚠️⚠️

This is experimental code published for research purposes.

The implementation remains under active development and has not yet undergone the level of security review, testing, auditing, and hardening required for production deployment. The issues tracked in this repository are not a complete accounting of security limitations, known issues, or remaining risks.

***This code MUST NOT be used to protect sensitive data or in security-critical applications.***

For example:
- **Authentication is a placeholder.** The reference server accepts a
  client-asserted identity on connection and does not yet verify it. Security
  rests on the cryptographic verification each client performs on server
  responses, *not* on server-enforced access control.
- **Fast-forward proofs require `--features real-proofs`.** Default builds run
  RISC Zero in dev mode (`RISC0_DEV_MODE=1`), which accepts non-cryptographic
  "fake" receipts for fast iteration. Builds intended to rely on succinct
  fast-forward proofs must enable the `real-proofs` feature.
- **DoS hardening is incomplete.** Some deserialization paths are not yet
  depth-bounded, so malformed input can exhaust resources. The insider-DoS
  resistance described in the design goals below is a target, not a hardened
  guarantee in this prototype.

## What is this?

Most collaboration software stores shared application state on servers in
plaintext, requiring users to trust those servers (and any integrated
services) with the contents of their documents, chats, and databases.
End-to-end encryption addresses messaging but does not directly generalize
to applications where participants must read, modify, and verify long-lived
structured state.

An *Encrypted Collaboration Space* is a shared, mutable application state
with dynamic membership, shared cryptographic key material, an authenticated
history of operations, and a verifiable database representing current
contents. The server holds only ciphertexts and proof material; clients
verify every server response locally with cryptographic proofs.

The design targets four security properties:

- **Verifiable history.** Members can confirm that the membership list and
  shared data are the correct result of all previous operations.
- **Selective data retention.** Members can delete shared data items and
  give new members access to the non-deleted older data.
- **Insider robustness.** Malicious insiders cannot compromise security of
  communications occurring before or after their membership, or cause
  denial of service for other members.
- **Deniable sender authentication.** Members can authenticate the author
  of each data object without producing publicly-verifiable cryptographic
  evidence of user relationships.

## Architecture at a glance

- **Changelog.** Append-only, hash-chained log of operations. The
  authenticated history of a Space.
- **Verifiable database.** Current state exposed via Merkle search trees;
  clients check the database commitment against the latest changelog
  commitment on every response.
- **Tables, lists, text.** Relational rows with primary-key and
  secondary-index search trees; ordered lists via keyless
  order-statistic trees; collaborative text layered on lists.
- **Membership.** A members table tracks who can participate. Existing
  members invite new ones by issuing provisional keys; removal triggers a
  rekey of the remaining members, without requiring re-encryption of data.
- **Access control.** Per-table rules constrain which members can write or
  delete which rows, enforced cryptographically rather than by
  server policy.
- **Retention.** Members can grant new joiners access to historic data, or
  selectively delete data before a chosen point so neither the server nor
  future members can read it, without re-encrypting the entire Space.
- **Fast-forward proofs.** Succinct zero-knowledge proofs that let clients
  skip ahead in the changelog without replaying every operation.

## Repository layout

| Path           | Contents                                                |
| -------------- | ------------------------------------------------------- |
| `sdk/`         | Client SDK and verifiable database API (start here)     |
| `crypto/`      | Core cryptographic primitives                           |
| `zkp/`         | Zero-knowledge proof system                             |
| `ffproof/`     | Fast-forward changelog proofs                           |
| `retention/`   | Selective data retention construction                   |
| `key_manager/` | Group key state and rekey protocols                     |
| `backend/`     | Reference server implementation                         |
| `demos/`       | Example applications                                    |

## Using the SDK

The `sdk/` crate is the main interface for application developers. It
exposes a relational database API: tables, rows, schemas, and access
control rules. It also handles encryption, proof verification, and
synchronization with the backend behind the scenes.

See [`sdk/README.md`](sdk/README.md) for an overview, code examples, and
quickstart instructions.

## Runtime bridge

The `encrypted-spaces-bridge` binary exposes the Rust SDK as bounded,
versioned JSONL RPC over standard input and output. It is intended for
non-Rust clients that need the prototype's actual encryption, verification,
storage, synchronization, and membership behavior without reimplementing
those algorithms.

Each process owns one untrusted diagnostic label, one schema trust bundle, one
backend endpoint, and at most one active Space. Configure them before launch:

```sh
export ENCRYPTED_SPACES_CLIENT_LABEL=local-client
export ENCRYPTED_SPACES_SCHEMA_PATH=/path/to/app-schema.kdl
export ENCRYPTED_SPACES_BACKEND_URL=ws://127.0.0.1:8080/ws
# Optional, 1..3600000; defaults to 30000.
export ENCRYPTED_SPACES_REQUEST_TIMEOUT_MS=30000
# Optional diagnostics go to stderr; the default filter is `warn`.
export RUST_LOG=encrypted_spaces_sdk=info,encrypted_spaces_changelog_core=debug
encrypted-spaces-bridge
```

Requests cannot override the schema, data commitment, or fast-forward guest
image ID. `hello` reports those process-derived trust values and the diagnostic
client label. The label is not an identity or authorization claim; Space
credentials and membership proofs establish authorization. The
bridge reserves stdout for JSONL responses. Configurable host SDK and verifier
diagnostics use stderr through the standard `RUST_LOG` filter; verifier code
inside a RISC Zero guest uses the zkVM debug console instead. The
bridge supports Space create/join/snapshot/restore/sync, table insert/select,
scoped list and collaborative text operations, encrypted file put/get,
member invite/join/remove, cancellation, close, and shutdown. Request and
response payloads use protocol version `1`; request frames larger than 64 KiB
are rejected, and oversized results are replaced by a correlated
`RESPONSE_TOO_LARGE` error. A waiting `space.sync` wakes on an SDK-applied
remote update or its configured timeout and reports that cause as
`wait_trigger`. It is cancellable while waiting or queued for the runtime;
once verified synchronization starts, it runs to completion so cancellation
cannot leave provisional cryptographic state behind. Native SDK WebSocket and
file requests share a bounded process-configured deadline that defaults to 30
seconds and can be raised to one hour for real proof generation. Timed-out or
canceled requests remove their abandoned correlation entries. HTTP response
bodies are streamed with a 64 MiB ceiling before bridge response encoding.
Opaque list and text references include the creating Space ID and cannot be
reused after the process activates another Space.

If a mutating WebSocket request times out after transmission begins, the bridge
returns `COMMIT_UNKNOWN` rather than reporting a definite failure. The backend
may have committed the operation; clients must synchronize and inspect current
state before deciding whether another mutation is required.

Restore validates the snapshot's initial commitment, application schemas and
actions, fast-forward guest image ID, and matching snapshot/authentication
Space IDs against the process configuration before authenticating or
activating the Space. A foreign or identity-inconsistent snapshot fails with
`TRUST_MISMATCH` instead of replacing process-pinned trust roots.

`text.edit` validates positions before mutation and preserves typed SDK errors
when its first operation is rejected. Because a replacement can span multiple
SDK commits, a failure in the second operation after a successful delete
returns `PARTIAL_COMMIT`; clients must synchronize and read the document before
continuing rather than retrying the edit blindly.

Invites and snapshots contain private client custody material and must be
stored as secrets. The backend remains untrusted and stores ciphertext and
proof material, but loss of a client snapshot can prevent that client from
recovering its Space state. This bridge does not add authentication to the
prototype server.

Release archives contain native backend and bridge binaries for Linux and
macOS on amd64 and arm64, together with checksums, provenance, and Apache
attribution. Release binaries enable the `real-proofs` feature and embed real,
nonzero RISC Zero guest images, including the in-process prover needed on a
clean host. The packaged-binary API conformance suite sets `RISC0_DEV_MODE=1`
explicitly to test runtime behavior without performing a full proof for every
API case; a separate Linux release gate launches the packaged bridge and
release backend, removes `r0vm` from the runtime environment, and has a second
client verify a real fast-forward receipt with development mode unset. Both
binaries report their release with `--version`.

The build and real-proof workflow is read-only and cannot attest or publish.
A separate `workflow_run` publisher loaded from the default branch verifies
that the completed run's exact commit is on `main`, rechecks every archive and
checksum, and only then obtains OIDC attestation or release permissions. A tag
is publishable only when `v0.1.0` resolves to that verified `main` commit.

## Prerequisites

The Rust toolchain is pinned by [`rust-toolchain.toml`](rust-toolchain.toml)
and installed automatically by `cargo`; you only need
[rustup](https://rustup.rs/) itself.

| Dependency                   | Needed for                                  | Notes                                                            |
| ---------------------------- | ------------------------------------------- | ---------------------------------------------------------------- |
| rustup / Rust                | everything                                  | toolchain auto-installs from `rust-toolchain.toml`               |
| protobuf compiler            | building the workspace                      |                                                                  |
| Node.js + npm                | the Tauri demo                              |                                                                  |
| WebKit / GTK system libraries | the Tauri demo (Linux only)                | see [Tauri prerequisites](https://v2.tauri.app/start/prerequisites/) |
| Python 3 (with `venv`)       | the demo launcher                           | the launcher bootstraps its own venv + Textual                   |
| RISC Zero                    | **optional** — succinct fast-forward proofs | skip with `RISC0_SKIP_BUILD=1`                                   |

### macOS

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
brew install protobuf node
# Optional: RISC Zero, only for succinct fast-forward proofs
curl -L https://risczero.com/install | bash
```

### Ubuntu

```bash
sudo apt install libssl-dev pkg-config protobuf-compiler nodejs npm \
    libwebkit2gtk-4.1-dev libgtk-3-dev libayatana-appindicator3-dev librsvg2-dev
# Optional: RISC Zero, only for succinct fast-forward proofs
curl -L https://risczero.com/install | bash
```

See [`openssl-sys`](https://docs.rs/openssl/latest/openssl/#automatic) for
other systems, and the
[Tauri prerequisites guide](https://v2.tauri.app/start/prerequisites/) for the
demo's system dependencies on other platforms.

### Build and test

```bash
cargo build
cargo test
```

RISC Zero is only required for succinct fast-forward proofs. If you have not
installed it, skip building the zkVM guest by setting `RISC0_SKIP_BUILD=1`:

```bash
RISC0_SKIP_BUILD=1 cargo build
```

`--release` improves runtime performance significantly at the cost of
compile time.

## Demo

The canonical end-to-end demo lives in `demos/tauri`. This demonstrates
how one might build various applications such as a multi-channel chat,
document editor, calendar, task list, and file system. The demo is built
with [Tauri v2](https://v2.tauri.app/) and [Next.js](https://nextjs.org/).
It exercises the full stack: the Rust SDK manages a `Space`, encrypts and
signs each operation, and synchronizes with the reference backend over WebSocket;
the React frontend talks to the SDK through Tauri's IPC bridge. Multiple instances
connect to the same backend and verify each other's writes via cryptographic proofs.

Once the prerequisites above are installed, run the launcher from the
repository root:

```bash
python3 demos/tauri/demo_launcher.py
```

The launcher builds everything, starts the backend and the Next.js dev server,
and provides a TUI for spawning additional client instances. It bootstraps its
own Python virtual environment (installing
[Textual](https://github.com/Textualize/textual) for the TUI); the remaining
prerequisites — Node.js and, on Linux, the WebKit/GTK libraries — must already
be installed. If RISC Zero is not detected, the launcher automatically builds
and runs without succinct fast-forward proofs.

See [`demos/tauri/README.md`](demos/tauri/README.md) for prerequisites,
manual run instructions, and architecture details.

## About

Encrypted Spaces is a project of the Encrypted Spaces Foundation, a nonprofit corporation,
developed with close collaboration and support from the Cryptography Group at Microsoft Research
and the Applied Social Media Lab at Harvard's
Berkman Klein Center for Internet & Society.

## License

Apache License 2.0. See [LICENSE](LICENSE).

# Remi SDK

This repository contains the shared Remi Rust SDK crates used by Remi clients.

## Crates

- `remi-client-sdk`: local-first runtime APIs for Things, chat/auth/sync, notifications, app keys, realtime, and shared transport.
- `remi-things-crdt`: CRDT domain model, operations, extraction, materialization, and schema helpers for Things data.

## Public Runtime Names

- `RemiSdk` is the local runtime entrypoint for SQLite-backed Things state, local notifications, chat runtime integration, and virtual filesystem tools.
- `RemiPublicClient` is the neutral public RPC client used by CRDT sync helpers.

Legacy automation product code has been staged outside this SDK repository for a future standalone repository. New SDK builds do not depend on it, expose it, or create Things binding fields for it.

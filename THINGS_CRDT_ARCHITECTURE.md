# Things CRDT Architecture

The Things CRDT layer stores root, collection, thing metadata, thing markdown, and JSON object entry documents. `ThingsDocumentSet` coordinates document loading, mutation, extraction, materialization, and persistence.

Application code should enter through `RemiSdk`; lower-level CRDT helpers remain crate-internal unless a caller specifically needs domain serialization, schema initialization, or sync tests.

Collection and thing metadata no longer includes automation binding metadata. Materialization produces only collection and thing upserts/deletes plus content updates.

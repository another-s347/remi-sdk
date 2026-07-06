# Things API and Migration Notes

`RemiSdk` is the application-facing entrypoint for local-first Things access. Applications should use the typed Things APIs instead of editing CRDT documents directly.

The current Things upsert models contain collection and thing identity, title, collection type, app id, datatype, content data, parent, status, and timestamps. Legacy automation binding fields are accepted only as ignored unknown JSON when reading old data through serde-compatible paths; new SDK writes do not generate those fields.

Use `RemiPublicClient` with `things_sync::sync_v3_documents_with_server(...)` for server synchronization.

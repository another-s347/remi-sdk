use super::*;

use async_trait::async_trait;
use automerge::ROOT;
use automerge::transaction::Transactable;
use std::path::PathBuf;
use tempfile::Builder;

struct MockSyncTransport {
    list_calls: usize,
    snapshot_calls: usize,
    batch_snapshot_calls: usize,
    batch_sync_calls: usize,
    sync_calls: usize,
    server_keys: Vec<ServerCrdtDocumentKey>,
    snapshots: std::collections::HashMap<(String, i32), (Vec<u8>, String)>,
}

#[async_trait]
impl CrdtSyncTransport for MockSyncTransport {
    async fn sync_crdt_document(
        &mut self,
        _device_id: String,
        _document_uuid: String,
        _data_type: i32,
        _sync_message: Vec<u8>,
    ) -> Result<(Vec<Vec<u8>>, String)> {
        self.sync_calls += 1;
        Ok((Vec::new(), String::new()))
    }

    async fn sync_crdt_documents(
        &mut self,
        _device_id: String,
        documents: Vec<(String, i32, Vec<u8>)>,
    ) -> Result<Vec<(String, i32, Vec<Vec<u8>>, String)>> {
        self.batch_sync_calls += 1;
        self.sync_calls += documents.len();
        Ok(documents
            .into_iter()
            .map(|(document_uuid, data_type, _sync_message)| {
                (document_uuid, data_type, Vec::new(), String::new())
            })
            .collect())
    }

    async fn get_crdt_document_snapshot(
        &mut self,
        _device_id: String,
        document_uuid: String,
        data_type: i32,
        _reset_sync_state: bool,
    ) -> Result<(Vec<u8>, String)> {
        self.snapshot_calls += 1;
        Ok(self
            .snapshots
            .get(&(document_uuid, data_type))
            .cloned()
            .unwrap_or_else(|| (Vec::new(), String::new())))
    }

    async fn get_crdt_document_snapshots(
        &mut self,
        _device_id: String,
        documents: Vec<(String, i32)>,
        _reset_sync_state: bool,
    ) -> Result<Vec<(String, i32, Vec<u8>, String)>> {
        self.batch_snapshot_calls += 1;
        Ok(documents
            .into_iter()
            .map(|(document_uuid, data_type)| {
                let (automerge_doc, last_sync_at) = self
                    .snapshots
                    .get(&(document_uuid.clone(), data_type))
                    .cloned()
                    .unwrap_or_else(|| (Vec::new(), String::new()));
                (document_uuid, data_type, automerge_doc, last_sync_at)
            })
            .collect())
    }

    async fn list_crdt_document_keys(&mut self) -> Result<Vec<ServerCrdtDocumentKey>> {
        self.list_calls += 1;
        Ok(self.server_keys.clone())
    }
}

fn test_sdk() -> TriggerSdk {
    let dir = Builder::new()
        .prefix("remi-things-sync-test-")
        .tempdir()
        .expect("tempdir")
        .keep();
    let db_path: PathBuf = dir.join("sdk.sqlite3");
    TriggerSdk::initialize(&db_path).expect("sdk init")
}

fn mutated_root_doc(device_id: &str) -> Vec<u8> {
    let doc_bytes = remi_things_crdt::Schema::init_root_doc(device_id).expect("init root doc");
    let mut doc = automerge::AutoCommit::load(&doc_bytes).expect("load root doc");
    doc.put(ROOT, "_sync_test_marker", "changed")
        .expect("mutate root doc");
    doc.save()
}

fn advanced_sync_state_for(device_id: &str) -> Vec<u8> {
    let _ = device_id;
    vec![1, 2, 3]
}

fn seed_dirty_root_document(sdk: &TriggerSdk, doc: &[u8], sync_state: Vec<u8>) {
    sdk.crdt_save_document("root", "root", doc, &sync_state, true, None)
        .expect("save dirty root doc");
}

fn seed_clean_root_document(sdk: &TriggerSdk, doc: &[u8], sync_state: Vec<u8>) {
    sdk.crdt_save_document("root", "root", doc, &sync_state, false, None)
        .expect("save clean root doc");
}

fn seed_dirty_collection_document(
    sdk: &TriggerSdk,
    uuid: &str,
    device_id: &str,
    sync_state: Vec<u8>,
) {
    let doc = remi_things_crdt::Schema::init_collection_doc(device_id, uuid)
        .expect("init collection doc");
    sdk.crdt_save_document(uuid, "collection", &doc, &sync_state, true, None)
        .expect("save dirty collection doc");
}

fn test_doc_row(sync_state: Vec<u8>, last_sync_at: Option<&str>) -> crate::types::CrdtDocumentRow {
    crate::types::CrdtDocumentRow {
        uuid: "doc-1".to_string(),
        data_type: "root".to_string(),
        automerge_doc: Vec::new(),
        sync_state,
        dirty: true,
        last_sync_at: last_sync_at.map(str::to_string),
        created_at: 0,
        updated_at: 0,
    }
}

#[test]
fn test_document_key_data_type_str() {
    let root = DocumentKey::root();
    assert_eq!(root.data_type_str(), "root");

    let coll = DocumentKey::collection("coll-1");
    assert_eq!(coll.data_type_str(), "collection");

    let md = DocumentKey::thing_markdown("thing-1");
    assert_eq!(md.data_type_str(), "thing_markdown");
}

#[test]
fn document_head_match_detects_identical_single_head_doc() {
    let doc = remi_things_crdt::Schema::init_root_doc("device-a").unwrap();
    let head = local_canonical_head(&doc).unwrap();

    assert!(document_is_at_server_head(&doc, &head));
}

#[test]
fn document_head_match_requires_non_empty_server_head() {
    let doc = remi_things_crdt::Schema::init_root_doc("device-a").unwrap();

    assert!(!document_is_at_server_head(&doc, &[]));
}

#[test]
fn sync_history_ignores_last_sync_at_when_sync_state_is_initial() {
    let initial = crate::crdt_sync::init_sync_state();

    assert!(!has_sync_history(&test_doc_row(
        Vec::new(),
        Some("2026-03-21T00:00:00Z")
    )));
    assert!(!has_sync_history(&test_doc_row(
        initial,
        Some("2026-03-21T00:00:00Z")
    )));
}

#[test]
fn sync_history_detects_advanced_sync_state_without_last_sync_at() {
    let advanced_state = advanced_sync_state_for("device-a");

    assert!(has_sync_history(&test_doc_row(advanced_state, None)));
}

#[test]
fn never_synced_dirty_keys_follows_sync_state_history() {
    let advanced_state = advanced_sync_state_for("device-a");

    let unsynced = crate::types::CrdtDocumentRow {
        uuid: "unsynced".to_string(),
        ..test_doc_row(
            crate::crdt_sync::init_sync_state(),
            Some("2026-03-21T00:00:00Z"),
        )
    };
    let synced = crate::types::CrdtDocumentRow {
        uuid: "synced".to_string(),
        ..test_doc_row(advanced_state, None)
    };

    let keys = never_synced_dirty_keys(&[unsynced, synced]);
    assert_eq!(keys, vec![("unsynced".to_string(), "root".to_string())]);
}

#[tokio::test]
async fn incremental_mode_uses_server_key_discovery_after_dirty_push() {
    let sdk = test_sdk();
    let device_id = "device-a";
    let advanced_state = advanced_sync_state_for(device_id);
    let synced_doc = mutated_root_doc(device_id);
    seed_dirty_root_document(&sdk, &synced_doc, advanced_state);

    let mut transport = MockSyncTransport {
        list_calls: 0,
        snapshot_calls: 0,
        batch_snapshot_calls: 0,
        batch_sync_calls: 0,
        sync_calls: 0,
        server_keys: Vec::new(),
        snapshots: std::collections::HashMap::new(),
    };

    let output = sync_v3_documents_with_transport_mode(
        &sdk,
        &mut transport,
        device_id,
        ThingsSyncMode::Incremental,
    )
    .await
    .expect("incremental sync succeeds");

    assert_eq!(transport.list_calls, 1);
    assert_eq!(transport.snapshot_calls, 0);
    assert_eq!(transport.batch_snapshot_calls, 0);
    assert_eq!(transport.sync_calls, 1);
    assert_eq!(transport.batch_sync_calls, 1);
    assert_eq!(output.documents_synced, 1);
    let summary = output.summary();
    assert_eq!(summary.documents_synced, output.documents_synced);
    assert_eq!(summary.documents_pushed, output.documents_pushed);
    assert_eq!(summary.documents_pulled, output.documents_pulled);
    assert_eq!(summary.generated_event_range, output.generated_event_range);
}

#[tokio::test]
async fn incremental_mode_upgrades_to_full_when_bootstrap_discovery_is_required() {
    let sdk = test_sdk();
    seed_dirty_root_document(
        &sdk,
        &mutated_root_doc("device-a"),
        crate::crdt_sync::init_sync_state(),
    );

    let mut transport = MockSyncTransport {
        list_calls: 0,
        snapshot_calls: 0,
        batch_snapshot_calls: 0,
        batch_sync_calls: 0,
        sync_calls: 0,
        server_keys: Vec::new(),
        snapshots: std::collections::HashMap::new(),
    };

    let output = sync_v3_documents_with_transport_mode(
        &sdk,
        &mut transport,
        "device-a",
        ThingsSyncMode::Incremental,
    )
    .await
    .expect("bootstrap sync succeeds");

    assert_eq!(transport.list_calls, 1);
    assert!(transport.sync_calls >= 1);
    assert!(transport.batch_sync_calls >= 1);
    assert!(output.documents_synced >= 1);
}

#[tokio::test]
async fn incremental_mode_receives_existing_clean_doc_when_server_head_differs() {
    let sdk = test_sdk();
    let device_id = "device-a";
    let local_doc = remi_things_crdt::Schema::init_root_doc(device_id).expect("init root doc");
    seed_clean_root_document(&sdk, &local_doc, advanced_sync_state_for(device_id));

    let server_doc = mutated_root_doc("device-b");
    let server_head = local_canonical_head(&server_doc).expect("server head");

    let mut transport = MockSyncTransport {
        list_calls: 0,
        snapshot_calls: 0,
        batch_snapshot_calls: 0,
        batch_sync_calls: 0,
        sync_calls: 0,
        server_keys: vec![ServerCrdtDocumentKey {
            document_uuid: "root".to_string(),
            data_type: 1,
            canonical_head: server_head,
        }],
        snapshots: std::collections::HashMap::new(),
    };

    let output = sync_v3_documents_with_transport_mode(
        &sdk,
        &mut transport,
        device_id,
        ThingsSyncMode::Incremental,
    )
    .await
    .expect("incremental receive succeeds");

    assert_eq!(transport.list_calls, 1);
    assert_eq!(transport.snapshot_calls, 0);
    assert_eq!(transport.batch_snapshot_calls, 0);
    assert_eq!(transport.sync_calls, 1);
    assert_eq!(transport.batch_sync_calls, 1);
    assert_eq!(output.documents_pushed, 0);
    assert_eq!(output.documents_pulled, 1);
    assert_eq!(output.metrics.phase1b_documents_synced, 1);
}

#[tokio::test]
async fn pull_missing_documents_saves_snapshots_without_followup_sync_roundtrips() {
    let sdk = test_sdk();
    let device_id = "device-a";
    let root_doc = remi_things_crdt::Schema::init_root_doc("server-device").expect("init root doc");
    let root_head = local_canonical_head(&root_doc).expect("root head");

    let mut transport = MockSyncTransport {
        list_calls: 0,
        snapshot_calls: 0,
        batch_snapshot_calls: 0,
        batch_sync_calls: 0,
        sync_calls: 0,
        server_keys: vec![ServerCrdtDocumentKey {
            document_uuid: "root".to_string(),
            data_type: 1,
            canonical_head: root_head,
        }],
        snapshots: std::collections::HashMap::from([(
            ("root".to_string(), 1),
            (root_doc.clone(), "2026-03-25T00:00:00Z".to_string()),
        )]),
    };

    let prefetched_keys = transport.server_keys.clone();

    let output = pull_missing_documents(
        &sdk,
        &mut transport,
        device_id,
        "test-sync-run",
        Some(&prefetched_keys),
        None,
    )
    .await
    .expect("pull_missing_documents succeeds");

    assert_eq!(transport.list_calls, 0);
    assert_eq!(transport.snapshot_calls, 0);
    assert_eq!(transport.batch_snapshot_calls, 1);
    assert_eq!(transport.sync_calls, 0);
    assert_eq!(output.documents_pulled, 1);
    assert_eq!(output.last_sync_at.as_deref(), Some("2026-03-25T00:00:00Z"));
    assert_eq!(output.snapshot_downloads, 1);

    let saved = sdk
        .crdt_get_document("root", "root")
        .expect("load root")
        .expect("root exists after pull");
    assert_eq!(saved.automerge_doc, root_doc);
    assert!(!saved.dirty);
}

#[tokio::test]
async fn phase1_batches_same_priority_documents_into_one_transport_call() {
    let sdk = test_sdk();
    let device_id = "device-a";
    seed_dirty_collection_document(
        &sdk,
        "collection-a",
        device_id,
        advanced_sync_state_for(device_id),
    );
    seed_dirty_collection_document(
        &sdk,
        "collection-b",
        device_id,
        advanced_sync_state_for(device_id),
    );

    let mut transport = MockSyncTransport {
        list_calls: 0,
        snapshot_calls: 0,
        batch_snapshot_calls: 0,
        batch_sync_calls: 0,
        sync_calls: 0,
        server_keys: Vec::new(),
        snapshots: std::collections::HashMap::new(),
    };

    let output = sync_v3_documents_with_transport_mode(
        &sdk,
        &mut transport,
        device_id,
        ThingsSyncMode::Full,
    )
    .await
    .expect("batched phase1 sync succeeds");

    assert_eq!(transport.batch_sync_calls, 1);
    assert_eq!(transport.sync_calls, 2);
    assert_eq!(output.documents_synced, 2);
}

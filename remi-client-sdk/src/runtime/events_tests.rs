use anyhow::Result;

use super::TriggerSdk;
use crate::things_crdt::{DocumentKey, DocumentState, ThingCollectionUpsert, ThingsDocumentSet};
use crate::things_events::{ThingsDocumentKind, ThingsEvent};

#[test]
fn things_watch_since_recovers_events_after_reopen() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("events.sqlite");
    let db_path = db_path.to_string_lossy().to_string();
    let device_id = "device-events";

    let sdk = TriggerSdk::initialize(&db_path)?;
    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "collection-events".to_string(),
            title: "Events".to_string(),
            collection_type: Default::default(),
            app_id: None,
            trigger_uuid: None,
            trigger_uuid_patch: Default::default(),
            created_at: None,
            updated_at: None,
        },
    )?;

    let events = sdk.things_watch_since(device_id, 0, 100)?;
    assert!(!events.is_empty());
    let collection_last_event_id = events.last().unwrap().event_id;
    assert!(
        events
            .iter()
            .any(|event| event.entity_type == "collection"
                && event.entity_uuid == "collection-events")
    );

    sdk.emit_snapshot_replace(device_id)?;
    let snapshot_events = sdk.things_watch_since(device_id, collection_last_event_id, 100)?;
    assert!(
        snapshot_events.iter().any(
            |event| event.entity_type == "snapshot" && event.change_kind == "snapshot_replaced"
        )
    );
    let last_event_id = snapshot_events.last().unwrap().event_id;

    drop(sdk);
    let reopened = TriggerSdk::initialize(&db_path)?;
    assert!(
        reopened
            .things_watch_since(device_id, last_event_id, 100)?
            .is_empty()
    );

    Ok(())
}

#[test]
fn wipe_all_data_records_durable_data_wiped_event_for_device() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("data-wiped.sqlite");
    let db_path = db_path.to_string_lossy().to_string();
    let device_id = "device-data-wiped";

    let sdk = TriggerSdk::initialize(&db_path)?;
    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "before-wipe".to_string(),
            title: "Before wipe".to_string(),
            collection_type: Default::default(),
            app_id: None,
            trigger_uuid: None,
            trigger_uuid_patch: Default::default(),
            created_at: None,
            updated_at: None,
        },
    )?;

    sdk.wipe_all_data_and_notify_for_device(device_id)?;
    let events = sdk.things_watch_since(device_id, 0, 100)?;
    let data_wiped = events
        .iter()
        .find(|event| event.entity_type == "data" && event.change_kind == "data_wiped")
        .expect("expected durable data_wiped event");
    assert_eq!(data_wiped.entity_uuid, "all");
    assert!(matches!(
        serde_json::from_str::<ThingsEvent>(&data_wiped.payload_json)?,
        ThingsEvent::DataWiped
    ));

    Ok(())
}

#[test]
fn remote_apply_does_not_clear_existing_local_dirty_documents() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("remote-apply.sqlite");
    let db_path = db_path.to_string_lossy().to_string();
    let device_id = "device-remote-apply";

    let sdk = TriggerSdk::initialize(&db_path)?;
    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "local-dirty".to_string(),
            title: "Local Dirty".to_string(),
            collection_type: Default::default(),
            app_id: None,
            trigger_uuid: None,
            trigger_uuid_patch: Default::default(),
            created_at: None,
            updated_at: None,
        },
    )?;

    let dirty_before = sdk.crdt_get_dirty_documents()?;
    assert!(dirty_before.iter().any(|row| row.uuid == "local-dirty"));

    let remote_doc =
        remi_things_crdt::Schema::init_thing_markdown_doc("remote-device", "remote-thing")?;
    sdk.things_apply_remote_document(
        device_id,
        "sync-run-test",
        DocumentKey::thing_markdown("remote-thing"),
        DocumentState {
            automerge_doc: remote_doc,
            sync_state: Vec::new(),
            dirty: false,
            last_sync_at: Some("2026-06-28T00:00:00Z".to_string()),
        },
    )?;

    let dirty_after = sdk.crdt_get_dirty_documents()?;
    assert!(dirty_after.iter().any(|row| row.uuid == "local-dirty"));
    assert!(!dirty_after.iter().any(|row| row.uuid == "remote-thing"));

    Ok(())
}

#[test]
fn local_mutation_broadcasts_from_pipeline() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("local-broadcast.sqlite");
    let db_path = db_path.to_string_lossy().to_string();
    let device_id = "device-local-broadcast";

    let sdk = TriggerSdk::initialize(&db_path)?;
    let mut rx = sdk.things_subscribe();
    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "broadcast-collection".to_string(),
            title: "Broadcast".to_string(),
            collection_type: Default::default(),
            app_id: None,
            trigger_uuid: None,
            trigger_uuid_patch: Default::default(),
            created_at: None,
            updated_at: None,
        },
    )?;

    let mut received = 0;
    let mut saw_collection = false;
    while let Ok(event) = rx.try_recv() {
        received += 1;
        if let ThingsEvent::DocumentChanged {
            device_id: got_device_id,
            document,
        } = event
        {
            assert_eq!(got_device_id, device_id);
            if document.document_kind == ThingsDocumentKind::Collection
                && document.document_uuid == "broadcast-collection"
            {
                saw_collection = true;
            }
        }
    }
    assert!(received > 0, "expected at least one broadcast event");
    assert!(saw_collection, "expected collection broadcast event");

    Ok(())
}

#[test]
fn remote_apply_broadcasts_from_pipeline() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("remote-broadcast.sqlite");
    let db_path = db_path.to_string_lossy().to_string();
    let device_id = "device-remote-broadcast";

    let sdk = TriggerSdk::initialize(&db_path)?;
    let mut rx = sdk.things_subscribe();
    let remote_doc =
        remi_things_crdt::Schema::init_thing_markdown_doc("remote-device", "remote-thing")?;
    sdk.things_apply_remote_document(
        device_id,
        "sync-run-broadcast",
        DocumentKey::thing_markdown("remote-thing"),
        DocumentState {
            automerge_doc: remote_doc,
            sync_state: Vec::new(),
            dirty: false,
            last_sync_at: Some("2026-06-28T00:00:00Z".to_string()),
        },
    )?;

    let event = rx.try_recv()?;
    match event {
        ThingsEvent::DocumentChanged {
            device_id: got_device_id,
            document,
        } => {
            assert_eq!(got_device_id, device_id);
            assert_eq!(document.document_kind, ThingsDocumentKind::ThingMarkdown);
            assert_eq!(document.document_uuid, "remote-thing");
        }
        other => panic!("expected document change event, got {other:?}"),
    }

    Ok(())
}

#[test]
fn remote_apply_collection_doc_emits_created_event_from_snapshot_diff() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("remote-created-event.sqlite");
    let db_path = db_path.to_string_lossy().to_string();
    let device_id = "device-remote-created-event";
    let collection_uuid = "remote-created-collection";

    let mut remote_doc_set = ThingsDocumentSet::new("remote-device");
    remote_doc_set.get_or_init_collection(collection_uuid)?;
    remote_doc_set.update_collection_meta(
        collection_uuid,
        Some("Remote Created".to_string()),
        None,
        remi_things_crdt::TriggerUpdate::Noop,
    )?;
    let remote_collection_state = remote_doc_set
        .get(&DocumentKey::collection(collection_uuid))
        .expect("remote collection doc should exist")
        .clone();

    let sdk = TriggerSdk::initialize(&db_path)?;
    sdk.things_apply_remote_document(
        device_id,
        "sync-run-created",
        DocumentKey::collection(collection_uuid),
        DocumentState {
            automerge_doc: remote_collection_state.automerge_doc,
            sync_state: Vec::new(),
            dirty: false,
            last_sync_at: Some("2026-06-28T00:00:00Z".to_string()),
        },
    )?;

    let events = sdk.things_watch_since(device_id, 0, 100)?;
    assert!(events.iter().any(|event| {
        event.entity_type == "collection"
            && event.entity_uuid == collection_uuid
            && event.change_kind == "created"
    }));
    assert!(
        !sdk.crdt_get_dirty_documents()?
            .iter()
            .any(|row| row.uuid == collection_uuid)
    );

    Ok(())
}

#[test]
fn remote_apply_documents_batch_emits_events_and_marks_clean() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("remote-batch-event.sqlite");
    let db_path = db_path.to_string_lossy().to_string();
    let device_id = "device-remote-batch-event";
    let collection_uuid = "remote-batch-collection";

    let mut remote_doc_set = ThingsDocumentSet::new("remote-device");
    remote_doc_set.get_or_init_collection(collection_uuid)?;
    remote_doc_set.update_collection_meta(
        collection_uuid,
        Some("Remote Batch".to_string()),
        None,
        remi_things_crdt::TriggerUpdate::Noop,
    )?;
    let remote_root_state = remote_doc_set
        .get(&DocumentKey::root())
        .expect("remote root doc should exist")
        .clone();
    let remote_collection_state = remote_doc_set
        .get(&DocumentKey::collection(collection_uuid))
        .expect("remote collection doc should exist")
        .clone();

    let sdk = TriggerSdk::initialize(&db_path)?;
    let event_range = sdk.things_apply_remote_documents(
        device_id,
        "sync-run-batch",
        vec![
            (
                DocumentKey::root(),
                DocumentState {
                    automerge_doc: remote_root_state.automerge_doc,
                    sync_state: Vec::new(),
                    dirty: false,
                    last_sync_at: Some("2026-06-28T00:00:00Z".to_string()),
                },
            ),
            (
                DocumentKey::collection(collection_uuid),
                DocumentState {
                    automerge_doc: remote_collection_state.automerge_doc,
                    sync_state: Vec::new(),
                    dirty: false,
                    last_sync_at: Some("2026-06-28T00:00:00Z".to_string()),
                },
            ),
        ],
    )?;

    assert!(event_range.is_some());
    let events = sdk.things_watch_since(device_id, 0, 100)?;
    assert!(events.iter().any(|event| {
        event.entity_type == "collection"
            && event.entity_uuid == collection_uuid
            && event.change_kind == "created"
    }));
    assert!(sdk.crdt_get_dirty_documents()?.is_empty());

    Ok(())
}

#[test]
fn save_synced_clean_doc_emits_update_event_when_canonical_doc_changes_snapshot() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("synced-clean-event.sqlite");
    let db_path = db_path.to_string_lossy().to_string();
    let device_id = "device-synced-clean-event";
    let collection_uuid = "synced-clean-collection";

    let sdk = TriggerSdk::initialize(&db_path)?;
    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: collection_uuid.to_string(),
            title: "Local Title".to_string(),
            collection_type: Default::default(),
            app_id: None,
            trigger_uuid: None,
            trigger_uuid_patch: Default::default(),
            created_at: None,
            updated_at: None,
        },
    )?;
    let last_local_event_id = sdk
        .things_watch_since(device_id, 0, 100)?
        .last()
        .map(|event| event.event_id)
        .unwrap_or(0);
    assert!(
        sdk.crdt_get_dirty_documents()?
            .iter()
            .any(|row| row.uuid == collection_uuid)
    );

    let mut canonical_doc_set = ThingsDocumentSet::new("remote-device");
    canonical_doc_set.get_or_init_collection(collection_uuid)?;
    canonical_doc_set.update_collection_meta(
        collection_uuid,
        Some("Server Title".to_string()),
        None,
        remi_things_crdt::TriggerUpdate::Noop,
    )?;
    let canonical_collection_state = canonical_doc_set
        .get(&DocumentKey::collection(collection_uuid))
        .expect("canonical collection doc should exist")
        .clone();

    let event_range = sdk.things_save_synced_document_clean(
        device_id,
        "sync-run-clean",
        DocumentKey::collection(collection_uuid),
        canonical_collection_state.automerge_doc,
        Vec::new(),
        Some("2026-06-28T00:00:00Z"),
    )?;

    assert!(event_range.is_some());
    let events = sdk.things_watch_since(device_id, last_local_event_id, 100)?;
    assert!(events.iter().any(|event| {
        event.entity_type == "collection"
            && event.entity_uuid == collection_uuid
            && event.change_kind == "updated"
    }));
    assert!(
        !sdk.crdt_get_dirty_documents()?
            .iter()
            .any(|row| row.uuid == collection_uuid)
    );

    Ok(())
}

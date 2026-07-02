use super::*;

fn collection_upsert(uuid: &str) -> ThingCollectionUpsert {
    ThingCollectionUpsert {
        uuid: uuid.to_string(),
        title: "Inbox".to_string(),
        collection_type: Default::default(),
        app_id: None,
        trigger_uuid: None,
        trigger_uuid_patch: Default::default(),
        created_at: None,
        updated_at: None,
    }
}

fn thing_upsert(uuid: &str, collection_uuid: &str) -> ThingUpsert {
    ThingUpsert {
        uuid: uuid.to_string(),
        title: "Task".to_string(),
        datatype: crate::things_crdt::ThingDatatype::Markdown,
        data: Some(json!({ "markdown": "hello" })),
        collection_uuid: collection_uuid.to_string(),
        trigger_uuid: None,
        trigger_uuid_patch: Default::default(),
        parent_uuid: None,
        created_at: None,
        updated_at: None,
    }
}

#[test]
fn snapshot_projects_system_collections_without_persisting_read_side_writes() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);

    let snapshot = service.snapshot_lite("device-system-snapshot")?;

    assert!(!snapshot.dirty);
    assert!(snapshot.collections.iter().any(|collection| {
        collection.uuid == SYSTEM_DEFAULT_COLLECTION_ID
            && collection.collection_type == CollectionType::Default
            && collection.title == "Drafts"
    }));
    assert!(snapshot.collections.iter().any(|collection| {
        collection.uuid == SYSTEM_TRASH_COLLECTION_ID
            && collection.collection_type == CollectionType::Trash
            && collection.title == "Trash"
    }));
    assert!(storage.list_crdt_documents()?.is_empty());
    assert!(storage.list_things_change_log(10, 0)?.is_empty());
    Ok(())
}

#[test]
fn upsert_thing_without_collection_routes_to_system_default() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);

    let result = service.upsert_thing("device-default-route", thing_upsert("task-1", ""))?;

    assert_eq!(result.value.collection_uuid, SYSTEM_DEFAULT_COLLECTION_ID);
    let snapshot = service.snapshot_lite("device-default-route")?;
    assert!(snapshot.collections.iter().any(|collection| {
        collection.uuid == SYSTEM_DEFAULT_COLLECTION_ID
            && collection.collection_type == CollectionType::Default
    }));
    assert!(snapshot.collections.iter().any(|collection| {
        collection.uuid == SYSTEM_TRASH_COLLECTION_ID
            && collection.collection_type == CollectionType::Trash
    }));
    Ok(())
}

#[test]
fn system_collections_cannot_be_archived() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_thing("device-system-protect", thing_upsert("task-1", ""))?;

    let default_result =
        service.delete_collection("device-system-protect", SYSTEM_DEFAULT_COLLECTION_ID)?;
    let trash_result =
        service.delete_collection("device-system-protect", SYSTEM_TRASH_COLLECTION_ID)?;

    assert!(!default_result.value.deleted);
    assert!(!trash_result.value.deleted);
    let snapshot = service.snapshot_lite("device-system-protect")?;
    assert!(
        snapshot
            .collections
            .iter()
            .find(|collection| collection.uuid == SYSTEM_DEFAULT_COLLECTION_ID)
            .and_then(|collection| collection.archived_at.as_ref())
            .is_none()
    );
    assert!(
        snapshot
            .collections
            .iter()
            .find(|collection| collection.uuid == SYSTEM_TRASH_COLLECTION_ID)
            .and_then(|collection| collection.archived_at.as_ref())
            .is_none()
    );
    Ok(())
}

#[test]
fn delete_thing_archives_to_trash_and_restore_moves_back() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-archive-restore", collection_upsert("inbox"))?;
    service.upsert_thing("device-archive-restore", thing_upsert("task-1", "inbox"))?;

    service.delete_thing("device-archive-restore", "inbox", "task-1")?;
    let archived = service
        .get_thing("device-archive-restore", "task-1", false)?
        .expect("archived thing is visible");
    assert_eq!(archived.collection_uuid, SYSTEM_TRASH_COLLECTION_ID);
    assert_eq!(
        archived.archived_from_collection_uuid.as_deref(),
        Some("inbox")
    );
    assert!(archived.archived_at.is_some());

    let restored = service
        .restore_thing("device-archive-restore", "task-1", None, None)?
        .value
        .expect("restored thing");
    assert_eq!(restored.collection_uuid, "inbox");
    assert!(restored.archived_at.is_none());
    assert!(restored.archived_from_collection_uuid.is_none());
    Ok(())
}

#[test]
fn delete_collection_archives_metadata_and_restore_clears_it() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-collection-archive", collection_upsert("inbox"))?;

    service.delete_collection("device-collection-archive", "inbox")?;
    let archived = service
        .get_collection("device-collection-archive", "inbox")?
        .expect("archived collection remains visible");
    assert!(archived.archived_at.is_some());

    let restored = service
        .restore_collection("device-collection-archive", "inbox")?
        .value
        .expect("restored collection");
    assert!(restored.archived_at.is_none());
    assert_eq!(restored.collection_type, CollectionType::Normal);
    Ok(())
}

#[test]
fn app_collection_is_stable_per_app_id() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);

    let first = service
        .ensure_app_collection(
            "device-app-collection",
            "com.example.share",
            Some("Share".into()),
        )?
        .value;
    let second = service
        .ensure_app_collection("device-app-collection", "com.example.share", None)?
        .value;

    assert_eq!(first.uuid, second.uuid);
    assert_eq!(first.collection_type, CollectionType::App);
    assert_eq!(first.app_id.as_deref(), Some("com.example.share"));
    Ok(())
}

#[test]
fn collection_upsert_records_user_visible_change_log_by_default() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);

    let result = service.upsert_collection("device-changelog", collection_upsert("inbox"))?;

    assert_eq!(result.change_log_ids.len(), 1);
    let logs = storage.list_things_change_log(10, 0)?;
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].entity_type, "collection");
    assert_eq!(logs[0].entity_uuid, "inbox");
    Ok(())
}

#[test]
fn collection_upsert_honors_suppress_change_log_policy() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let mut context = ThingsMutationContext::local_command("device-changelog-suppress");
    context.change_log_policy = ChangeLogPolicy::Suppress;

    let result = service.upsert_collection_with_context(context, collection_upsert("inbox"))?;

    assert!(result.change_log_ids.is_empty());
    assert!(result.event_range.is_some());
    assert!(storage.list_things_change_log(10, 0)?.is_empty());
    assert!(!storage.get_dirty_crdt_documents()?.is_empty());
    Ok(())
}

#[test]
fn collection_card_jsx_round_trips_through_local_service() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-card-jsx", collection_upsert("inbox"))?;

    assert_eq!(
        service.get_collection_card_jsx("device-card-jsx", "inbox")?,
        None
    );

    service.set_collection_card_jsx(
        "device-card-jsx",
        "inbox",
        Some("  export default function Card() { return null; }  "),
    )?;
    assert_eq!(
        service.get_collection_card_jsx("device-card-jsx", "inbox")?,
        Some("export default function Card() { return null; }".to_string())
    );

    service.set_collection_card_jsx("device-card-jsx", "inbox", Some("   "))?;
    assert_eq!(
        service.get_collection_card_jsx("device-card-jsx", "inbox")?,
        None
    );
    Ok(())
}

#[test]
fn change_log_sync_state_flows_through_local_service() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-log-sync", collection_upsert("inbox"))?;
    service.upsert_thing("device-log-sync", thing_upsert("task-1", "inbox"))?;
    service.delete_thing("device-log-sync", "inbox", "task-1")?;

    let logs = service.get_unsynced_change_logs(10)?;
    assert!(!logs.is_empty());
    let log_ids: Vec<i64> = logs.iter().map(|entry| entry.id).collect();
    service.mark_change_logs_synced(&log_ids)?;
    assert!(service.get_unsynced_change_logs(10)?.is_empty());

    let snapshots = service.get_unsynced_content_snapshots(10)?;
    assert_eq!(snapshots.len(), 1);
    let snapshot_ids: Vec<i64> = snapshots.iter().map(|snapshot| snapshot.id).collect();
    service.mark_content_snapshots_synced(&snapshot_ids)?;
    assert!(service.get_unsynced_content_snapshots(10)?.is_empty());

    service.insert_synced_change_log(
        "device-log-sync",
        ThingsOperationType::SyncApplied,
        "sync",
        "remote-run",
        "Remote sync applied",
        "{}",
        Utc::now().timestamp_millis(),
    )?;
    service.insert_synced_content_snapshot(
        "device-log-sync",
        "remote-thing",
        r#"{"uuid":"remote-thing"}"#,
        Utc::now().timestamp_millis(),
    )?;

    assert!(service.get_unsynced_change_logs(10)?.is_empty());
    assert!(service.get_unsynced_content_snapshots(10)?.is_empty());
    Ok(())
}

#[test]
fn remote_apply_record_sync_summary_writes_synced_technical_change_log() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let mut context = ThingsMutationContext::remote_sync("device-sync-summary", "sync-run-1");
    context.change_log_policy = ChangeLogPolicy::RecordSyncSummary;

    let doc = remi_things_crdt::Schema::init_root_doc("remote-device")?;
    let result = service.pipeline().apply_remote_document(
        &context,
        DocumentKey::root(),
        DocumentState {
            automerge_doc: doc,
            sync_state: Vec::new(),
            dirty: false,
            last_sync_at: None,
        },
    )?;

    assert_eq!(result.change_log_ids.len(), 1);
    assert!(result.event_range.is_some());
    let logs = storage.list_things_change_log(10, 0)?;
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].op_type, ThingsOperationType::SyncApplied);
    assert_eq!(logs[0].entity_type, "sync");
    assert_eq!(logs[0].entity_uuid, "sync-run-1");
    assert!(!logs[0].can_undo);
    assert!(storage.get_unsynced_change_logs(10)?.is_empty());
    Ok(())
}

#[test]
fn remote_apply_merges_system_collection_instead_of_replacing() -> Result<()> {
    let temp_a = tempfile::tempdir()?;
    let temp_b = tempfile::tempdir()?;
    let storage_a = Storage::new(temp_a.path().join("things-a.sqlite"))?;
    let storage_b = Storage::new(temp_b.path().join("things-b.sqlite"))?;
    let service_a = ThingsLocalService::new(&storage_a);
    let service_b = ThingsLocalService::new(&storage_b);

    service_a.upsert_thing("device-a", thing_upsert("task-a", ""))?;
    service_b.upsert_thing("device-b", thing_upsert("task-b", ""))?;

    let remote_root = storage_b
        .get_crdt_document(remi_things_crdt::ROOT_DOC_UUID, "root")?
        .expect("remote root document");
    let remote_default = storage_b
        .get_crdt_document(SYSTEM_DEFAULT_COLLECTION_ID, "collection")?
        .expect("remote default collection document");
    let context = ThingsMutationContext::remote_sync("device-a", "sync-merge-default");

    service_a.pipeline().apply_remote_documents(
        &context,
        vec![
            (
                DocumentKey::root(),
                DocumentState {
                    automerge_doc: remote_root.automerge_doc,
                    sync_state: remote_root.sync_state,
                    dirty: false,
                    last_sync_at: remote_root.last_sync_at,
                },
            ),
            (
                DocumentKey::collection(SYSTEM_DEFAULT_COLLECTION_ID),
                DocumentState {
                    automerge_doc: remote_default.automerge_doc,
                    sync_state: remote_default.sync_state,
                    dirty: false,
                    last_sync_at: remote_default.last_sync_at,
                },
            ),
        ],
    )?;

    let snapshot = service_a.snapshot_lite("device-a")?;
    let default_tasks = snapshot
        .things
        .iter()
        .filter(|thing| thing.collection_uuid == SYSTEM_DEFAULT_COLLECTION_ID)
        .map(|thing| thing.uuid.as_str())
        .collect::<Vec<_>>();

    assert!(default_tasks.contains(&"task-a"));
    assert!(default_tasks.contains(&"task-b"));
    assert!(
        storage_a
            .get_crdt_document(SYSTEM_DEFAULT_COLLECTION_ID, "collection")?
            .expect("local default collection document")
            .dirty
    );
    Ok(())
}

#[test]
fn remote_apply_merges_normal_collection_document_instead_of_replacing() -> Result<()> {
    let temp_a = tempfile::tempdir()?;
    let temp_b = tempfile::tempdir()?;
    let storage_a = Storage::new(temp_a.path().join("things-a.sqlite"))?;
    let storage_b = Storage::new(temp_b.path().join("things-b.sqlite"))?;
    let service_a = ThingsLocalService::new(&storage_a);
    let service_b = ThingsLocalService::new(&storage_b);

    service_a.upsert_collection("device-a", collection_upsert("shared"))?;
    service_b.upsert_collection("device-b", collection_upsert("shared"))?;
    service_a.upsert_thing("device-a", thing_upsert("task-a", "shared"))?;
    service_b.upsert_thing("device-b", thing_upsert("task-b", "shared"))?;

    let remote_root = storage_b
        .get_crdt_document(remi_things_crdt::ROOT_DOC_UUID, "root")?
        .expect("remote root document");
    let remote_collection = storage_b
        .get_crdt_document("shared", "collection")?
        .expect("remote shared collection document");
    let context = ThingsMutationContext::remote_sync("device-a", "sync-merge-normal");

    service_a.pipeline().apply_remote_documents(
        &context,
        vec![
            (
                DocumentKey::root(),
                DocumentState {
                    automerge_doc: remote_root.automerge_doc,
                    sync_state: remote_root.sync_state,
                    dirty: false,
                    last_sync_at: remote_root.last_sync_at,
                },
            ),
            (
                DocumentKey::collection("shared"),
                DocumentState {
                    automerge_doc: remote_collection.automerge_doc,
                    sync_state: remote_collection.sync_state,
                    dirty: false,
                    last_sync_at: remote_collection.last_sync_at,
                },
            ),
        ],
    )?;

    let snapshot = service_a.snapshot_lite("device-a")?;
    let shared_tasks = snapshot
        .things
        .iter()
        .filter(|thing| thing.collection_uuid == "shared")
        .map(|thing| thing.uuid.as_str())
        .collect::<Vec<_>>();

    assert!(shared_tasks.contains(&"task-a"));
    assert!(shared_tasks.contains(&"task-b"));
    assert!(
        storage_a
            .get_crdt_document("shared", "collection")?
            .expect("local shared collection document")
            .dirty
    );
    Ok(())
}

#[test]
fn bootstrap_snapshot_replay_preserves_collection_and_thing_metadata() -> Result<()> {
    let stash_temp = tempfile::tempdir()?;
    let current_temp = tempfile::tempdir()?;
    let stash_storage = Storage::new(stash_temp.path().join("stash.sqlite"))?;
    let current_storage = Storage::new(current_temp.path().join("current.sqlite"))?;
    let stash_service = ThingsLocalService::new(&stash_storage);

    let app_uuid = ThingsLocalService::app_collection_uuid("com.example.share")?;
    stash_service.ensure_app_collection(
        "device-stash",
        "com.example.share",
        Some("Shared from App".to_string()),
    )?;
    stash_service.delete_collection("device-stash", &app_uuid)?;
    stash_service.upsert_collection("device-stash", collection_upsert("inbox"))?;
    stash_service.upsert_thing("device-stash", thing_upsert("task-archived", "inbox"))?;
    stash_service.set_status("device-stash", "task-archived", "done", None)?;
    stash_service.delete_thing("device-stash", "inbox", "task-archived")?;
    let stashed_state = stash_service.snapshot_lite("device-stash")?;
    let stashed_snapshot = ThingsSnapshot {
        collections: stashed_state.collections,
        things: stashed_state.things,
    };

    let mut doc_set =
        DocumentPersistence::new(&current_storage).load_or_init_document_set("device-current")?;
    replay_missing_snapshot_into_document_set(&mut doc_set, &stashed_snapshot)?;
    let replayed = doc_set.extract_snapshot()?;

    let replayed_app = replayed
        .collections
        .iter()
        .find(|collection| collection.uuid == app_uuid)
        .expect("app collection replayed");
    assert_eq!(replayed_app.collection_type, CollectionType::App);
    assert_eq!(replayed_app.app_id.as_deref(), Some("com.example.share"));
    assert!(replayed_app.archived_at.is_some());

    let replayed_thing = replayed
        .things
        .iter()
        .find(|thing| thing.uuid == "task-archived")
        .expect("archived thing replayed");
    assert_eq!(replayed_thing.status, ThingStatus::Done.as_str());
    assert_eq!(replayed_thing.collection_uuid, SYSTEM_TRASH_COLLECTION_ID);
    assert_eq!(
        replayed_thing.archived_from_collection_uuid.as_deref(),
        Some("inbox")
    );
    assert!(replayed_thing.archived_at.is_some());
    Ok(())
}

#[test]
fn thing_upsert_records_user_visible_change_log_by_default() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-thing-changelog", collection_upsert("inbox"))?;

    let result = service.upsert_thing("device-thing-changelog", thing_upsert("task-1", "inbox"))?;

    assert_eq!(result.change_log_ids.len(), 1);
    let logs = storage.list_things_change_log(10, 0)?;
    assert_eq!(logs.len(), 2);
    assert!(
        logs.iter()
            .any(|log| log.entity_type == "thing" && log.entity_uuid == "task-1")
    );
    Ok(())
}

#[test]
fn thing_upsert_honors_suppress_change_log_policy() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let mut context = ThingsMutationContext::local_command("device-thing-suppress");
    context.change_log_policy = ChangeLogPolicy::Suppress;
    service.upsert_collection_with_context(context.clone(), collection_upsert("inbox"))?;

    let result = service.upsert_thing_with_context(context, thing_upsert("task-1", "inbox"))?;

    assert!(result.change_log_ids.is_empty());
    assert!(result.event_range.is_some());
    assert!(storage.list_things_change_log(10, 0)?.is_empty());
    assert!(!storage.get_dirty_crdt_documents()?.is_empty());
    Ok(())
}

#[test]
fn status_update_records_user_visible_change_log_by_default() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-status-changelog", collection_upsert("inbox"))?;
    service.upsert_thing("device-status-changelog", thing_upsert("task-1", "inbox"))?;

    let result = service.set_thing_status("device-status-changelog", "task-1", "done")?;

    assert_eq!(result.change_log_ids.len(), 1);
    let logs = storage.list_things_change_log(10, 0)?;
    assert!(logs.iter().any(|log| {
        log.entity_type == "thing"
            && log.entity_uuid == "task-1"
            && log.summary.contains("Set thing status")
    }));
    Ok(())
}

#[test]
fn status_update_honors_suppress_change_log_policy() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let mut context = ThingsMutationContext::local_command("device-status-suppress");
    context.change_log_policy = ChangeLogPolicy::Suppress;
    service.upsert_collection_with_context(context.clone(), collection_upsert("inbox"))?;
    service.upsert_thing_with_context(context.clone(), thing_upsert("task-1", "inbox"))?;

    let result = service.set_thing_status_with_context(context, "task-1", "done")?;

    assert!(result.change_log_ids.is_empty());
    assert!(result.event_range.is_some());
    assert!(storage.list_things_change_log(10, 0)?.is_empty());
    let snapshot = DocumentPersistence::new(&storage)
        .load_document_set("device-status-suppress")?
        .extract_snapshot()?;
    let thing = snapshot
        .things
        .iter()
        .find(|thing| thing.uuid == "task-1")
        .expect("thing exists");
    assert_eq!(thing.status, "done");
    Ok(())
}

#[test]
fn delete_thing_records_user_visible_change_log_by_default() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-delete-changelog", collection_upsert("inbox"))?;
    service.upsert_thing("device-delete-changelog", thing_upsert("task-1", "inbox"))?;

    let result = service.delete_thing("device-delete-changelog", "inbox", "task-1")?;

    assert!(result.value);
    assert_eq!(result.change_log_ids.len(), 1);
    let logs = storage.list_things_change_log(10, 0)?;
    assert!(logs.iter().any(|log| {
        log.entity_type == "thing"
            && log.entity_uuid == "task-1"
            && log.summary.contains("Archived thing")
    }));
    assert!(
        !storage
            .list_things_content_snapshots("task-1", 10)?
            .is_empty()
    );
    Ok(())
}

#[test]
fn delete_thing_honors_suppress_change_log_policy() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let mut context = ThingsMutationContext::local_command("device-delete-suppress");
    context.change_log_policy = ChangeLogPolicy::Suppress;
    service.upsert_collection_with_context(context.clone(), collection_upsert("inbox"))?;
    service.upsert_thing_with_context(context.clone(), thing_upsert("task-1", "inbox"))?;

    let result = service.delete_thing_with_context(context, "inbox", "task-1")?;

    assert!(result.value);
    assert!(result.change_log_ids.is_empty());
    assert!(result.event_range.is_some());
    assert!(storage.list_things_change_log(10, 0)?.is_empty());
    assert!(
        storage
            .list_things_content_snapshots("task-1", 10)?
            .is_empty()
    );
    let snapshot = DocumentPersistence::new(&storage)
        .load_document_set("device-delete-suppress")?
        .extract_snapshot()?;
    let thing = snapshot
        .things
        .iter()
        .find(|thing| thing.uuid == "task-1")
        .expect("archived thing remains visible");
    assert_eq!(thing.collection_uuid, SYSTEM_TRASH_COLLECTION_ID);
    assert!(thing.archived_at.is_some());
    Ok(())
}

#[test]
fn delete_collection_records_user_visible_change_log_by_default() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-delete-collection", collection_upsert("inbox"))?;
    service.upsert_thing("device-delete-collection", thing_upsert("task-1", "inbox"))?;

    let result = service.delete_collection("device-delete-collection", "inbox")?;

    assert!(result.value.deleted);
    assert_eq!(result.change_log_ids.len(), 1);
    let logs = storage.list_things_change_log(10, 0)?;
    assert!(logs.iter().any(|log| {
        log.entity_type == "collection"
            && log.entity_uuid == "inbox"
            && log.summary.contains("Archived collection")
    }));
    Ok(())
}

#[test]
fn delete_collection_honors_suppress_change_log_policy() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let mut context = ThingsMutationContext::local_command("device-delete-collection-suppress");
    context.change_log_policy = ChangeLogPolicy::Suppress;
    service.upsert_collection_with_context(context.clone(), collection_upsert("inbox"))?;
    service.upsert_thing_with_context(context.clone(), thing_upsert("task-1", "inbox"))?;

    let result = service.delete_collection_with_context(context, "inbox")?;

    assert!(result.value.deleted);
    assert!(result.change_log_ids.is_empty());
    assert!(result.event_range.is_some());
    assert!(storage.list_things_change_log(10, 0)?.is_empty());
    let snapshot = DocumentPersistence::new(&storage)
        .load_document_set("device-delete-collection-suppress")?
        .extract_snapshot()?;
    let collection = snapshot
        .collections
        .iter()
        .find(|item| item.uuid == "inbox")
        .expect("archived collection remains visible");
    assert!(collection.archived_at.is_some());
    assert!(snapshot.things.iter().any(|thing| thing.uuid == "task-1"));
    Ok(())
}

#[test]
fn set_status_records_user_visible_change_log_by_default() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-set-status", collection_upsert("inbox"))?;
    service.upsert_thing("device-set-status", thing_upsert("task-1", "inbox"))?;

    let result = service.set_status("device-set-status", "task-1", "done", Some(42))?;

    assert_eq!(result.change_log_ids.len(), 1);
    let logs = storage.list_things_change_log(10, 0)?;
    assert!(logs.iter().any(|log| {
        log.entity_type == "thing"
            && log.entity_uuid == "task-1"
            && log.summary.contains("Changed status")
    }));
    Ok(())
}

#[test]
fn set_status_honors_suppress_change_log_policy() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let mut context = ThingsMutationContext::local_command("device-set-status-suppress");
    context.change_log_policy = ChangeLogPolicy::Suppress;
    service.upsert_collection_with_context(context.clone(), collection_upsert("inbox"))?;
    service.upsert_thing_with_context(context.clone(), thing_upsert("task-1", "inbox"))?;

    let result = service.set_status_with_context(context, "task-1", "done", Some(42))?;

    assert!(result.change_log_ids.is_empty());
    assert!(result.event_range.is_some());
    assert!(storage.list_things_change_log(10, 0)?.is_empty());
    let snapshot = DocumentPersistence::new(&storage)
        .load_document_set("device-set-status-suppress")?
        .extract_snapshot()?;
    let thing = snapshot
        .things
        .iter()
        .find(|thing| thing.uuid == "task-1")
        .expect("thing exists");
    assert_eq!(thing.status, "done");
    Ok(())
}

#[test]
fn move_thing_records_user_visible_change_log_by_default() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-move-thing", collection_upsert("inbox"))?;
    service.upsert_collection("device-move-thing", collection_upsert("later"))?;
    service.upsert_thing("device-move-thing", thing_upsert("task-1", "inbox"))?;

    let result = service.move_thing("device-move-thing", "task-1", "later", None)?;

    assert_eq!(result.value.collection_uuid, "later");
    assert_eq!(result.change_log_ids.len(), 1);
    let logs = storage.list_things_change_log(10, 0)?;
    assert!(logs.iter().any(|log| {
        log.entity_type == "thing"
            && log.entity_uuid == "task-1"
            && log.summary.contains("Moved thing")
    }));
    Ok(())
}

#[test]
fn move_thing_honors_suppress_change_log_policy() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let mut context = ThingsMutationContext::local_command("device-move-thing-suppress");
    context.change_log_policy = ChangeLogPolicy::Suppress;
    service.upsert_collection_with_context(context.clone(), collection_upsert("inbox"))?;
    service.upsert_collection_with_context(context.clone(), collection_upsert("later"))?;
    service.upsert_thing_with_context(context.clone(), thing_upsert("task-1", "inbox"))?;

    let result = service.move_thing_with_context(context, "task-1", "later", None)?;

    assert_eq!(result.value.collection_uuid, "later");
    assert!(result.change_log_ids.is_empty());
    assert!(result.event_range.is_some());
    assert!(storage.list_things_change_log(10, 0)?.is_empty());
    let snapshot = DocumentPersistence::new(&storage)
        .load_document_set("device-move-thing-suppress")?
        .extract_snapshot()?;
    let thing = snapshot
        .things
        .iter()
        .find(|thing| thing.uuid == "task-1")
        .expect("thing exists");
    assert_eq!(thing.collection_uuid, "later");
    Ok(())
}

#[test]
fn splice_text_records_user_visible_change_log_by_default() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-splice-text", collection_upsert("inbox"))?;
    service.upsert_thing("device-splice-text", thing_upsert("task-1", "inbox"))?;

    let result = service.splice_text("device-splice-text", "task-1", "main", 5, 0, " world")?;

    assert!(result.value);
    assert_eq!(result.change_log_ids.len(), 1);
    let logs = storage.list_things_change_log(10, 0)?;
    assert!(logs.iter().any(|log| {
        log.entity_type == "thing"
            && log.entity_uuid == "task-1"
            && log.summary.contains("Edited thing content")
    }));
    Ok(())
}

#[test]
fn splice_text_honors_suppress_change_log_policy() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let mut context = ThingsMutationContext::local_command("device-splice-text-suppress");
    context.change_log_policy = ChangeLogPolicy::Suppress;
    service.upsert_collection_with_context(context.clone(), collection_upsert("inbox"))?;
    service.upsert_thing_with_context(context.clone(), thing_upsert("task-1", "inbox"))?;

    let result = service.splice_text_with_context(context, "task-1", "main", 5, 0, " world")?;

    assert!(result.value);
    assert!(result.change_log_ids.is_empty());
    assert!(result.event_range.is_some());
    assert!(storage.list_things_change_log(10, 0)?.is_empty());
    let doc_set =
        DocumentPersistence::new(&storage).load_document_set("device-splice-text-suppress")?;
    assert_eq!(
        doc_set.get_thing_markdown_text("task-1")?.as_deref(),
        Some("hello world")
    );
    Ok(())
}

#[test]
fn edit_content_records_user_visible_change_log_by_default() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-edit-content", collection_upsert("inbox"))?;
    service.upsert_thing("device-edit-content", thing_upsert("task-1", "inbox"))?;

    let result = service.edit_content(
        "device-edit-content",
        "task-1",
        "overwrite",
        Some("Task updated"),
        Some("new markdown"),
        None,
        None,
        None,
        None,
        None,
    )?;

    assert_eq!(result.change_log_ids.len(), 1);
    let logs = storage.list_things_change_log(10, 0)?;
    assert!(logs.iter().any(|log| {
        log.entity_type == "thing"
            && log.entity_uuid == "task-1"
            && log.summary.contains("Edited thing")
    }));
    Ok(())
}

#[test]
fn edit_content_honors_suppress_change_log_policy() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let mut context = ThingsMutationContext::local_command("device-edit-content-suppress");
    context.change_log_policy = ChangeLogPolicy::Suppress;
    service.upsert_collection_with_context(context.clone(), collection_upsert("inbox"))?;
    service.upsert_thing_with_context(context.clone(), thing_upsert("task-1", "inbox"))?;

    let result = service.edit_content_with_context(
        context,
        "task-1",
        "overwrite",
        Some("Task updated"),
        Some("new markdown"),
        None,
        None,
        None,
        None,
        None,
    )?;

    assert!(result.change_log_ids.is_empty());
    assert!(result.event_range.is_some());
    assert!(storage.list_things_change_log(10, 0)?.is_empty());
    let doc_set =
        DocumentPersistence::new(&storage).load_document_set("device-edit-content-suppress")?;
    assert_eq!(
        doc_set.get_thing_markdown_text("task-1")?.as_deref(),
        Some("new markdown")
    );
    Ok(())
}

#[test]
fn edit_content_append_uses_mergeable_text_splice() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let base_storage = Storage::new(temp.path().join("base.sqlite"))?;
    let base_service = ThingsLocalService::new(&base_storage);
    base_service.upsert_collection("base-device", collection_upsert("inbox"))?;
    base_service.upsert_thing("base-device", thing_upsert("task-1", "inbox"))?;
    let base_rows = base_storage.list_crdt_documents()?;

    let storage_a = Storage::new(temp.path().join("a.sqlite"))?;
    let storage_b = Storage::new(temp.path().join("b.sqlite"))?;
    for storage in [&storage_a, &storage_b] {
        for row in &base_rows {
            storage.save_crdt_document(
                &row.uuid,
                &row.data_type,
                &row.automerge_doc,
                &row.sync_state,
                false,
                row.last_sync_at.as_deref(),
            )?;
        }
    }

    let service_a = ThingsLocalService::new(&storage_a);
    let service_b = ThingsLocalService::new(&storage_b);
    service_a.edit_content(
        "device-a",
        "task-1",
        "append",
        None,
        None,
        None,
        None,
        None,
        None,
        Some("\nA edit"),
    )?;
    service_b.edit_content(
        "device-b",
        "task-1",
        "append",
        None,
        None,
        None,
        None,
        None,
        None,
        Some("\nB edit"),
    )?;

    let row_a = storage_a
        .get_crdt_document("task-1", "thing_markdown")?
        .expect("a markdown doc");
    let row_b = storage_b
        .get_crdt_document("task-1", "thing_markdown")?
        .expect("b markdown doc");
    let mut merged = automerge::AutoCommit::load(&row_a.automerge_doc)?;
    let mut incoming = automerge::AutoCommit::load(&row_b.automerge_doc)?;
    merged.merge(&mut incoming)?;

    let mut doc_set = ThingsDocumentSet::new("merged-device");
    doc_set.set(
        DocumentKey::thing_markdown("task-1"),
        DocumentState {
            automerge_doc: merged.save(),
            sync_state: Vec::new(),
            dirty: false,
            last_sync_at: None,
        },
    );
    let text = doc_set
        .get_thing_markdown_text("task-1")?
        .expect("merged markdown");
    assert!(text.contains("hello"), "{text}");
    assert!(text.contains("A edit"), "{text}");
    assert!(text.contains("B edit"), "{text}");
    Ok(())
}

#[test]
fn action_bindings_round_trip_through_local_service() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-bindings", collection_upsert("inbox"))?;
    service.upsert_thing("device-bindings", thing_upsert("task-1", "inbox"))?;

    let bindings = vec![EntityActionBinding {
        action_uuid: "action-1".to_string(),
        label_override: Some("Run".to_string()),
        args_json: json!({ "mode": "fast" }),
    }];

    service.set_collection_action_bindings("device-bindings", "inbox", &bindings)?;
    service.set_thing_action_bindings("device-bindings", "task-1", &bindings)?;

    assert_eq!(
        service.list_collection_action_bindings("device-bindings", "inbox")?,
        bindings
    );
    assert_eq!(
        service.list_thing_action_bindings("device-bindings", "task-1")?,
        bindings
    );
    Ok(())
}

#[test]
fn record_undo_change_log_marks_original_and_records_undo() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let original_id = storage.insert_things_change_log(
        "device-undo",
        ThingsOperationType::CreateThing,
        "thing",
        "task-1",
        "Created thing 'Task'",
        &json!({ "uuid": "task-1" }).to_string(),
        None,
        true,
    )?;
    let original = storage
        .get_things_change_log(original_id)?
        .expect("original log exists");

    let undo_id = service
        .record_undo_change_log("device-undo", &original, "Undone: Created thing 'Task'")?
        .expect("undo log id");

    let original_after = storage
        .get_things_change_log(original_id)?
        .expect("original log still exists");
    assert!(!original_after.can_undo);
    let undo = storage
        .get_things_change_log(undo_id)?
        .expect("undo log exists");
    assert_eq!(undo.op_type, ThingsOperationType::UndoCreateThing);
    assert_eq!(undo.entity_type, "thing");
    assert_eq!(undo.entity_uuid, "task-1");
    assert!(!undo.can_undo);
    Ok(())
}

#[test]
fn cleanup_change_logs_is_exposed_by_local_service() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    let log_id = storage.insert_things_change_log(
        "device-cleanup",
        ThingsOperationType::CreateThing,
        "thing",
        "task-1",
        "Created thing 'Task'",
        &json!({ "uuid": "task-1" }).to_string(),
        None,
        true,
    )?;
    storage.insert_things_content_snapshot(
        "device-cleanup",
        "task-1",
        r#"{"uuid":"task-1"}"#,
        Some(log_id),
    )?;

    let (logs_deleted, snapshots_deleted) = service.cleanup_change_logs(-1)?;

    assert_eq!(logs_deleted, 1);
    assert_eq!(snapshots_deleted, 1);
    assert!(storage.list_things_change_log(10, 0)?.is_empty());
    assert!(
        storage
            .list_things_content_snapshots("task-1", 10)?
            .is_empty()
    );
    Ok(())
}

#[test]
fn preview_undo_reads_change_log_and_conflict_from_local_service() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);

    let result = service.upsert_collection("device-preview-undo", collection_upsert("inbox"))?;
    let log_id = result
        .change_log_ids
        .first()
        .copied()
        .expect("collection upsert records a changelog entry");

    let preview = service.preview_undo("device-preview-undo", log_id)?;

    assert_eq!(preview.log_entry.id, log_id);
    assert_eq!(preview.log_entry.entity_type, "collection");
    assert_eq!(preview.log_entry.entity_uuid, "inbox");
    assert!(!preview.needs_cascade_restore);
    assert!(preview.conflict.is_none());
    assert!(preview.cascade_entries.is_empty());
    Ok(())
}

#[test]
fn execute_undo_runs_through_local_service() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Storage::new(temp.path().join("things.sqlite"))?;
    let service = ThingsLocalService::new(&storage);
    service.upsert_collection("device-execute-undo", collection_upsert("inbox"))?;
    let result = service.upsert_thing("device-execute-undo", thing_upsert("task-undo", "inbox"))?;
    let log_id = result
        .change_log_ids
        .first()
        .copied()
        .expect("thing upsert records a changelog entry");

    let message = service.execute_undo(
        "device-execute-undo",
        ThingsUndoExecution {
            log_id,
            resolution_option: None,
            target_collection_uuid: None,
        },
    )?;

    assert_eq!(message, "Undone: Created thing 'Task'");
    let archived = service
        .get_thing("device-execute-undo", "task-undo", false)?
        .expect("undo create archives thing under new delete semantics");
    assert_eq!(archived.collection_uuid, SYSTEM_TRASH_COLLECTION_ID);
    assert!(archived.archived_at.is_some());
    let logs = storage.list_things_change_log(10, 0)?;
    assert!(
        logs.iter()
            .any(|entry| entry.op_type == ThingsOperationType::UndoCreateThing)
    );
    assert!(
        !storage
            .get_things_change_log(log_id)?
            .expect("original log still exists")
            .can_undo
    );
    let events = service.watch_since("device-execute-undo", 0, 20)?;
    assert!(events.iter().any(|event| event.entity_type == "thing"
        && event.entity_uuid == "task-undo"
        && event.change_kind == "deleted"));
    Ok(())
}

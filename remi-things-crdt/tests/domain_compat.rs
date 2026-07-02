use remi_things_crdt::{
    format_domain_datetime, parse_domain_datetime, CollectionId, ContentEntry, ContentEntryId,
    ContentEntryPayload, FieldPatch, ThingCollectionEntry, ThingCollectionUpsert, ThingDatatype,
    ThingEntry, ThingId, ThingStatus, ThingUpsert, ThingsChangeLogEntry, ThingsOperationType,
    ThingsSnapshotState, ThingsSyncSummary, ThingsUndoPreview,
};
use serde_json::json;
use std::str::FromStr;

fn dt(raw: &str) -> chrono::DateTime<chrono::Utc> {
    parse_domain_datetime(raw).unwrap()
}

#[test]
fn sdk_facing_domain_models_keep_legacy_json_shape() {
    let collection = ThingCollectionEntry {
        uuid: "collection-1".to_string(),
        title: "Inbox".to_string(),
        collection_type: Default::default(),
        app_id: None,
        archived_at: None,
        trigger_uuid: None,
        card_jsx: Some("<Card />".to_string()),
        created_at: dt("2026-01-01T00:00:00Z"),
        updated_at: dt("2026-01-02T00:00:00Z"),
        actor_type: Some("application".to_string()),
        actor_app_id: Some("app-1".to_string()),
        actor_display_name: Some("Remi".to_string()),
    };
    let thing = ThingEntry {
        uuid: "thing-1".to_string(),
        title: "Read plan".to_string(),
        datatype: ThingDatatype::Text,
        data: json!({ "content": { "kind": "markdown" } }),
        collection_uuid: "collection-1".to_string(),
        trigger_uuid: Some("trigger-1".to_string()),
        parent_uuid: None,
        archived_at: None,
        archived_from_collection_uuid: None,
        created_at: dt("2026-01-01T00:00:00Z"),
        updated_at: dt("2026-01-02T00:00:00Z"),
        status: "in-progress".to_string(),
        status_timestamp_ms: Some(1_767_225_600_000),
        actor_type: None,
        actor_app_id: None,
        actor_display_name: None,
    };
    let snapshot = ThingsSnapshotState {
        collections: vec![collection],
        things: vec![thing],
        dirty: true,
        last_sync_at: Some(dt("2026-01-03T00:00:00Z")),
    };

    let encoded = serde_json::to_value(snapshot).unwrap();
    assert_eq!(encoded["collections"][0]["uuid"], "collection-1");
    assert_eq!(encoded["collections"][0]["card_jsx"], "<Card />");
    assert!(encoded["collections"][0].get("trigger_uuid").is_none());
    assert_eq!(encoded["things"][0]["datatype"], "text");
    assert_eq!(encoded["things"][0]["trigger_uuid"], "trigger-1");
    assert!(encoded["things"][0].get("parent_uuid").is_some());
    assert_eq!(encoded["dirty"], true);
    assert_eq!(encoded["last_sync_at"], "2026-01-03T00:00:00Z");
}

#[test]
fn upsert_models_keep_compatibility_tri_state_fields() {
    let collection = ThingCollectionUpsert {
        uuid: "collection-1".to_string(),
        title: "Inbox".to_string(),
        collection_type: Default::default(),
        app_id: None,
        trigger_uuid_patch: FieldPatch::Noop,
        trigger_uuid: Some(String::new()),
        created_at: None,
        updated_at: None,
    };
    let thing = ThingUpsert {
        uuid: "thing-1".to_string(),
        title: "Read plan".to_string(),
        datatype: ThingDatatype::Text,
        data: None,
        collection_uuid: "collection-1".to_string(),
        trigger_uuid_patch: FieldPatch::Noop,
        trigger_uuid: Some(String::new()),
        parent_uuid: None,
        created_at: None,
        updated_at: None,
    };

    let collection_json = serde_json::to_value(collection).unwrap();
    let thing_json = serde_json::to_value(thing).unwrap();

    assert_eq!(collection_json["trigger_uuid"], "");
    assert!(collection_json.get("created_at").is_some());
    assert_eq!(thing_json["trigger_uuid"], "");
    assert!(thing_json.get("data").is_none());
}

#[test]
fn compat_trigger_uuid_fields_adapt_to_explicit_patch() {
    let noop = ThingCollectionUpsert {
        uuid: "collection-1".to_string(),
        title: "Inbox".to_string(),
        collection_type: Default::default(),
        app_id: None,
        trigger_uuid_patch: FieldPatch::Noop,
        trigger_uuid: None,
        created_at: None,
        updated_at: None,
    };
    assert_eq!(noop.trigger_uuid_patch(), FieldPatch::Noop);

    let clear = ThingCollectionUpsert {
        trigger_uuid: Some("  ".to_string()),
        ..noop.clone()
    };
    assert_eq!(clear.trigger_uuid_patch(), FieldPatch::Clear);

    let explicit_set = ThingCollectionUpsert {
        trigger_uuid_patch: FieldPatch::Set("explicit-trigger".to_string()),
        trigger_uuid: Some("legacy-trigger".to_string()),
        ..noop.clone()
    };
    assert_eq!(
        explicit_set.trigger_uuid_patch(),
        FieldPatch::Set("explicit-trigger".to_string())
    );

    let set = ThingUpsert {
        uuid: "thing-1".to_string(),
        title: "Read plan".to_string(),
        datatype: ThingDatatype::Text,
        data: None,
        collection_uuid: "collection-1".to_string(),
        trigger_uuid_patch: FieldPatch::Noop,
        trigger_uuid: Some(" trigger-1 ".to_string()),
        parent_uuid: None,
        created_at: None,
        updated_at: None,
    };
    assert_eq!(
        set.trigger_uuid_patch(),
        FieldPatch::Set("trigger-1".to_string())
    );
}

#[test]
fn thing_status_parses_and_serializes_storage_strings() {
    assert_eq!(
        ThingStatus::from_str("in-progress").unwrap(),
        ThingStatus::InProgress
    );
    assert_eq!(ThingStatus::Done.as_str(), "done");
    assert_eq!(
        serde_json::to_value(ThingStatus::Stalled).unwrap(),
        "stalled"
    );
    assert!(ThingStatus::from_str("blocked").is_err());

    let thing = ThingEntry {
        uuid: "thing-1".to_string(),
        title: "Read plan".to_string(),
        datatype: ThingDatatype::Text,
        data: json!({}),
        collection_uuid: "collection-1".to_string(),
        trigger_uuid: None,
        parent_uuid: None,
        archived_at: None,
        archived_from_collection_uuid: None,
        created_at: dt("2026-01-01T00:00:00Z"),
        updated_at: dt("2026-01-02T00:00:00Z"),
        status: "done".to_string(),
        status_timestamp_ms: None,
        actor_type: None,
        actor_app_id: None,
        actor_display_name: None,
    };
    assert_eq!(thing.status_enum().unwrap(), ThingStatus::Done);
}

#[test]
fn domain_id_newtypes_are_json_compatible_and_adapt_legacy_fields() {
    let thing_id = ThingId::from("thing-1");
    let collection_id = CollectionId::from("collection-1");
    let entry_id = ContentEntryId::from("entry-1");

    assert_eq!(serde_json::to_value(&thing_id).unwrap(), "thing-1");
    assert_eq!(
        serde_json::from_value::<CollectionId>(json!("collection-1"))
            .unwrap()
            .as_str(),
        collection_id.as_str()
    );
    assert_eq!(entry_id.to_string(), "entry-1");
    assert!(" ".parse::<ThingId>().is_err());

    let collection = ThingCollectionEntry {
        uuid: collection_id.to_string(),
        title: "Inbox".to_string(),
        collection_type: Default::default(),
        app_id: None,
        archived_at: None,
        trigger_uuid: None,
        card_jsx: None,
        created_at: dt("2026-01-01T00:00:00Z"),
        updated_at: dt("2026-01-02T00:00:00Z"),
        actor_type: None,
        actor_app_id: None,
        actor_display_name: None,
    };
    assert_eq!(collection.id(), collection_id);

    let thing = ThingEntry {
        uuid: thing_id.to_string(),
        title: "Read plan".to_string(),
        datatype: ThingDatatype::Text,
        data: json!({}),
        collection_uuid: collection_id.to_string(),
        trigger_uuid: None,
        archived_at: None,
        archived_from_collection_uuid: None,
        parent_uuid: Some("parent-1".to_string()),
        created_at: dt("2026-01-01T00:00:00Z"),
        updated_at: dt("2026-01-02T00:00:00Z"),
        status: "none".to_string(),
        status_timestamp_ms: None,
        actor_type: None,
        actor_app_id: None,
        actor_display_name: None,
    };
    assert_eq!(thing.id(), thing_id);
    assert_eq!(thing.collection_id(), collection_id);
    assert_eq!(thing.parent_id().unwrap(), ThingId::from("parent-1"));

    let entry = ContentEntry {
        id: entry_id.to_string(),
        title: None,
        order: 0.0,
        payload: ContentEntryPayload::Custom {
            content_type: "test".to_string(),
            data: json!({}),
        },
    };
    assert_eq!(entry.id(), entry_id);
}

#[test]
fn domain_time_adapters_parse_rfc3339_without_changing_json_shape() {
    let collection = ThingCollectionEntry {
        uuid: "collection-1".to_string(),
        title: "Inbox".to_string(),
        collection_type: Default::default(),
        app_id: None,
        archived_at: None,
        trigger_uuid: None,
        card_jsx: None,
        created_at: dt("2026-01-01T08:00:00+08:00"),
        updated_at: dt("2026-01-02T00:00:00Z"),
        actor_type: None,
        actor_app_id: None,
        actor_display_name: None,
    };
    assert_eq!(
        format_domain_datetime(collection.created_at_utc().unwrap()),
        "2026-01-01T00:00:00Z"
    );
    assert_eq!(
        format_domain_datetime(collection.updated_at_utc().unwrap()),
        "2026-01-02T00:00:00Z"
    );

    let thing = ThingEntry {
        uuid: "thing-1".to_string(),
        title: "Read plan".to_string(),
        datatype: ThingDatatype::Text,
        data: json!({}),
        collection_uuid: "collection-1".to_string(),
        trigger_uuid: None,
        parent_uuid: None,
        archived_at: None,
        archived_from_collection_uuid: None,
        created_at: dt("2026-01-01T00:00:00Z"),
        updated_at: dt("2026-01-02T00:00:00Z"),
        status: "done".to_string(),
        status_timestamp_ms: Some(1_767_225_600_000),
        actor_type: None,
        actor_app_id: None,
        actor_display_name: None,
    };
    assert_eq!(
        format_domain_datetime(thing.status_updated_at_utc().unwrap()),
        "2026-01-01T00:00:00Z"
    );

    let upsert = ThingUpsert {
        uuid: "thing-1".to_string(),
        title: "Read plan".to_string(),
        datatype: ThingDatatype::Text,
        data: None,
        collection_uuid: "collection-1".to_string(),
        trigger_uuid_patch: FieldPatch::Noop,
        trigger_uuid: None,
        parent_uuid: None,
        created_at: Some("2026-01-01T00:00:00Z".to_string()),
        updated_at: None,
    };
    assert_eq!(
        format_domain_datetime(upsert.created_at_utc().unwrap().unwrap()),
        "2026-01-01T00:00:00Z"
    );
    assert!(upsert.updated_at_utc().unwrap().is_none());

    let snapshot = ThingsSnapshotState {
        collections: vec![collection],
        things: vec![thing],
        dirty: false,
        last_sync_at: Some(dt("2026-01-03T00:00:00Z")),
    };
    assert_eq!(
        format_domain_datetime(snapshot.last_sync_at_utc().unwrap().unwrap()),
        "2026-01-03T00:00:00Z"
    );
    assert_eq!(
        serde_json::to_value(snapshot).unwrap()["last_sync_at"],
        "2026-01-03T00:00:00Z"
    );
}

#[test]
fn domain_time_adapters_reject_invalid_timestamps() {
    assert!(parse_domain_datetime("not-a-date").is_err());

    let collection = ThingCollectionUpsert {
        uuid: "collection-1".to_string(),
        title: "Inbox".to_string(),
        collection_type: Default::default(),
        app_id: None,
        trigger_uuid_patch: FieldPatch::Noop,
        trigger_uuid: None,
        created_at: Some("2026-99-99T00:00:00Z".to_string()),
        updated_at: None,
    };
    assert!(collection.created_at_utc().is_err());
    assert!(collection.updated_at_utc().unwrap().is_none());
}

#[test]
fn changelog_undo_and_sync_summary_are_domain_serializable() {
    let created_at = parse_domain_datetime("2026-06-28T12:34:56Z").unwrap();
    let entry = ThingsChangeLogEntry {
        id: 42,
        device_id: "device-1".to_string(),
        op_type: ThingsOperationType::MoveThing,
        entity_type: "thing".to_string(),
        entity_uuid: "thing-1".to_string(),
        summary: "Moved thing".to_string(),
        details_json: json!({"from": "a", "to": "b"}).to_string(),
        parent_log_id: None,
        cascade_log_ids_json: None,
        created_at,
        can_undo: true,
        synced: false,
    };

    assert_eq!(
        ThingsOperationType::from_str("move_thing").unwrap(),
        ThingsOperationType::MoveThing
    );
    assert_eq!(ThingsOperationType::MoveThing.as_str(), "move_thing");
    assert_eq!(
        ThingsOperationType::MoveThing.to_undo_variant(),
        Some(ThingsOperationType::UndoMoveThing)
    );

    let preview = ThingsUndoPreview {
        log_entry: entry.clone(),
        needs_cascade_restore: false,
        conflict: None,
        cascade_entries: vec![entry.clone()],
    };
    let summary = ThingsSyncSummary {
        documents_synced: 3,
        documents_pushed: 1,
        documents_pulled: 2,
        last_sync_at: Some(dt("2026-06-28T12:35:00Z")),
        generated_event_range: Some((10, 12)),
    };

    let encoded_entry = serde_json::to_value(&entry).unwrap();
    assert_eq!(encoded_entry["op_type"], "move_thing");
    assert_eq!(encoded_entry["created_at"], "2026-06-28T12:34:56Z");
    assert_eq!(
        serde_json::to_value(preview).unwrap()["cascade_entries"][0]["id"],
        42
    );
    assert_eq!(
        serde_json::to_value(summary).unwrap()["documents_pulled"],
        2
    );
}

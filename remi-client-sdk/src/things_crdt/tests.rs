use super::{DocumentPersistence, SnapshotOptions, ThingsDocumentSet, format_domain_datetime};
use crate::things_events::{ThingsDocumentChangeKind, ThingsDocumentEvent};
use remi_things_crdt::{ContentEntry, ContentEntryPayload, ThingDatatype};
use serde_json::{Value, json};

#[test]
fn test_document_set_basic() {
    let mut docs = ThingsDocumentSet::new("test-device");
    docs.init_root().unwrap();

    // Add a collection
    docs.get_or_init_collection("coll-1").unwrap();
    let snapshot = docs.extract_snapshot().unwrap();
    assert!(
        snapshot
            .collections
            .iter()
            .any(|coll| coll.uuid == "coll-1")
    );

    // Add a thing
    docs.upsert_thing_meta(
        "coll-1",
        "thing-1",
        Some(ThingDatatype::Markdown),
        Some("none".to_string()),
        Some("My Task".to_string()),
        None,
    )
    .unwrap();

    let coll = docs.collection_view("coll-1").unwrap();
    assert_eq!(coll.things.len(), 1);
    assert_eq!(coll.things[0].id, "thing-1");
}

#[test]
fn test_load_or_init_document_set_does_not_persist_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate::storage::Storage::new(dir.path().join("sdk.sqlite3")).unwrap();
    let persistence = DocumentPersistence::new(&storage);

    let docs = persistence
        .load_or_init_document_set("test-device")
        .unwrap();
    assert!(docs.has_root_document());
    assert!(docs.has_pending_changes());
    assert!(storage.list_crdt_documents().unwrap().is_empty());
}

#[test]
fn test_document_set_snapshot() {
    let mut docs = ThingsDocumentSet::new("test-device");
    docs.get_or_init_collection("coll-1").unwrap();
    docs.update_collection_meta_with_timestamps(
        "coll-1",
        Some("My Collection".to_string()),
        None,
        Some("2026-01-01T00:00:00Z".to_string()),
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();
    docs.upsert_thing_meta_with_timestamps(
        "coll-1",
        "thing-1",
        Some(ThingDatatype::Markdown),
        Some("none".to_string()),
        Some("Task 1".to_string()),
        None,
        Some("2026-01-03T00:00:00Z".to_string()),
        Some("2026-01-04T00:00:00Z".to_string()),
    )
    .unwrap();

    let snapshot = docs
        .extract_snapshot_with_options(SnapshotOptions {
            include_content: false,
        })
        .unwrap();
    assert_eq!(snapshot.collections.len(), 1);
    assert_eq!(snapshot.collections[0].title, "My Collection");
    assert_eq!(
        format_domain_datetime(snapshot.collections[0].created_at),
        "2026-01-01T00:00:00Z"
    );
    assert_eq!(
        format_domain_datetime(snapshot.collections[0].updated_at),
        "2026-01-02T00:00:00Z"
    );
    assert_eq!(snapshot.things.len(), 1);
    assert_eq!(snapshot.things[0].title, "Task 1");
    assert_eq!(
        format_domain_datetime(snapshot.things[0].created_at),
        "2026-01-03T00:00:00Z"
    );
    assert_eq!(
        format_domain_datetime(snapshot.things[0].updated_at),
        "2026-01-04T00:00:00Z"
    );
    assert_eq!(snapshot.things[0].data.get("attrs"), Some(&Value::Null));
}

#[test]
fn test_document_set_normalizes_rfc3339_timestamps_to_utc() {
    let mut docs = ThingsDocumentSet::new("test-device");
    docs.update_collection_meta_with_timestamps(
        "coll-1",
        Some("My Collection".to_string()),
        None,
        Some("2026-01-01T08:00:00+08:00".to_string()),
        Some("2026-01-02T08:00:00+08:00".to_string()),
    )
    .unwrap();

    let snapshot = docs.extract_snapshot().unwrap();
    assert_eq!(
        format_domain_datetime(snapshot.collections[0].created_at),
        "2026-01-01T00:00:00Z"
    );
    assert_eq!(
        format_domain_datetime(snapshot.collections[0].updated_at),
        "2026-01-02T00:00:00Z"
    );
}

#[test]
fn test_document_set_rejects_invalid_timestamps_before_writing() {
    let mut docs = ThingsDocumentSet::new("test-device");
    assert!(
        docs.update_collection_meta_with_timestamps(
            "coll-1",
            Some("My Collection".to_string()),
            None,
            Some("not-a-date".to_string()),
            None,
        )
        .is_err()
    );

    docs.get_or_init_collection("coll-1").unwrap();
    assert!(
        docs.upsert_thing_meta_with_timestamps(
            "coll-1",
            "thing-1",
            Some(ThingDatatype::Markdown),
            Some("none".to_string()),
            Some("Task 1".to_string()),
            None,
            None,
            Some("2026-99-99T00:00:00Z".to_string()),
        )
        .is_err()
    );
}

#[test]
fn test_document_set_preserves_created_at_across_updates() {
    let mut docs = ThingsDocumentSet::new("test-device");
    docs.get_or_init_collection("coll-1").unwrap();
    docs.upsert_thing_meta_with_timestamps(
        "coll-1",
        "thing-1",
        Some(ThingDatatype::Markdown),
        Some("none".to_string()),
        Some("Task 1".to_string()),
        None,
        Some("2026-01-03T00:00:00Z".to_string()),
        Some("2026-01-04T00:00:00Z".to_string()),
    )
    .unwrap();
    docs.upsert_thing_meta_with_timestamps(
        "coll-1",
        "thing-1",
        Some(ThingDatatype::Markdown),
        Some("none".to_string()),
        Some("Task 1 renamed".to_string()),
        None,
        None,
        Some("2026-01-05T00:00:00Z".to_string()),
    )
    .unwrap();

    let snapshot = docs.extract_snapshot().unwrap();
    assert_eq!(snapshot.things.len(), 1);
    assert_eq!(
        format_domain_datetime(snapshot.things[0].created_at),
        "2026-01-03T00:00:00Z"
    );
    assert_eq!(
        format_domain_datetime(snapshot.things[0].updated_at),
        "2026-01-05T00:00:00Z"
    );
}

#[test]
fn test_document_events_follow_crdt_mutations() {
    let mut docs = ThingsDocumentSet::new("test-device");

    let collection_events = docs
        .update_collection_meta("coll-1", Some("Inbox".to_string()), None)
        .unwrap();
    assert_eq!(
        collection_events,
        vec![
            ThingsDocumentEvent::root(ThingsDocumentChangeKind::Updated),
            ThingsDocumentEvent::collection(ThingsDocumentChangeKind::Created, "coll-1"),
        ]
    );

    let thing_events = docs
        .upsert_thing_meta(
            "coll-1",
            "thing-1",
            Some(ThingDatatype::Markdown),
            Some("none".to_string()),
            Some("Task".to_string()),
            None,
        )
        .unwrap();
    assert_eq!(
        thing_events,
        vec![ThingsDocumentEvent::thing(
            ThingsDocumentChangeKind::Created,
            "coll-1",
            "thing-1",
        )]
    );

    let markdown_create = docs.set_thing_markdown_text("thing-1", "hello").unwrap();
    assert_eq!(
        markdown_create,
        vec![ThingsDocumentEvent::thing_markdown(
            ThingsDocumentChangeKind::Created,
            Some("coll-1"),
            "thing-1",
        )]
    );

    let markdown_update = docs
        .try_splice_thing_text("thing-1", "main", 5, 0, " world")
        .unwrap()
        .unwrap();
    assert_eq!(
        markdown_update,
        vec![ThingsDocumentEvent::thing_markdown(
            ThingsDocumentChangeKind::Updated,
            Some("coll-1"),
            "thing-1",
        )]
    );

    let entry = ContentEntry {
        id: "entry-1".to_string(),
        title: Some("Example".to_string()),
        order: 0.0,
        payload: ContentEntryPayload::Custom {
            content_type: "test/custom".to_string(),
            data: json!({ "value": 1 }),
        },
    };

    let add_entry_events = docs.add_content_entry("coll-1", "thing-1", entry).unwrap();
    assert_eq!(
        add_entry_events,
        vec![ThingsDocumentEvent::content_entry(
            ThingsDocumentChangeKind::Created,
            "coll-1",
            "thing-1",
            "entry-1",
        )]
    );

    let update_entry_events = docs
        .update_content_entry(
            "coll-1",
            "thing-1",
            "entry-1",
            Some(Some("Renamed".to_string())),
            None,
            None,
        )
        .unwrap();
    assert_eq!(
        update_entry_events,
        vec![ThingsDocumentEvent::content_entry(
            ThingsDocumentChangeKind::Updated,
            "coll-1",
            "thing-1",
            "entry-1",
        )]
    );

    let delete_entry_events = docs
        .delete_content_entry("coll-1", "thing-1", "entry-1")
        .unwrap();
    assert_eq!(
        delete_entry_events,
        vec![ThingsDocumentEvent::content_entry(
            ThingsDocumentChangeKind::Deleted,
            "coll-1",
            "thing-1",
            "entry-1",
        )]
    );
}

#[test]
fn first_splice_creates_main_markdown_block() {
    let mut docs = ThingsDocumentSet::new("test-device");
    docs.get_or_init_collection("coll-1").unwrap();

    let thing_events = docs
        .upsert_thing_meta(
            "coll-1",
            "thing-empty",
            Some(ThingDatatype::Markdown),
            Some("none".to_string()),
            Some("Empty".to_string()),
            None,
        )
        .unwrap();
    assert_eq!(
        thing_events,
        vec![ThingsDocumentEvent::thing(
            ThingsDocumentChangeKind::Created,
            "coll-1",
            "thing-empty",
        )]
    );

    let markdown_create = docs
        .try_splice_thing_text("thing-empty", "main", 0, 0, "hello")
        .unwrap()
        .unwrap();
    assert_eq!(
        markdown_create,
        vec![ThingsDocumentEvent::thing_markdown(
            ThingsDocumentChangeKind::Created,
            Some("coll-1"),
            "thing-empty",
        )]
    );
    assert_eq!(
        docs.get_thing_markdown_text("thing-empty").unwrap(),
        Some("hello".to_string())
    );
}

#[test]
fn replace_markdown_text_creates_main_block_when_missing() {
    let mut docs = ThingsDocumentSet::new("test-device");
    docs.get_or_init_collection("coll-1").unwrap();

    docs.upsert_thing_meta(
        "coll-1",
        "thing-overwrite",
        Some(ThingDatatype::Markdown),
        Some("none".to_string()),
        Some("Overwrite".to_string()),
        None,
    )
    .unwrap();

    let markdown_create = docs
        .replace_thing_markdown_text("thing-overwrite", "seed")
        .unwrap();
    assert_eq!(
        markdown_create,
        vec![ThingsDocumentEvent::thing_markdown(
            ThingsDocumentChangeKind::Created,
            Some("coll-1"),
            "thing-overwrite",
        )]
    );
    assert_eq!(
        docs.get_thing_markdown_text("thing-overwrite").unwrap(),
        Some("seed".to_string())
    );
}

#[test]
fn test_upsert_thing_requires_existing_collection() {
    let mut docs = ThingsDocumentSet::new("test-device");
    let err = docs
        .upsert_thing_meta(
            "missing-coll",
            "thing-1",
            Some(ThingDatatype::Markdown),
            Some("none".to_string()),
            Some("Task 1".to_string()),
            None,
        )
        .expect_err("thing upsert without collection should fail");

    assert!(
        err.to_string()
            .contains("must exist before adding or reparenting things"),
        "{err:?}"
    );
}

#[test]
fn test_live_reachability_skips_deleted_things() {
    let mut docs = ThingsDocumentSet::new("test-device");
    docs.get_or_init_collection("coll-1").unwrap();
    docs.upsert_thing_meta(
        "coll-1",
        "thing-1",
        Some(ThingDatatype::Markdown),
        Some("none".to_string()),
        Some("Task 1".to_string()),
        None,
    )
    .unwrap();
    docs.set_thing_markdown_text("thing-1", "hello").unwrap();

    let live_collections = docs.active_collection_uuids().unwrap();
    let live_things = docs.active_thing_uuids().unwrap();
    assert!(live_collections.contains("coll-1"));
    assert!(live_things.contains("thing-1"));

    docs.delete_thing("coll-1", "thing-1").unwrap();
    let live_things = docs.active_thing_uuids().unwrap();
    assert!(!live_things.contains("thing-1"));

    let live_collections = docs.active_collection_uuids().unwrap();
    let live_things = docs.active_thing_uuids().unwrap();
    assert!(live_collections.contains("coll-1"));
    assert!(!live_things.contains("thing-1"));
}

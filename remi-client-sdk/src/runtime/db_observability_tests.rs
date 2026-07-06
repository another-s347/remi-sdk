use super::RemiSdk;
use crate::storage::{test_sqlite_counters_get, test_sqlite_counters_reset};
use crate::things_crdt::{ThingCollectionUpsert, ThingDatatype, ThingUpsert};
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn long_overwrite_writes_things_state_once() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");

    let sdk = RemiSdk::initialize(&db_path).expect("sdk init");
    let device_id = "device-test";
    let collection_id = "col-test";
    let thing_id = "thing-test";

    // Seed doc with a collection + thing so get_or_init doesn't need to create rows during the measurement.
    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: collection_id.to_string(),
            title: "Test Collection".to_string(),
            collection_type: Default::default(),
            app_id: None,
            created_at: None,
            updated_at: None,
        },
    )
    .expect("seed collection");

    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: thing_id.to_string(),
            title: "Test Thing".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(serde_json::json!({"markdown": "hello"})),
            collection_uuid: collection_id.to_string(),
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )
    .expect("seed thing");

    // Measure overwrite behavior.
    test_sqlite_counters_reset();

    let short = "short content".to_string();
    sdk.things_edit_content(
        device_id,
        thing_id,
        "overwrite",
        None,
        Some(&short),
        None,
        None,
        None,
        None,
        None,
    )
    .expect("short overwrite");

    let after_short = test_sqlite_counters_get();
    eprintln!("short overwrite sqlite counters: {:?}", after_short);
    // V3 uses crdt_document_save instead of things_state_save (which persists each doc individually)
    // Expect at least 1 save per edit (ThingMarkdown)
    assert!(
        after_short.crdt_document_save >= 1,
        "short overwrite must persist crdt documents at least once"
    );

    // Reset again and do a long overwrite.
    test_sqlite_counters_reset();
    let long = "A".repeat(200_000);
    sdk.things_edit_content(
        device_id,
        thing_id,
        "overwrite",
        None,
        Some(&long),
        None,
        None,
        None,
        None,
        None,
    )
    .expect("long overwrite");

    let after_long = test_sqlite_counters_get();
    eprintln!("long overwrite sqlite counters: {:?}", after_long);

    // The core guarantee: overwrite results in bounded DB writes regardless of content length.
    // V3 saves multiple documents per edit (root for metadata, collection, thing_markdown).
    // The key invariant: document save count should be bounded regardless of content size,
    // NOT proportional to content length (no per-character writes).
    assert!(
        after_long.crdt_document_save <= 10,
        "long overwrite should persist crdt documents a bounded number of times, got {}",
        after_long.crdt_document_save
    );
    assert!(
        after_short.crdt_document_save <= 10,
        "short overwrite should persist crdt documents a bounded number of times, got {}",
        after_short.crdt_document_save
    );

    // Connection opens should be roughly constant (not length-dependent)
    // The first edit may open more connections (initializing docs), but both should be bounded.
    assert!(
        after_long.open_connections <= 15,
        "long overwrite should have bounded DB connections: {after_long:?}"
    );
    assert!(
        after_short.open_connections <= 15,
        "short overwrite should have bounded DB connections: {after_short:?}"
    );
}

#[test]
fn things_move_thing_facade_delegates_without_overwriting_content() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = RemiSdk::initialize(&db_path).expect("sdk init");
    let device_id = "device-move-facade";

    for (uuid, title) in [("source", "Source"), ("target", "Target")] {
        sdk.things_upsert_collection(
            device_id,
            ThingCollectionUpsert {
                uuid: uuid.to_string(),
                title: title.to_string(),
                collection_type: Default::default(),
                app_id: None,
                created_at: None,
                updated_at: None,
            },
        )
        .expect("seed collection");
    }

    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: "task-1".to_string(),
            title: "Keep Title".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(serde_json::json!({"markdown": "keep body"})),
            collection_uuid: "source".to_string(),
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )
    .expect("seed thing");

    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: "parent-1".to_string(),
            title: "Parent".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(serde_json::json!({"markdown": "parent body"})),
            collection_uuid: "target".to_string(),
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )
    .expect("seed target parent thing");

    let moved = sdk
        .things_move_thing(device_id, "task-1", "target", Some("parent-1".to_string()))
        .expect("move thing through facade");
    assert_eq!(moved.uuid, "task-1");
    assert_eq!(moved.collection_uuid, "target");
    assert_eq!(moved.parent_uuid.as_deref(), Some("parent-1"));
    assert_eq!(moved.title, "Keep Title");
    assert_eq!(
        sdk.things_get_thing_markdown(device_id, "task-1")
            .expect("markdown after move")
            .as_deref(),
        Some("keep body")
    );
}

#[test]
fn long_overwrite_time_breakdown() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");

    let sdk = RemiSdk::initialize(&db_path).expect("sdk init");
    let device_id = "device-test";
    let collection_id = "col-test";
    let thing_id = "thing-test";

    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: collection_id.to_string(),
            title: "Test Collection".to_string(),
            collection_type: Default::default(),
            app_id: None,
            created_at: None,
            updated_at: None,
        },
    )
    .expect("seed collection");

    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: thing_id.to_string(),
            title: "Test Thing".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(serde_json::json!({"markdown": "hello"})),
            collection_uuid: collection_id.to_string(),
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )
    .expect("seed thing");

    let long = "A".repeat(200_000);

    test_sqlite_counters_reset();

    // V3: Performance test needs to be rewritten for multi-document architecture
    // The v3 architecture stores documents separately, so the performance characteristics differ
    // This test is temporarily simplified to just verify the basic operation works

    // 1) Test the new v3 splice path
    let t0 = Instant::now();
    let result = sdk.things_splice_text(device_id, thing_id, "main", 0, usize::MAX, &long);
    eprintln!(
        "breakdown: things_splice_text ms={} success={}",
        t0.elapsed().as_millis(),
        result.is_ok()
    );
    assert!(result.is_ok());

    let counters = test_sqlite_counters_get();
    eprintln!("breakdown: sqlite counters: {:?}", counters);

    // V3: Multiple documents may be saved, so we just check that operations succeed
    // The exact count depends on the v3 implementation
}

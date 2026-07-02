use anyhow::{Context, Result};
use remi_client_sdk::TriggerSdk;
use remi_client_sdk::things_crdt::{ThingCollectionUpsert, ThingDatatype, ThingUpsert};
use remi_client_sdk::things_local::SYSTEM_TRASH_COLLECTION_ID;
use remi_things_crdt::{ROOT_DOC_UUID, extract_collection_doc_view, extract_root_view};

fn temp_db_path() -> Result<String> {
    let dir = tempfile::tempdir().context("tempdir")?;
    let path = dir.path().join("remi-sdk-test.sqlite3");
    let path_str = path.to_string_lossy().to_string();
    std::mem::forget(dir);
    Ok(path_str)
}

fn parse_snapshot(sdk: &TriggerSdk, device_id: &str) -> Result<serde_json::Value> {
    let snapshot = sdk.things_list_snapshot(device_id)?;
    serde_json::to_value(snapshot).context("serialize snapshot")
}

#[test]
fn delete_collection_archives_metadata_and_survives_reload() -> Result<()> {
    let db_path = temp_db_path()?;
    let sdk = TriggerSdk::initialize(&db_path).context("init sdk")?;
    let device_id = "device-a";

    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "c1".to_string(),
            title: "Inbox".to_string(),
            collection_type: Default::default(),
            app_id: None,
            trigger_uuid: None,
            trigger_uuid_patch: Default::default(),
            created_at: None,
            updated_at: None,
        },
    )?;
    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: "t1".to_string(),
            title: "Thing 1".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(serde_json::json!({"markdown": "hello world"})),
            collection_uuid: "c1".to_string(),
            trigger_uuid: None,
            trigger_uuid_patch: Default::default(),
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )?;

    let before = parse_snapshot(&sdk, device_id)?;
    assert_eq!(before["collections"].as_array().unwrap().len(), 3);
    assert_eq!(before["things"].as_array().unwrap().len(), 1);

    assert!(sdk.things_delete_collection(device_id, "c1")?);

    let after = parse_snapshot(&sdk, device_id)?;
    assert_eq!(after["collections"].as_array().unwrap().len(), 3);
    assert_eq!(after["things"].as_array().unwrap().len(), 1);
    let archived = after["collections"]
        .as_array()
        .unwrap()
        .iter()
        .find(|collection| collection["uuid"] == "c1")
        .context("archived collection remains in snapshot")?;
    assert_eq!(archived["collection_type"], "normal");
    assert!(archived["archived_at"].as_str().is_some());

    let root_row = sdk
        .crdt_get_document(ROOT_DOC_UUID, "root")?
        .context("root document should exist")?;
    let root_view = extract_root_view(&root_row.automerge_doc)?;
    assert!(root_view.collection_uuids.contains(&"c1".to_string()));

    let collection_row = sdk
        .crdt_get_document("c1", "collection")?
        .context("collection document should be retained for archive sync")?;
    let collection_view = extract_collection_doc_view(&collection_row.automerge_doc, "c1")?;
    assert!(
        !collection_view
            .meta
            .tombstone
            .as_ref()
            .map(|t| t.deleted)
            .unwrap_or(false)
    );

    assert!(sdk.crdt_get_document("t1", "thing_markdown")?.is_some());

    drop(sdk);

    let sdk = TriggerSdk::initialize(&db_path).context("re-init sdk")?;
    let reloaded = parse_snapshot(&sdk, device_id)?;
    assert_eq!(reloaded["collections"].as_array().unwrap().len(), 3);
    assert_eq!(reloaded["things"].as_array().unwrap().len(), 1);
    assert_eq!(
        sdk.things_get_thing_markdown(device_id, "t1")?,
        Some("hello world".to_string())
    );

    Ok(())
}

#[test]
fn delete_thing_archives_to_trash_and_keeps_content_after_reload() -> Result<()> {
    let db_path = temp_db_path()?;
    let sdk = TriggerSdk::initialize(&db_path).context("init sdk")?;
    let device_id = "device-a";

    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "c1".to_string(),
            title: "Inbox".to_string(),
            collection_type: Default::default(),
            app_id: None,
            trigger_uuid: None,
            trigger_uuid_patch: Default::default(),
            created_at: None,
            updated_at: None,
        },
    )?;
    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: "t1".to_string(),
            title: "Thing 1".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(serde_json::json!({"markdown": "hello world"})),
            collection_uuid: "c1".to_string(),
            trigger_uuid: None,
            trigger_uuid_patch: Default::default(),
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )?;

    assert!(sdk.things_delete_thing(device_id, "c1", "t1")?);

    let after = parse_snapshot(&sdk, device_id)?;
    assert_eq!(after["things"].as_array().unwrap().len(), 1);
    assert_eq!(
        after["things"][0]["collection_uuid"],
        SYSTEM_TRASH_COLLECTION_ID
    );
    assert!(after["things"][0]["archived_at"].as_str().is_some());
    assert_eq!(after["things"][0]["archived_from_collection_uuid"], "c1");
    assert!(sdk.crdt_get_document("t1", "thing_markdown")?.is_some());
    assert_eq!(
        sdk.things_get_thing_markdown(device_id, "t1")?,
        Some("hello world".to_string())
    );

    drop(sdk);

    let sdk = TriggerSdk::initialize(&db_path).context("re-init sdk")?;
    let reloaded = parse_snapshot(&sdk, device_id)?;
    assert_eq!(reloaded["things"].as_array().unwrap().len(), 1);
    assert_eq!(
        reloaded["things"][0]["collection_uuid"],
        SYSTEM_TRASH_COLLECTION_ID
    );
    assert_eq!(
        sdk.things_get_thing_markdown(device_id, "t1")?,
        Some("hello world".to_string())
    );

    let collection_row = sdk
        .crdt_get_document("c1", "collection")?
        .context("collection document should exist")?;
    let collection_view = extract_collection_doc_view(&collection_row.automerge_doc, "c1")?;
    let thing = collection_view
        .things
        .iter()
        .find(|thing| thing.id == "t1")
        .context("source thing metadata should remain tombstoned after archive move")?;
    assert!(thing.tombstone.as_ref().map(|t| t.deleted).unwrap_or(false));

    Ok(())
}

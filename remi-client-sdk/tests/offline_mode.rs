use anyhow::{Context, Result};
use remi_client_sdk::RemiSdk;
use remi_client_sdk::things_crdt::{ThingCollectionUpsert, ThingDatatype, ThingUpsert};

#[test]
fn local_things_work_without_configuring_remote_transport() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("tempdir")?;
    let db_path = temp_dir.path().join("offline-mode.sqlite3");
    let sdk = RemiSdk::initialize(&db_path).context("init sdk")?;
    let device_id = "offline-device";

    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "offline-inbox".to_string(),
            title: "Offline Inbox".to_string(),
            collection_type: Default::default(),
            app_id: None,
            created_at: None,
            updated_at: None,
        },
    )?;
    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: "offline-note".to_string(),
            title: "Local note".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(serde_json::json!({ "markdown": "runs without a server" })),
            collection_uuid: "offline-inbox".to_string(),
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )?;

    let snapshot = sdk.things_list_snapshot(device_id)?;
    assert_eq!(snapshot.collections.len(), 1);
    assert_eq!(snapshot.collections[0].uuid, "offline-inbox");
    assert_eq!(snapshot.things.len(), 1);
    assert_eq!(snapshot.things[0].uuid, "offline-note");
    assert_eq!(
        sdk.things_get_thing_markdown(device_id, "offline-note")?,
        Some("runs without a server".to_string())
    );
    assert!(sdk.things_has_pending_changes(device_id)?);

    Ok(())
}

use remi_client_sdk::RemiSdk;
use remi_client_sdk::things_crdt::{ThingCollectionUpsert, ThingDatatype, ThingUpsert};
use serde_json::json;

#[test]
fn things_status_write_rejects_unknown_status() {
    let temp = tempfile::tempdir().unwrap();
    let sdk = RemiSdk::initialize(temp.path().join("status.sqlite")).unwrap();
    let device_id = "device-status";

    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "collection-status".to_string(),
            title: "Status".to_string(),
            collection_type: Default::default(),
            app_id: None,
            created_at: None,
            updated_at: None,
        },
    )
    .unwrap();
    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: "thing-status".to_string(),
            title: "Status Thing".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(json!({"markdown": ""})),
            collection_uuid: "collection-status".to_string(),
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )
    .unwrap();

    let err = sdk
        .things_set_status(device_id, "thing-status", "blocked", None)
        .unwrap_err();
    assert!(err.to_string().contains("Invalid thing status"));

    sdk.things_set_status(device_id, "thing-status", "done", None)
        .unwrap();
    let snapshot = sdk.things_list_snapshot_lite(device_id).unwrap();
    let thing = snapshot
        .things
        .iter()
        .find(|thing| thing.uuid == "thing-status")
        .unwrap();
    assert_eq!(thing.status, "done");
}

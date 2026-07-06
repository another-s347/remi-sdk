use serde::{Deserialize, Serialize};

use crate::{view::ThingStatusView, ThingDatatype};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectionRow {
    pub id: String,
    pub title: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThingRow {
    pub id: String,
    pub collection_id: String,
    pub datatype: ThingDatatype,
    pub status: ThingStatusView,
    pub title: Option<String>,
    pub parent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct MaterializePlan {
    pub upsert_collections: Vec<CollectionRow>,
    pub delete_collections: Vec<String>,
    pub upsert_things: Vec<ThingRow>,
    pub delete_things: Vec<String>,
}

pub fn materialize_plan(view: &View) -> MaterializePlan {
    let mut plan = MaterializePlan::default();

    for c in &view.collections {
        if c.tombstone.as_ref().map(|t| t.deleted).unwrap_or(false) {
            plan.delete_collections.push(c.id.clone());
        } else {
            plan.upsert_collections.push(CollectionRow {
                id: c.id.clone(),
                title: c.title.clone(),
                status: c.status.clone(),
            });
        }
    }

    for t in &view.things {
        if t.tombstone.as_ref().map(|x| x.deleted).unwrap_or(false) {
            plan.delete_things.push(t.id.clone());
        } else {
            plan.upsert_things.push(ThingRow {
                id: t.id.clone(),
                collection_id: t.collection_id.clone(),
                datatype: t.datatype.clone(),
                status: t.status.clone(),
                title: t.title.clone(),
                parent_id: t.parent_id.clone(),
            });
        }
    }

    plan
}

pub type View = crate::view::View;

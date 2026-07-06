pub mod compaction;
pub mod datatype;
pub mod domain;
pub mod extract;
pub mod markdown;
pub mod materialize;
pub mod ops;
pub mod schema;
pub mod util;
pub mod view;

pub use datatype::{
    generate_uuid,
    // V3 multi-value content entries
    ContentEntry,
    ContentEntryKind,
    ContentEntryPayload,
    ContentEntryUpdate,
    CrdtDataType,
    DateField,
    ImageField,
    JsonObjectField,
    LocationField,
    ThingBuiltInFields,
    ThingBuiltInFieldsUpdate,
    ThingDatatype,
    UrlField,
    ROOT_DOC_UUID,
};
pub use domain::{
    format_domain_datetime, parse_domain_datetime, parse_domain_datetime_or_unix_epoch,
    parse_optional_domain_datetime, CollectionId, CollectionType, ContentEntryId, FieldPatch,
    Thing, ThingCollection, ThingCollectionEntry, ThingCollectionUpsert, ThingEntry, ThingId,
    ThingStatus, ThingUpsert, ThingsChangeLogEntry, ThingsContentSnapshot, ThingsMutationEvent,
    ThingsOperationType, ThingsSnapshot, ThingsSnapshotState, ThingsSyncSummary,
    ThingsUndoConflict, ThingsUndoConflictType, ThingsUndoExecution, ThingsUndoPreview,
    ThingsUndoResolutionOption,
};
pub use extract::{
    extract_collection_doc_view,
    extract_collection_doc_view_from_doc,
    // V3 extraction functions
    extract_root_view,
    extract_root_view_from_doc,
    extract_thing_content_view,
    extract_thing_content_view_from_doc,
    extract_thing_markdown,
    extract_thing_markdown_from_doc,
    extract_thing_markdown_view,
    extract_thing_markdown_view_from_doc,
    extract_view,
    extract_view_from_doc,
    extract_view_from_doc_with_options,
    extract_view_with_options_and_scale,
    extract_view_with_scale,
    DocScale,
    ExtractOptions,
};
pub use markdown::{decode_markdown_only_content, decode_markdown_only_thing, MarkdownOnlyDecoded};
pub use materialize::{CollectionRow, MaterializePlan, ThingRow, View};
pub use ops::{apply_op, Block, Content, Op};
// V3 operations
pub use ops::{
    apply_collection_op, apply_root_op, apply_thing_markdown_op, CollectionOp, RootOp,
    ThingMarkdownOp,
};
pub use schema::{Schema, CURRENT_SCHEMA_VERSION};
// V3 views
pub use view::{
    CollectionDocView, CollectionMetaView, RootView, ThingBuiltInFieldsView, ThingContentView,
    ThingMarkdownView, ThingMetaView,
};
// V3 compaction
pub use compaction::{
    compact_collection_doc, compact_root_doc, compact_thing_content_doc,
    compact_thing_markdown_doc, needs_compaction, DEFAULT_COMPACTION_THRESHOLD,
};

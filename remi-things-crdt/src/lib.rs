pub mod compaction;
pub mod datatype;
pub mod extract;
pub mod markdown;
pub mod materialize;
pub mod ops;
pub mod schema;
pub mod util;
pub mod view;

pub use datatype::{
	CrdtDataType, DateField, ImageField, JsonObjectField, LocationField, UrlField, ThingBuiltInFields, ThingBuiltInFieldsUpdate, ThingDatatype, ROOT_DOC_UUID,
    // V3 multi-value content entries
    ContentEntry, ContentEntryKind, ContentEntryPayload, ContentEntryUpdate, generate_uuid,
};
pub use extract::{
	DocScale, ExtractOptions, extract_view, extract_view_from_doc, extract_view_from_doc_with_options,
	extract_thing_markdown, extract_thing_markdown_from_doc, extract_view_with_options_and_scale, extract_view_with_scale,
	// V3 extraction functions
	extract_root_view, extract_root_view_from_doc, extract_collection_doc_view, extract_collection_doc_view_from_doc,
	extract_thing_content_view, extract_thing_content_view_from_doc, extract_thing_markdown_view, extract_thing_markdown_view_from_doc,
};
pub use markdown::{decode_markdown_only_content, decode_markdown_only_thing, MarkdownOnlyDecoded};
pub use materialize::{BindingRow, CollectionRow, MaterializePlan, ThingRow, View};
pub use ops::{apply_op, Block, Content, Op, TriggerUpdate};
// V3 operations
pub use ops::{apply_root_op, apply_collection_op, apply_thing_markdown_op, RootOp, CollectionOp, ThingMarkdownOp};
pub use schema::{Schema, CURRENT_SCHEMA_VERSION};
// V3 views
pub use view::{RootView, CollectionDocView, CollectionMetaView, ThingBuiltInFieldsView, ThingContentView, ThingMarkdownView, ThingMetaView};
// V3 compaction
pub use compaction::{
    needs_compaction, DEFAULT_COMPACTION_THRESHOLD,
	compact_root_doc, compact_collection_doc, compact_thing_content_doc, compact_thing_markdown_doc,
};

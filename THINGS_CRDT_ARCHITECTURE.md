# Things / ThingCollection CRDT 架构总览

## 配套文档

当前 Things 文档分成三份，分别回答不同层级的问题：

- `THINGS_CRDT_ARCHITECTURE.md`：当前这份，聚焦整体分层、职责边界、同步模型和扩展方向
- `THINGS_SCHEMA_REFERENCE.md`：聚焦 root / collection / thing_markdown / content entry 的 schema 与字段语义
- `THINGS_API_AND_MIGRATION.md`：聚焦 `TriggerSdk` 的正式用法，以及旧 JSON API 到 typed API 的迁移

## 文档目标

这份文档聚焦当前 Things / ThingCollection 系统的正式架构，不以历史兼容实现作为主线。

目标是回答下面几个问题：

- 现在 Things 的 CRDT 架构到底是什么
- 各层分别负责什么，边界在哪里
- 当前支持哪些能力
- 未来应该如何扩展

API 使用与旧 JSON API 迁移细节已拆到 `THINGS_API_AND_MIGRATION.md`。

本文对应的当前实现主要位于：

- `remi-client-sdk`
- `remi-things-crdt`

其中：

- `remi-client-sdk` 是应用层实际依赖的主入口
- `remi-things-crdt` 是底层 CRDT schema、操作、视图提取和压缩库

## 一句话架构

当前 Things 系统是一个本地优先、基于 Automerge 的多文档 CRDT 架构：

- 一个 root 文档负责发现 collection
- 每个 collection 一个 collection 文档，负责 collection 元数据和该 collection 下所有 thing 元数据
- 每个需要协作文本内容的 thing 额外拥有一个 markdown 文档
- 应用通过 typed `TriggerSdk` API 读写
- 本地以 SQLite 持久化多份 CRDT 文档
- 同步时以文档为单位推拉，而不是把整个 things 树当成一个大 blob

## 设计目标

当前设计优先解决这些问题：

- 本地优先写入，不依赖服务端往返
- 多设备并发编辑最终收敛
- 用 typed API 代替 JSON string + 手写解析
- 把 metadata 编辑和 markdown 文本编辑拆开，降低冲突面
- UI 和 agent 可以只读轻量 snapshot，而不是总把全文内容物化
- 为新 content 类型和未来新的文档族保留扩展空间

## 非目标

当前设计没有把所有业务状态都强行塞进 CRDT：

- 变更日志不是 CRDT，本地 SQL 维护即可
- actor attribution 不是 CRDT，来自服务端缓存回填
- 某些迁移、诊断和 undo 辅助状态也不是 CRDT

换句话说，CRDT 只承载真正需要跨设备收敛的 Things 领域状态。

## 分层总览

从下到上可以分为 6 层。

### 1. 存储层

主要位于 `remi-client-sdk/src/storage.rs`。

职责：

- 把 CRDT 文档持久化到 SQLite
- 持久化文档 sync state 和 dirty 标记
- 维护 trigger binding、change log、content snapshot、actor meta 等辅助表

### 2. 核心 CRDT 定义层

主要位于 `remi-things-crdt`。

职责：

- 定义文档类型 `CrdtDataType`
- 定义根 schema、collection schema、markdown schema
- 定义 typed operation 和 view extraction
- 提供 compact / extract / schema helper

### 3. 文档图领域层

主要位于 `remi-client-sdk/src/things_crdt.rs`。

职责：

- 把多份文档管理成一个逻辑上的 Things 状态图
- 封装跨文档领域规则
- 从多文档抽取 typed snapshot
- 提供 collection / thing / markdown / content entry 级别的高层操作

### 4. SDK 应用服务层

主要位于 `remi-client-sdk/src/runtime.rs`。

职责：

- 暴露给应用使用的 `TriggerSdk` API
- 把业务调用转成 `ThingsDocumentSet` 操作
- 保存 dirty 文档并在必要时压缩
- 发送 UI 事件
- 写入 change log 和 content snapshot

### 5. 同步层

主要位于 `remi-client-sdk/src/things_sync.rs`。

职责：

- 以文档为单位推送本地变更
- 接收服务端已有文档或对端改动
- 做首次同步 bootstrap，避免本地自动初始化文档 fork 掉服务端 canonical state
- 用 reachability 规则过滤该不该拉、该不该接收

### 6. 应用边界层

主要是这些消费者：

- desktop Tauri commands
- mobile Rust bridge / UniFFI / JNI wrappers
- CLI
- 测试和场景脚本

职责：

- 调用 typed SDK API
- 只在边界层做 JSON 序列化
- 不把 schema 细节复制到应用层

## 为什么是多文档，而不是单文档

当前设计明确放弃了“一个大 Automerge 文档表示整个 Things 世界”的模型。

原因很直接：不同类型的数据变化频率、同步粒度和读取方式完全不同。

### Root 文档

root 文档很小，只负责 collection 发现。

它的核心职责：

- 标记当前有哪些 collection 应该被看见
- 在同步顺序上先于 collection 文档
- 让缺失文档拉取知道有哪些 collection 应该存在

它不是所有业务数据的容器。

### Collection 文档

每个 collection 一个文档，持有：

- collection 元数据
- collection 下所有 thing 元数据
- thing 的 built-in structured content entries
- thing / collection 级 trigger binding
- tombstone 和 edit_clock 等领域元数据

重要点：collection 文档不承载大段 markdown 正文。

这样做的好处：

- 纯 metadata 更新不会把全文带上
- 列表页或 agent 摘要可以不加载正文
- 文本协作与元数据协作的冲突域被隔离

### Thing Markdown 文档

每个需要协作文本文档的 thing，可以有一个 `ThingMarkdown` 文档。

它承载：

- `thing_uuid`
- `content_type`
- markdown blocks
- block 内部的 Automerge text

这样正文编辑可以独立于 collection 文档发生，文本冲突也限定在自己的文档内部。

## 存储层设计

### 1. CRDT 文档主表

当前 Things 的正式持久化载体是 SQLite 表 `crdt_documents`：

```sql
CREATE TABLE crdt_documents (
    uuid TEXT NOT NULL,
    data_type TEXT NOT NULL,
    automerge_doc BLOB NOT NULL,
    sync_state BLOB NOT NULL,
    dirty INTEGER NOT NULL DEFAULT 0,
    last_sync_at TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (uuid, data_type)
)
```

每一行就是一份 CRDT 文档，由 `(uuid, data_type)` 唯一标识。

其中：

- `automerge_doc` 是文档本体
- `sync_state` 是与服务端同步协议相关的状态
- `dirty` 表示本地是否还有未推送变更
- `last_sync_at` 记录该文档上次成功同步时间

### 2. 辅助表

当前 Things 还有一些围绕 CRDT 工作的辅助表。

#### `trigger_bindings`

这是 SQL 索引表，不是 collection / thing trigger 绑定的 canonical source of truth。

正式状态仍在 CRDT 文档里，SQL 表用于：

- 快速查询绑定关系
- 清理和兼容逻辑

#### `things_change_log`

本地变更日志，用于：

- undo / redo 体系
- 操作审计和诊断
- 合并短时间重复编辑

#### `things_content_snapshots`

保存和 change log 关联的内容快照，用于：

- 恢复
- 调试
- 删除/编辑回放

#### `things_actor_meta`

服务端同步完成后回填的 actor attribution 缓存。

它不写进 CRDT，本地 snapshot 抽取后再叠加，方便 UI 直接展示 `actor_type`、`actor_app_id`、`actor_display_name`。

### 3. 旧表 `things_state`

`things_state` 仍在 schema 中保留，但当前 Things 的正式实现已经使用 `crdt_documents` 作为 canonical store。

理解当前系统时，不应该再把 `things_state` 视为主路径。

## 核心 CRDT 定义

### 文档类型 `CrdtDataType`

当前正式的文档族是：

- `Root`
- `Collection`
- `ThingMarkdown`

它同时定义同步优先级：

- root = 0
- collection = 1
- thing_markdown = 2

同步必须遵守这个顺序。

### Root schema

可以把 root 理解成一个轻量 discovery index：

```text
{
  schema_version: 3,
  epoch: 0,
  collection_uuids: []
}
```

root 不负责承载 thing 数据。

### Collection schema

collection 文档可以概括成：

```text
{
  schema_version: 3,
  meta: {
    id,
    title,
    status,
    edit_clock,
    tombstone?,
    trigger?,
    attrs?
  },
  things: {
    <thing_uuid>: {
      id,
      datatype,
      status,
      edit_clock,
      tombstone?,
      title?,
      parent_id?,
      trigger?,
      built_in,
      attrs?
    }
  }
}
```

collection 文档是 thing metadata 的真正归属地。

### ThingMarkdown schema

正文文档可以概括成：

```text
{
  schema_version: 3,
  document_uuid,
  thing_uuid,
  content_type: "markdown",
  content: {
    kind: "markdown",
    blocks: [
      {
        id,
        type,
        attrs_json?,
        text?
      }
    ]
  }
}
```

这里才是 `splice_text` 一类编辑操作直接作用的地方。

## 数据结构层

### Snapshot 返回类型

应用层读取 Things 时，正式读模型是 `ThingsSnapshotState`：

```rust
pub struct ThingsSnapshotState {
    pub collections: Vec<ThingCollectionEntry>,
    pub things: Vec<ThingEntry>,
    pub dirty: bool,
    pub last_sync_at: Option<String>,
}
```

这已经是应用层可用的 typed 结构，不需要应用自己再解析 Automerge。

### Collection / Thing entry

`ThingCollectionEntry` 和 `ThingEntry` 是 UI / agent / app 最常用的数据结构。

其中 `ThingEntry` 里最关键的几个字段是：

- `datatype`
- `data`
- `collection_uuid`
- `trigger_uuid`
- `parent_uuid`
- `status`
- actor attribution 字段

### Upsert 输入类型

当前正式写接口输入是：

- `ThingCollectionUpsert`
- `ThingUpsert`

这两者替代了旧时代的 JSON string payload。

### ContentEntry 体系

Thing 的结构化内建内容用 `ContentEntry` 表示：

```rust
pub struct ContentEntry {
    pub id: String,
    pub title: Option<String>,
    pub order: f64,
    pub payload: ContentEntryPayload,
}
```

#### 为什么 `order` 是 `f64`

因为这样允许在相邻 entry 中间插入，不必每次重排整个列表。

### ContentEntryPayload

当前 built-in payload 包括：

- `Markdown { doc_uuid }`
- `Url(UrlField)`
- `Location(LocationField)`
- `Date(DateField)`
- `Image(ImageField)`
- `Custom { content_type, data }`

这里有一个很重要的设计点：

- `ThingDatatype` 是 thing 级语义标签
- `ContentEntryPayload` 是 thing 内真正承载结构化内容的单元

所以一个 thing 可以是“task”语义，但内部同时携带 date、location、url 等 entry。

### ContentTypeRegistry

`ContentTypeRegistry` 是当前结构化内容扩展的核心枢纽。

它负责：

- 把边界层 JSON 解析成 typed `ContentEntryPayload`
- 把 typed payload 序列化回边界 JSON 形状
- 把 `ThingBuiltInFieldsView` materialize 成 snapshot `data`
- 从 snapshot `data` 反解 markdown / built-in entries
- 支持 `Custom` payload 的无损 round-trip

如果你要扩展新内容类型，基本都会碰到它。

## 领域层：`ThingsDocumentSet`

当前 Things 领域模型的核心对象是 `ThingsDocumentSet`。

可以把它理解成：一个设备上一整组 Things 相关文档的内存工作集。

概念结构：

```text
ThingsDocumentSet {
  device_id,
  documents: HashMap<DocumentKey, DocumentState>
}
```

### `DocumentKey`

通过下面两个维度唯一标识一份文档：

- `uuid`
- `CrdtDataType`

例如：

- root: `(ROOT_DOC_UUID, Root)`
- 某 collection: `(<collection_uuid>, Collection)`
- 某 thing markdown: `(<thing_uuid>, ThingMarkdown)`

### `DocumentState`

每个文档的运行时状态包括：

- `automerge_doc`
- `sync_state`
- `dirty`
- `last_sync_at`

### `ThingsDocumentSet` 的职责

它是“跨文档规则”的真正落点。典型职责包括：

- 初始化 root
- 创建 / 获取 collection 文档
- 在 root 里增加 / 删除 collection 引用
- 在 collection 文档里 upsert thing metadata
- 解析、添加、更新、删除 content entries
- 定位一个 thing 属于哪个 collection
- 对 markdown 文档做 splice / replace
- 提取全局 snapshot
- 计算 active collection / active thing reachability
- 在保存前做必要 compaction

## 领域不变量

### 1. Root 是发现索引，不是唯一真相

root 应该引用 live collection，但实现不会无脑依赖 root。

例如 `find_thing_collection_uuid` 可以直接扫描 collection 文档，而不是假设 root 一定完整。

这么做的原因是：

- root 和 collection linkage 可能短暂不一致
- bootstrap / sync / migration 时可能出现 root 暂时滞后

因此当前设计是“优先使用 root 做发现，但领域逻辑保留跨文档兜底”。

### 2. 删除采用 tombstone，而不是立即物理抹除

当前删除语义是：

- 删除 collection: 从 root 移除引用，同时 tombstone collection 文档
- 删除 thing: tombstone thing metadata
- markdown 文档本地可以保留，不再被 reachability 视为 live

这是一个分布式一致性选择，目的不是“节省空间”，而是避免多端同步时 resurrection bug。

### 3. Reachability 决定业务可见性

文档存在不代表用户可见。

只有满足“通过 live collection reachable”的 collection / thing，才会进入 snapshot 和后续同步可见性判断。

这也是为什么：

- tombstoned collection 下的 thing 会从 snapshot 消失
- 孤立 markdown 文档不会单独冒出来变成一个 live thing

### 4. Parent 关系受约束

thing parent 关系会校验，避免：

- self-parenting
- cycle
- 指向不存在的父节点

### 5. Metadata 与文本编辑分离

title / status / parent / trigger / built-in entries 属于 collection 文档。

正文文本编辑属于 `ThingMarkdown` 文档。

这是当前设计稳定性和可扩展性的关键。

## 快照抽取模型

应用层不直接读取文档字节，而是通过多文档抽取 typed snapshot。

抽取过程逻辑上会遍历：

1. root 文档
2. live collection 文档
3. 如果需要，再抽取对应 markdown 文档内容

当前正式读 API：

- `things_list_snapshot(device_id)`
- `things_list_snapshot_lite(device_id)`
- `things_list_snapshot_with_options(device_id, include_things, SnapshotOptions)`

### 为什么有 lite 模式

`things_list_snapshot_lite` 会省略 `data.content`，适合：

- 列表页
- agent 轻量摘要
- 同步状态检查
- 不需要正文的 UI

这样可以显著减少大 markdown 内容的物化成本。

## SDK 应用层：`TriggerSdk`

### 为什么 `TriggerSdk` 是正式入口

对于桌面端、移动端、CLI、本地 agent 来说，Things 的正式入口是 `TriggerSdk`。

原因：

- 它持有本地 SQLite
- 它负责文档集初始化和持久化
- 它负责事件广播
- 它负责 change log / snapshot 这些应用体验相关能力
- 它才符合 local-first 模型

`ThingsClient` 或远程 CRUD 客户端不应该被当成主业务入口。

### 当前正式读 API

```rust
let snapshot = sdk.things_list_snapshot(device_id)?;
let lite = sdk.things_list_snapshot_lite(device_id)?;
let has_pending = sdk.things_has_pending_changes(device_id)?;
```

### Collection API

```rust
use remi_client_sdk::things_crdt::ThingCollectionUpsert;

sdk.things_upsert_collection(
    device_id,
    ThingCollectionUpsert {
        uuid: collection_uuid.clone(),
        title: "Inbox".to_string(),
        trigger_uuid: None,
        created_at: None,
        updated_at: None,
    },
)?;

sdk.things_delete_collection(device_id, &collection_uuid)?;
```

### Thing API

```rust
use remi_client_sdk::things_crdt::{ThingDatatype, ThingUpsert};

sdk.things_upsert_thing(
    device_id,
    ThingUpsert {
        uuid: thing_uuid.clone(),
        title: "Buy milk".to_string(),
        datatype: ThingDatatype::Markdown,
        data: None,
        collection_uuid: collection_uuid.clone(),
        trigger_uuid: None,
        parent_uuid: None,
        created_at: None,
        updated_at: None,
    },
)?;

sdk.things_delete_thing(device_id, &collection_uuid, &thing_uuid)?;
sdk.set_thing_status(device_id, &thing_uuid, "done")?;
```

### Content entry API

结构化字段应该走 content entry API，而不是塞进任意 JSON。

```rust
use remi_client_sdk::things_crdt::{
    ContentEntry, ContentEntryPayload, DateField, ContentEntryUpdate,
};

let entry_id = sdk.things_add_content_entry(
    device_id,
    &thing_uuid,
    ContentEntry {
        id: uuid::Uuid::new_v4().to_string(),
        title: Some("Due".to_string()),
        order: 0.0,
        payload: ContentEntryPayload::Date(DateField::date_only(1_742_588_800_000)),
    },
)?;

sdk.things_update_content_entry(
    device_id,
    &thing_uuid,
    ContentEntryUpdate {
        id: entry_id,
        title: Some(Some("Due Date".to_string())),
        order: None,
        payload: None,
    },
)?;

let entries = sdk.things_get_content_entries(device_id, &thing_uuid)?;
```

### Markdown / editor API

细粒度文本协作：

```rust
sdk.things_splice_text(device_id, &thing_uuid, "main", 0, 0, "- Buy milk")?;
```

编辑器风格操作：

```rust
let result = sdk.things_edit_content(
    device_id,
    &thing_uuid,
    "append",
    None,
    None,
    None,
    None,
    None,
    None,
    Some("- Buy eggs"),
)?;
```

`things_edit_content` 适合上层 agent / scripting / editor tool，内部再映射为领域操作。

### Trigger 绑定 API

```rust
sdk.things_set_collection_trigger_uuid(device_id, &collection_uuid, Some(&trigger_uuid))?;
sdk.things_set_thing_trigger_uuid(device_id, &thing_uuid, Some(""))?; // clear
```

这里的 trigger 绑定是 tri-state 语义：

- `None` 表示不改
- 空字符串表示清除
- 非空 UUID 表示设置

### 事件订阅 API

当前建议模式不是轮询，而是：

1. 启动时读一次 snapshot
2. 运行时订阅增量事件

```rust
let rx = sdk.things_subscribe();
```

当前主要事件包括：

- `SnapshotReplaced`
- `DocumentChanged`
- `DataWiped`

其中 `DocumentChanged` 直接映射底层文档变化，按 `document_kind` 区分 `root`、`collection`、`thing`、`thing_markdown`、`content_entry`。

## 同步层设计

### 同步单位

当前同步的最小单位是单个 CRDT 文档，而不是整棵 Things 树。

因此一次同步可能推：

- 一个 root 文档
- N 个 collection 文档
- M 个 markdown 文档

### 同步顺序

dirty 文档必须按优先级推送：

1. root
2. collection
3. thing_markdown

这可以避免“子文档先到、索引后到”的可见性异常。

### 当前同步流程

`things_sync.rs` 中的 v3 流程可以概括为：

1. 判断本地是否是首次同步或脏但无同步历史
2. 必要时先从服务端拉 bootstrap 文档
3. 推送本地 dirty 文档
4. 对本地已有但 head 落后的文档做 receive sync
5. 拉取本地缺失的服务端文档

### 为什么 bootstrap 要特殊处理

如果客户端从未同步过，而本地又自动初始化了 root 文档，直接把它推上去可能会和服务端已有 root fork。

这会导致：

- `collection_uuids` 竞争
- 服务端 canonical state 被错误覆盖或分叉

所以当前实现会在“首次同步”时优先拉取服务端，再把本地未同步的脏变更 stash/replay 回去。

### Reachability filter 在同步中的作用

同步不是“服务端给什么就全都当成 live 状态”。

本地 reachability filter 会参与决定：

- 哪些 clean local doc 应该接收更新
- 哪些 missing doc 应该被拉取
- tombstoned collection 是否应继续让 child thing 可见

它的目的就是防止 ghost resurrection 和 stale markdown 文档误显示。

## 当前功能覆盖

基于当前架构，Things 已经支持：

- 创建 / 更新 / 删除 collection
- 创建 / 更新 / 删除 thing
- parent-child 关系
- thing status
- collection / thing trigger 绑定
- markdown block / text CRDT 编辑
- typed content entries
- snapshot 抽取和 lite snapshot
- UI 增量事件流
- 多文档同步和首次同步 bootstrap
- actor attribution 回填
- 本地 change log / content snapshot / undo 支撑

## 扩展方式

当前最常见的扩展有两类。

在当前实现里，真正高频的未来扩展其实可以拆成三种：

- 给 collection 或 thing 新增一个正式字段
- 扩展 collection / thing 的状态值
- 给 content entry 增加新的 payload 类型

先判断你要做的是哪一类，再决定修改面。

### 扩展方向 0：给 collection 或 thing 新增字段

适用场景：

- 想给 collection 增加新的业务字段，例如 `color`、`icon`、`archived_reason`
- 想给 thing 增加新的 metadata 字段，例如 `priority`、`assignee`、`due_mode`
- 这个字段属于结构层，而不是正文内容

推荐判断顺序：

1. 这个字段是否会影响 collection / thing 的业务可见性、排序、过滤或领域规则
2. 这个字段是否需要跨设备收敛
3. 这个字段是否应该出现在 typed snapshot 中

如果三个问题答案都是“是”，它就应该成为正式 schema 字段，而不是长期躲在 `attrs` 或 `extra` 里。

推荐步骤：

1. 先决定字段归属
   - collection 级字段放 collection `meta`
   - thing 级字段放 collection 文档里的 thing metadata
   - 如果只是 entry 自身字段，放 content entry，而不是 collection / thing 顶层
2. 在 `remi-things-crdt` 的 view/schema/materialize 层补字段
3. 在底层 op / apply 路径补写入逻辑
4. 在 `remi-client-sdk::things_crdt` 的 typed snapshot 结构中决定是否暴露该字段
5. 在 `ThingCollectionUpsert` / `ThingUpsert` 或专门 update API 中暴露 typed 输入
6. 在边界层按需补序列化/反序列化
7. 增加 snapshot round-trip、sync merge、delete/reachability 不回归测试

一个重要原则：

- 会参与领域语义的字段，尽量做成 first-class typed field
- 只是短期附加信息的字段，才考虑先放 `attrs` / `extra`

### 扩展方向 0.1：给 collection / thing 增加新的状态值

当前 `thing.status` 已经是显式领域字段，未来 collection status 或 thing status 扩展时，不应该只改 UI 文案。

推荐步骤：

1. 先决定状态是否只是展示态，还是会影响业务规则
2. 统一更新状态的 canonical enum/string 映射点
3. 更新 snapshot materialize
4. 更新所有状态校验入口
5. 更新同步、筛选、事件和测试

具体来说：

- 如果只是扩展 thing status 的允许值：
  - 更新 `remi-things-crdt` 中对应状态表示
  - 更新 `runtime.rs` 里的校验逻辑，例如 `set_thing_status`
  - 更新依赖该状态的 UI 和测试
- 如果新增 collection status：
  - 先确认 collection status 是否只是 metadata，还是会影响可见性/归档/同步策略
  - 若会影响读路径，就必须把规则同时落到 snapshot / filtering 层

不要只在某一层硬编码一个新字符串，否则很容易出现：

- 写得进去，但 snapshot 不认
- snapshot 认了，但 UI 过滤没更新
- 本地可用，但同步后的另一端没有同样语义

### 扩展方向 A：增加新的结构化内容类型

适用场景：

- 数据是 metadata-like
- 不需要单独的协作文档
- 更像 thing 的一个内建结构化字段，而不是独立正文

例子：

- contact card
- audio reference
- checklist summary
- app-specific embed metadata

建议步骤：

1. 在 `remi-things-crdt` 中为 `ContentEntryPayload` 增加 variant
2. 如需要，补充对应字段结构体
3. 在 `ContentTypeRegistry` 中实现 parse / serialize
4. 更新边界层构造逻辑
5. 增加 round-trip 和 snapshot extraction 测试

如果这个新类型不仅要“存”，还要“被产品用起来”，通常还需要继续补：

6. 在 UI / automation / agent 层补该类型的 typed builder 或 helper
7. 在 `THINGS_SCHEMA_REFERENCE.md` 里补 payload 形状示例
8. 在 `THINGS_API_AND_MIGRATION.md` 里补使用示例，确保调用方不会回退到手写 JSON

如果只是试验性扩展，或者暂时不想改所有 schema surface，可以先使用：

```rust
ContentEntryPayload::Custom {
    content_type: "your_type".to_string(),
    data: json!(...),
}
```

这条路径可以保证：

- 本地 typed 模型仍然成立
- 同步可用
- 不必先把每种业务扩展都硬编码进内核

### 扩展方向 B：增加新的内容文档族

适用场景：

- 新内容本身也需要协作
- 体量可能较大
- 不适合继续塞进 collection 文档
- 有独立同步/压缩/抽取需求

例如未来如果要支持：

- 富文本文档
- 白板/画布
- 大型 checklist with fine-grained edits
- 富媒体注释文档

推荐步骤：

1. 在 `remi-things-crdt` 增加新的 `CrdtDataType`
2. 增加对应 schema、op、extract、compact
3. 在 `ThingsDocumentSet` 中加入新文档的生命周期管理
4. 更新 snapshot extraction，把它转为 app-level typed data
5. 在同步层增加数据类型映射与优先级
6. 视情况增加新的 typed SDK API

一个经验法则：

- “只是结构化字段”就留在 collection 文档 + content entries
- “有独立协作行为的内容”才值得晋升为单独文档族

### 扩展检查清单

无论你扩的是字段、状态还是 content entry 类型，落地前都建议检查下面几项：

1. schema 层有没有新的 canonical 表达
2. typed snapshot 有没有暴露或明确选择不暴露
3. typed write API 有没有新入口或字段
4. sync merge 之后另一端是否还能正确 materialize
5. tombstone / reachability 规则是否仍成立
6. 边界层是不是仍然只做序列化，而没有把 schema 逻辑复制出去

## 应用如何使用当前 API

### 典型写路径

典型本地优先写路径：

1. 应用调用 `TriggerSdk`
2. SDK 加载或初始化 `ThingsDocumentSet`
3. 领域层修改对应文档
4. 保存 dirty 文档到 `crdt_documents`
5. 发出 ThingsEvent
6. 后续由同步层与服务端收敛

### 典型读路径

典型读路径：

1. UI 启动时读取 `things_list_snapshot` 或 `things_list_snapshot_lite`
2. UI 保留本地 store
3. 运行时订阅 `things_subscribe`
4. 根据增量事件 patch UI

### 建议的 API 选择

如果你是应用开发者，建议直接遵守下面的选择规则：

- 读列表、读聚合状态：`things_list_snapshot_lite`
- 需要正文：`things_list_snapshot` 或 `things_get_thing_markdown`
- 改 collection / thing 元数据：typed upsert API
- 改结构化字段：content entry API
- 改 markdown 正文：`things_splice_text` 或 `things_edit_content`
- 做同步：使用 `things_sync.rs` 的 v3 同步流程，不要自己拼文档同步顺序

## 从旧 JSON API 迁移

### 迁移原则

如果你还在使用旧 JSON API，迁移目标不是“继续用 JSON 拼 typed payload”，而是：

- 应用内部直接使用 typed struct
- 只有边界层在需要时序列化成 JSON
- 不再把 SDK 当作一个收 JSON string 的黑盒

### API 对照表

| 旧 API | 当前推荐 API | 说明 |
| --- | --- | --- |
| `things_list_snapshot_json(device_id)` | `things_list_snapshot(device_id)` | 返回 `ThingsSnapshotState` |
| `things_list_snapshot_json_lite(device_id)` | `things_list_snapshot_lite(device_id)` | 返回 lite typed snapshot |
| `things_list_snapshot_json_with_options(...)` | `things_list_snapshot_with_options(...)` | 仍然 typed |
| `things_upsert_collection_json(device_id, payload_json)` | `things_upsert_collection(device_id, ThingCollectionUpsert)` | typed upsert |
| `things_upsert_thing_json(device_id, payload_json)` | `things_upsert_thing(device_id, ThingUpsert)` | typed upsert |
| `things_get_location(device_id, thing_uuid)` | `things_get_content_entries(...)` + `ContentTypeRegistry::find_first_payload_by_kind(...)` | location/date 不再是专用 SDK getter |
| `things_get_date(device_id, thing_uuid)` | 同上 | 统一通过 content entries 获取 |
| 手写 JSON block / payload | `ContentEntry` / `ContentEntryUpdate` | 用 typed payload 替代 |

### 迁移前后的代码对比

#### 旧写法

```rust
use serde_json::json;

sdk.things_upsert_thing_json(
    device_id,
    &json!({
        "uuid": thing_uuid,
        "title": "Buy milk",
        "datatype": "markdown",
        "data": { "markdown": "- Buy milk" },
        "collection_uuid": collection_uuid,
    }).to_string(),
)?;
```

#### 新写法

```rust
use remi_client_sdk::things_crdt::{ThingDatatype, ThingUpsert};

sdk.things_upsert_thing(
    device_id,
    ThingUpsert {
        uuid: thing_uuid,
        title: "Buy milk".to_string(),
        datatype: ThingDatatype::Markdown,
        data: Some(serde_json::json!({ "markdown": "- Buy milk" })),
        collection_uuid,
        trigger_uuid: None,
        parent_uuid: None,
        created_at: None,
        updated_at: None,
    },
)?;
```

#### 旧读取快照写法

```rust
let snapshot_json = sdk.things_list_snapshot_json(device_id)?;
let snapshot: serde_json::Value = serde_json::from_str(&snapshot_json)?;
```

#### 新读取快照写法

```rust
let snapshot = sdk.things_list_snapshot(device_id)?;
for thing in snapshot.things {
    println!("{} {}", thing.uuid, thing.title);
}
```

### 对边界层的建议

如果你维护的是：

- UniFFI
- JNI bridge wrapper
- Tauri command
- CLI JSON 输出

那么正确做法是：

1. 内部先使用 typed SDK API
2. 最后一层再 `serde_json::to_value` 或 `serde_json::to_string`

不要继续把 JSON 解析逻辑下沉回 SDK 核心。

### 迁移顺序建议

如果你在一个旧模块里仍然大量使用 JSON API，建议按这个顺序迁移：

1. 先把 snapshot 读取迁到 typed API
2. 再把 collection/thing upsert 改成 typed input
3. 再把 location/date 等专用 getter 改成 content entry 读取
4. 最后再清理边界层的 JSON helper

原因是：

- snapshot typed 化收益最大
- 写路径 typed 化能消掉最多 schema 漂移
- 专用 getter 清理要建立在 content entry 心智模型已经稳定之后

## 实践建议

### 对应用层

- 不要直接依赖底层 `remi-things-crdt`，除非你真的在写 editor / low-level tooling
- 优先用 `TriggerSdk`
- UI 默认先用 lite snapshot，正文按需取

### 对领域扩展

- 优先扩展 `ContentEntryPayload`
- 只有当内容本身需要独立 CRDT 行为时，才新建文档族

### 对同步实现

- 不要绕过 `things_sync.rs` 自己决定推送顺序
- 不要把“文档存在”误当成“业务上可见”

### 对边界封装

- JSON 应该只出现在边界层
- typed 结构应该在 SDK 内部一路保持到边界最后一层

## 总结

当前 Things / ThingCollection CRDT 设计的核心思想可以概括为三句话：

- 以多文档而不是单文档来隔离 discovery、metadata 和正文协作
- 以 `ThingsDocumentSet` 管理跨文档领域规则，以 `TriggerSdk` 暴露正式应用 API
- 以 typed snapshot 和 typed input 取代旧 JSON API，把 JSON 退回到边界层

如果你从今天开始接入或扩展 Things，建议直接以这套模型为准，而不是再围绕旧 JSON surface 设计新的调用方式。

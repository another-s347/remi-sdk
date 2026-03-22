# Things CRDT Schema 参考

## 适用范围

这份文档补充 `THINGS_CRDT_ARCHITECTURE.md`，专门回答更底层的问题：

- 当前多文档 schema 各自长什么样
- 哪些字段是结构性字段，哪些只是展示或辅助字段
- collection / thing / content entry / markdown block 分别如何映射
- 如果以后扩展新的文档族，应该参考什么模式

这里使用的是“概念 schema + 当前实现约束”的写法，不试图逐字逐节复制 Automerge 内部存储细节。

## 文档族总表

当前正式文档族：

- `root`
- `collection`
- `thing_markdown`

对应底层 `CrdtDataType`：

- `Root`
- `Collection`
- `ThingMarkdown`

同步优先级：

1. `root`
2. `collection`
3. `thing_markdown`

## 1. Root 文档

### 角色

root 是 collection discovery index，不是业务真相本体。

它的职责：

- 列出当前被发现的 collection UUID
- 在同步顺序上先于 collection 文档
- 为“缺失文档拉取”提供目录

### 概念 schema

```text
{
  schema_version: 3,
  epoch: 0,
  collection_uuids: [
    "coll-1",
    "coll-2"
  ]
}
```

### 字段说明

- `schema_version`
  - 当前 schema 版本
  - 用于未来迁移与兼容检查

- `epoch`
  - 预留的根级版本/时代字段
  - 当前一般保持为 `0`

- `collection_uuids`
  - root 的核心数据
  - 表示发现到的 collection 集合
  - 不是最终业务可见性的唯一依据

### 重要约束

- root 残留 UUID 不应让 tombstoned collection 重新变活
- root 缺少某个 collection UUID，也不应该让 live collection 永远不可见
- 当前实现会在 snapshot / lookup 时保留跨文档兜底，而不是完全迷信 root

## 2. Collection 文档

### 角色

collection 文档是 collection 和其下所有 thing metadata 的正式权威层。

这是当前 Things 系统里最重要的结构文档。

### 概念 schema

```text
{
  schema_version: 3,
  meta: {
    id: "coll-1",
    title: "Inbox",
    status: "none",
    edit_clock: {
      actor: "device-a",
      counter: 12
    },
    tombstone: {
      deleted: false,
      clock: { ... }
    },
    trigger: {
      state: "some",
      uuid: "trigger-1",
      clock: { ... }
    },
    attrs: { ... }
  },
  things: {
    "thing-1": {
      id: "thing-1",
      datatype: "markdown",
      status: "in-progress",
      edit_clock: { ... },
      tombstone: {
        deleted: false,
        clock: { ... }
      },
      title: "Buy milk",
      parent_id: null,
      trigger: {
        state: "none",
        uuid: null,
        clock: { ... }
      },
      built_in: {
        content_entries: [ ... ],
        extra: null
      },
      attrs: { ... }
    }
  }
}
```

### Collection `meta` 字段

- `id`
  - collection UUID

- `title`
  - collection 展示名

- `status`
  - 当前 collection 级状态字段
  - 目前不是最常见 UI 主路径，但 schema 预留在这里

- `edit_clock`
  - 语义级编辑时钟
  - 辅助高层冲突语义，而不是只靠底层 Automerge merge

- `tombstone`
  - 标记 collection 是否已删除
  - collection 一旦 tombstone，业务上整个 collection 与 child thing 都应隐藏

- `trigger`
  - collection 级 trigger binding
  - 采用 tri-state 语义

- `attrs`
  - 扩展属性
  - 当前不建议把结构性核心字段偷偷放进这里

### Thing metadata 字段

每个 thing 的 metadata 存在 collection 文档下。

- `id`
  - thing UUID

- `datatype`
  - thing 的语义类型，例如 `markdown`
  - 不是内容本体，只是 thing 级语义标签

- `status`
  - `none` / `in-progress` / `stalled` / `done`

- `edit_clock`
  - 该 thing metadata 的语义时钟

- `tombstone`
  - thing 删除标记

- `title`
  - 标题

- `parent_id`
  - 父 thing UUID
  - 用于层级结构

- `trigger`
  - thing 级 trigger binding

- `built_in`
  - thing 的结构化 built-in 内容容器

- `attrs`
  - 扩展属性

## 3. Built-in fields 与 content entries

### built_in 容器

当前 thing 的结构化 built-in 数据可以概括成：

```text
built_in: {
  content_entries: Vec<ContentEntry>,
  extra: Option<serde_json::Value>
}
```

### `content_entries`

这是当前结构化内容扩展的主入口。

每个 entry 概念上是：

```text
{
  id: "entry-1",
  title: "Due",
  order: 0.0,
  payload: {
    type: "date",
    ...
  }
}
```

字段说明：

- `id`
  - entry UUID
  - 用于后续精确更新/删除

- `title`
  - entry 的可选标题

- `order`
  - 排序键
  - 使用 `f64`，以便中间插入

- `payload`
  - 真正的内容类型和值

### `extra`

`extra` 是保底逃生口。

适用于：

- 临时业务元数据
- 还没有正式提升为 first-class schema 的附加字段

但不建议把核心结构规则长期依赖在 `extra` 里，否则会失去 typed schema 的价值。

## 4. ContentEntryPayload 参考

### Markdown payload

```text
{
  type: "markdown",
  doc_uuid: "thing-1"
}
```

它并不直接持有正文，而是指向 markdown 文档。

### Url payload

```text
{
  type: "url",
  url: "https://example.com",
  title: "Example",
  description: "...",
  image_url: "...",
  favicon_url: "...",
  site_name: "Example",
  resolved: true
}
```

### Location payload

当前支持两类：

#### Coordinate

```text
{
  type: "location",
  loc_type: "coordinate",
  lat: 39.9,
  lng: 116.4,
  coord_system: "wgs84",
  source_name: "gps"
}
```

#### Fuzzy

```text
{
  type: "location",
  loc_type: "fuzzy",
  name: "Home",
  place_type: "residential"
}
```

### Date payload

```text
{
  type: "date",
  timestamp_ms: 1742588800000,
  has_time: false,
  timezone: "Asia/Shanghai"
}
```

### Image payload

```text
{
  type: "image",
  uri: "file:///...",
  caption: "receipt",
  width: 1080,
  height: 720,
  size_bytes: 123456,
  device_id: "device-a"
}
```

### Custom payload

两种常见形状：

#### 显式 custom wrapper

```text
{
  type: "custom",
  data: { ... }
}
```

#### 命名扩展类型

```text
{
  type: "contact_card",
  name: "Skye",
  phone: "..."
}
```

`ContentTypeRegistry` 会对这两种形式做无损 round-trip。

## 5. ThingMarkdown 文档

### 角色

`thing_markdown` 文档承载需要协作文本语义的正文内容。

它不决定 thing 是否存在，也不决定它属于哪个 collection。

### 概念 schema

```text
{
  schema_version: 3,
  document_uuid: "thing-1",
  thing_uuid: "thing-1",
  content_type: "markdown",
  content: {
    kind: "markdown",
    blocks: [
      {
        id: "main",
        type: "markdown",
        attrs_json: null,
        text: "- Buy milk"
      }
    ]
  }
}
```

### block 字段

- `id`
  - block 标识，例如 `main`

- `type`
  - block 类型，当前最常见是 `markdown`

- `attrs_json`
  - block 级扩展属性

- `text`
  - block 文本本体
  - 内部通过 Automerge text 协作

### 为什么正文不放在 collection 文档里

因为正文编辑有三个明显特征：

- 频率更高
- 体积更大
- 需要更细粒度协作

把它从 collection 文档拆出来可以显著降低 metadata 更新成本和冲突复杂度。

## 6. Snapshot 视图与 schema 的映射

### `ThingCollectionEntry`

来自 collection `meta` 的 materialized 视图。

典型映射：

- `uuid` <- `meta.id`
- `title` <- `meta.title`
- `trigger_uuid` <- `meta.trigger`
- actor attribution <- 非 CRDT cache 叠加

### `ThingEntry`

来自 collection 文档中的 thing metadata，再与 markdown / built-in 数据拼接。

典型映射：

- `uuid` <- `thing.id`
- `title` <- `thing.title`
- `datatype` <- `thing.datatype`
- `collection_uuid` <- 所属 collection UUID
- `trigger_uuid` <- `thing.trigger`
- `parent_uuid` <- `thing.parent_id`
- `status` <- `thing.status`
- `data.built_in` <- `built_in.content_entries` 和 `extra`
- `data.content` <- 对应 markdown 文档物化内容

## 7. reachability 与 tombstone 规则

### collection tombstone

当 collection tombstoned：

- collection 本身不应出现在 snapshot
- 其下 thing 也不应出现在 snapshot
- 即便 root 仍保留该 UUID，也不应重建业务可见性

### thing tombstone

当 thing tombstoned：

- 该 thing 不应出现在 snapshot
- 它的 content entries 不应再被当作 live 数据
- markdown 文档可以本地保留，但不代表业务可见

### root 不一致

当 root 落后或不完整：

- snapshot / 直接 lookup 仍允许依赖 collection 文档兜底
- root 的角色是索引，不是最终 truth

## 8. 新文档族扩展参考

在新增完整文档族之前，更常见的需求其实是新增字段或新增 payload 类型。下面给出当前 schema 层的推荐做法。

### 8.1 给 collection 新增字段

如果字段属于 collection 自身的结构语义，例如：

- `color`
- `icon`
- `archived_at`
- `visibility`

优先放在 collection `meta`，而不是散落到：

- root
- 某个 thing
- 边界层 JSON 派生字段

推荐规则：

- 影响 collection 语义或 UI 主路径的字段，放 `meta`
- 仅短期透传的附加字段，才考虑 `attrs`

### 8.2 给 thing 新增字段

如果字段是 thing metadata，例如：

- `priority`
- `effort`
- `source`
- `review_state`

优先放在 collection 文档中的 thing metadata。

原因：

- thing 是否存在、属于哪个 collection、状态和 built-in 结构都已经在这里
- 继续把结构层聚拢在 collection 文档中，能保持 snapshot 逻辑一致

不推荐把这类字段直接塞到 markdown 文档里，除非它真的是正文内容的一部分。

### 8.3 扩展状态字段

当前 `thing.status` 已经是正式字段，因此新增状态值时需要把它视为 schema 变更，而不只是业务常量变更。

建议检查：

- snapshot materialize 是否能稳定输出新值
- 读路径是否把新值当作未知值过滤掉
- UI / agent / automation 是否有假设状态只来自旧集合

如果未来 collection 也要发展成更复杂的状态机，同样应走 `meta.status` 这一路，而不是新增旁路字段去表达“归档”“冻结”等状态。

### 8.4 新增 content entry 类型

新增 content entry 类型时，schema 层至少要明确三个问题：

1. 它的 payload 形状是什么
2. 它是 built-in entry，还是未来应该晋升为独立文档族
3. 它的最小稳定字段集合是什么

建议新增类型时，在这份文档中补一段 payload 参考，和现有 `url` / `location` / `date` / `image` 保持相同风格。

如果未来新增如 `ThingRichText`、`ThingCanvas` 之类的新文档族，建议遵守与 `ThingMarkdown` 相同的原则：

- collection 文档继续维护结构归属和 entry 索引
- 新文档只承载独立内容本体
- snapshot 提取层再把内容 materialize 到 `ThingEntry.data` 或新的 typed 读模型中
- 同步层为新文档族增加数据类型映射和优先级

一个实用判断标准：

- 如果只是 metadata-like 内容，继续用 `content_entries`
- 如果需要独立协作内容历史，再晋升为独立文档族

## 9. 扩展落地建议

如果你准备扩 schema，建议按下面顺序做设计确认：

1. 先决定新信息属于 root、collection、thing metadata、content entry 还是独立内容文档
2. 再决定它是不是正式 schema 字段，而不是临时 `attrs` / `extra`
3. 最后才去改边界层或 UI

如果顺序反过来，通常会得到一个“先能传 JSON，再慢慢补内核”的不稳定设计；这正是当前体系要避免回到的旧路。

## 小结

当前 Things schema 可以浓缩为一句话：

root 负责发现，collection 负责结构和可见性，thing_markdown 负责正文内容，snapshot 负责把这些层组合成应用真正使用的 typed 状态。
# Things API 使用与旧 JSON API 迁移

## 适用范围

这份文档面向直接使用 `remi-client-sdk` 的应用层开发者。

重点回答三个问题：

- 当前 Things 正式 API 应该怎么用
- 不同类型的读写应该走哪一组接口
- 旧 JSON API 应该如何迁移到 typed API

如果你想先理解整体设计，再看这份文档，建议先读：

- `THINGS_CRDT_ARCHITECTURE.md`
- `THINGS_SCHEMA_REFERENCE.md`

## 先建立正确心智模型

应用层的正式入口是 `TriggerSdk`，不是直接操作底层 CRDT 文档，也不是把 Things 当成一个远程 CRUD 服务。

标准工作流是：

1. 本地通过 typed `TriggerSdk` API 读写
2. UI 通过 snapshot + event stream 维护状态
3. 同步层在后台把 dirty 文档与服务端收敛

这意味着：

- 应用内部应该尽量直接使用 typed struct
- JSON 只保留在边界层，例如 CLI 输出、Tauri command 返回值、UniFFI / JNI bridge wrapper

## 当前推荐 API 地图

### 读路径

#### 读完整 snapshot

```rust
let snapshot = sdk.things_list_snapshot(device_id)?;
```

用途：

- 需要正文内容
- 需要完整 `data.content`
- 适合详情页或完整导出

#### 读 lite snapshot

```rust
let snapshot = sdk.things_list_snapshot_lite(device_id)?;
```

用途：

- 列表页
- 只关心 title / status / collection / trigger / built-in entries
- agent 轻量摘要
- 判断本地是否有数据、是否有 pending change

#### 自定义读取选项

```rust
let snapshot = sdk.things_list_snapshot_with_options(
    device_id,
    true,
    remi_client_sdk::things_crdt::SnapshotOptions {
        include_content: false,
    },
)?;
```

用途：

- 你明确知道不想要正文
- 想和 interrupt handler / tool layer 一致地控制提取成本

#### 读单个 thing 的 markdown 正文

```rust
let markdown = sdk.things_get_thing_markdown(device_id, &thing_uuid)?;
```

适合正文按需加载，而不是每次都拉 full snapshot。

### 写 collection

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
```

删除 collection：

```rust
sdk.things_delete_collection(device_id, &collection_uuid)?;
```

### 写 thing metadata

```rust
use remi_client_sdk::things_crdt::{ThingDatatype, ThingUpsert};

sdk.things_upsert_thing(
    device_id,
    ThingUpsert {
        uuid: thing_uuid.clone(),
        title: "Buy milk".to_string(),
        datatype: ThingDatatype::Markdown,
        data: Some(serde_json::json!({ "markdown": "- Buy milk" })),
        collection_uuid: collection_uuid.clone(),
        trigger_uuid: None,
        parent_uuid: None,
        created_at: None,
        updated_at: None,
    },
)?;
```

删除 thing：

```rust
sdk.things_delete_thing(device_id, &collection_uuid, &thing_uuid)?;
```

### 更新 status

```rust
sdk.set_thing_status(device_id, &thing_uuid, "done")?;
```

或兼容调用：

```rust
sdk.things_set_status(device_id, &thing_uuid, "done", None)?;
```

推荐直接优先使用更 typed 的状态接口。

### 操作结构化 content entries

```rust
use remi_client_sdk::things_crdt::{
    ContentEntry, ContentEntryPayload, ContentEntryUpdate, DateField,
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
        id: entry_id.clone(),
        title: Some(Some("Due Date".to_string())),
        order: None,
        payload: None,
    },
)?;

let entries = sdk.things_get_content_entries(device_id, &thing_uuid)?;

sdk.things_delete_content_entry(device_id, &thing_uuid, &entry_id)?;
```

这组接口适用于：

- date
- location
- url
- image
- custom payload

这类内容都不应该再通过应用层手写 JSON blob 直接塞进 thing。

### 操作 markdown 正文

#### 细粒度 CRDT splice

```rust
sdk.things_splice_text(device_id, &thing_uuid, "main", 0, 0, "- Buy milk")?;
```

适合：

- 编辑器
- 逐步插入
- 保留更细粒度协作语义

#### 编辑器风格高层 API

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

支持的 operation 当前包括：

- `overwrite`
- `set_title`
- `str_replace`
- `insert_at_line`
- `append`

适合：

- agent tool
- CLI / script
- 文本自动化修改

### 操作 trigger 绑定

```rust
sdk.things_set_collection_trigger_uuid(device_id, &collection_uuid, Some(&trigger_uuid))?;
sdk.things_set_thing_trigger_uuid(device_id, &thing_uuid, Some(""))?; // clear
```

trigger 绑定采用 tri-state 语义：

- `None`: 不改
- 空字符串: 清除
- 非空 UUID: 设置

### 事件订阅

```rust
let rx = sdk.things_subscribe();
```

推荐用法：

1. 启动时读一次 snapshot
2. 运行时订阅 ThingsEvent
3. 本地 store 按 `DocumentChanged` 或 `SnapshotReplaced` 做刷新/局部更新

当前常用事件：

- `SnapshotReplaced`
- `DocumentChanged`
- `DataWiped`

`DocumentChanged` 会暴露：

- `document_kind`: `root` / `collection` / `thing` / `thing_markdown` / `content_entry`
- `change_kind`: `created` / `updated` / `deleted`
- `document_uuid`
- 以及必要的 `collection_uuid`、`thing_uuid`、`entry_id`

## 典型应用模式

### 模式 1：普通 UI

建议：

- 首页 / 列表页用 `things_list_snapshot_lite`
- 点进详情再取 markdown 内容
- 运行期只依赖 event stream 做 patch

### 模式 2：CLI / Tauri / UniFFI 边界

建议：

1. 内部先拿 typed struct
2. 边界最后一层再 `serde_json::to_value` 或 `serde_json::to_string`

不要继续让 SDK 核心 API 接收 JSON string。

### 模式 3：Agent / Automation

建议：

- 如果是结构化字段操作，走 content entry API
- 如果是正文文本改写，优先 `things_edit_content`
- 如果是精细协作编辑，走 `things_splice_text`

## 旧 JSON API 迁移

### 迁移目标

迁移的目标不是“把旧 JSON 继续包一层”，而是：

- 让 SDK 核心层统一使用 typed input / output
- 让 JSON 只存在于边界层
- 让 schema 变化只影响 typed struct，而不是散落在各处字符串解析逻辑里

### API 对照表

| 旧 API | 当前推荐 API | 说明 |
| --- | --- | --- |
| `things_list_snapshot_json(device_id)` | `things_list_snapshot(device_id)` | 返回 `ThingsSnapshotState` |
| `things_list_snapshot_json_lite(device_id)` | `things_list_snapshot_lite(device_id)` | 返回 lite typed snapshot |
| `things_list_snapshot_json_with_options(...)` | `things_list_snapshot_with_options(...)` | 仍是 typed 返回 |
| `things_upsert_collection_json(device_id, payload_json)` | `things_upsert_collection(device_id, ThingCollectionUpsert)` | typed upsert |
| `things_upsert_thing_json(device_id, payload_json)` | `things_upsert_thing(device_id, ThingUpsert)` | typed upsert |
| `things_get_location(device_id, thing_uuid)` | `things_get_content_entries(...)` + `ContentTypeRegistry::find_first_payload_by_kind(...)` | location/date 不再走专用 getter |
| `things_get_date(device_id, thing_uuid)` | 同上 | 统一通过 content entries 读取 |
| 手写 JSON payload | `ContentEntry` / `ContentEntryUpdate` / `ThingUpsert` | 优先 typed 输入 |

### 代码迁移示例

#### 旧写法：把 JSON string 传给 SDK

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

#### 新写法：直接传 typed struct

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

#### 旧写法：拿 JSON snapshot 再手动 parse

```rust
let snapshot_json = sdk.things_list_snapshot_json(device_id)?;
let snapshot: serde_json::Value = serde_json::from_str(&snapshot_json)?;
```

#### 新写法：直接用 typed snapshot

```rust
let snapshot = sdk.things_list_snapshot(device_id)?;
for thing in snapshot.things {
    println!("{} {}", thing.uuid, thing.title);
}
```

### 专用 location/date getter 的迁移

旧设计里，location / date 常被当成专用 getter。

当前推荐方式是：

1. 先取 content entries
2. 再按 payload kind 过滤

例如：

```rust
use remi_client_sdk::things_crdt::{ContentEntryKind, ContentTypeRegistry};

let entries = sdk.things_get_content_entries(device_id, &thing_uuid)?;
let payload = ContentTypeRegistry::new()
    .find_first_payload_by_kind(&entries, &ContentEntryKind::Location);
```

这使 location、date、image、url、custom payload 都回到同一套内容模型里，而不是每种类型都发明一组独立 getter。

### 迁移顺序建议

建议按下面顺序迁移老代码：

1. 先把 snapshot 读取迁到 typed API
2. 再把 collection / thing upsert 迁到 typed input
3. 再把 location/date 专用 getter 改成 content entry 读取
4. 最后清理边界层 JSON helper

原因：

- snapshot typed 化收益最大
- typed write path 最能减少 schema 漂移
- location/date 等专用 getter 的清理需要团队已经接受 content entry 心智模型

## 对 client-demo / Tauri / mobile bridge 的建议

如果你维护这些边界模块，建议遵守同一个模式：

1. 内部统一用 typed SDK API
2. 边界层再序列化
3. 不要把 JSON helper 重新引回 `TriggerSdk`

例如：

- CLI 输出 JSON：在 action handler 最后 `serde_json::to_string_pretty(&snapshot)`
- Tauri command 返回 JSON：在 command 层 `serde_json::to_value(snapshot)`
- UniFFI / JNI 兼容 wrapper：内部 typed，外层只做临时 JSON 兼容

## 未来扩展时 API 应该怎么演进

如果未来要扩 Things，调用层最容易犯的错是“内核还没定，先让边界层 JSON 透传”。当前推荐反过来做。

### 给 collection / thing 新增字段

推荐顺序：

1. 先在 typed domain struct 中加字段，例如 `ThingCollectionUpsert`、`ThingUpsert`、`ThingCollectionEntry`、`ThingEntry`
2. 再让 `TriggerSdk` 暴露这些字段
3. 最后才更新 Tauri / CLI / UniFFI / JNI 的边界映射

这样能保证：

- 新字段是 SDK 正式能力，而不是边界 hack
- 测试可以直接针对 typed API 写
- JSON 边界只负责序列化，不负责解释 schema

### 扩展状态值

如果新增状态值，不要单独发明一个新 API。优先复用现有 typed API，让新状态值沿现有字段流动：

- thing status 继续走 `set_thing_status` / `things_set_status`
- collection status 未来如果变成正式字段，也应走 collection typed update，而不是平行 API

如果状态扩展需要额外行为，例如：

- 进入归档时隐藏
- 进入冻结时禁止编辑

应把行为写进领域规则，而不是让调用方靠约定自己判断。

### 新增 content entry 类型

推荐顺序：

1. 先扩 `ContentEntryPayload`
2. 再扩 `ContentTypeRegistry`
3. 再决定是否需要额外 helper builder 或 UI 层 draft 类型
4. 最后再补边界层 record / JSON 形状

如果调用方一开始就只能通过手写 JSON 才能构造新类型，说明扩展还没完成。

### 对移动端和桌面边界的建议

未来扩展时，下面这些层只应该做映射，不应该决定 schema：

- Kotlin `ThingCollectionUpsertInput` / `ThingUpsertInput`
- UniFFI record
- FRB wrapper
- Tauri command DTO
- CLI action JSON

它们应该跟着 SDK typed surface 走，而不是各自长出一套独立字段语义。

## 小结

对当前系统来说，最重要的迁移原则只有一句：

应用内部一律面向 typed Things API 编程，JSON 只存在于边界层。

如果继续沿用旧 JSON API 的思路，后面每扩展一种 content 类型、每新增一种文档族，代价都会重新回到字符串解析和 schema 漂移上。
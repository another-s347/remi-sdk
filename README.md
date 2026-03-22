# remi-sdk

`remi-sdk` 是 Remi 客户端侧 Rust 能力的独立工作区仓库，面向桌面端、移动端、CLI 以及其他需要接入 Remi 协议的应用。

这个仓库当前包含 3 个核心 crate：

- `remi-client-sdk`：应用直接依赖的主 SDK，负责传输、认证、聊天、Triggers、Things、Telemetry、Profile、应用 API Key 等能力。
- `remi-things-crdt`：Things 的底层 CRDT 数据模型与文档操作库，适合需要直接操作 Automerge 文档的场景。
- `rule-trigger-engine`：基于 CEL 的规则触发引擎，负责 trigger rule 的解析、校验、时间提取与条件评估。

另外，仓库根目录的 `proto/` 是一个 Git submodule，用来提供共享 protobuf 定义。

Things / ThingCollection 当前文档分为三份：

- `THINGS_CRDT_ARCHITECTURE.md`：整体架构、分层、同步与扩展
- `THINGS_SCHEMA_REFERENCE.md`：root / collection / markdown / content entry 的 schema 参考
- `THINGS_API_AND_MIGRATION.md`：typed API 使用方式与旧 JSON API 迁移说明

## 适用场景

如果你正在做下面这些事情，通常应该从这个仓库开始：

- 在 Rust 应用里接入 Remi 的认证、聊天、Things、Triggers、Telemetry。
- 在本地保存事件、触发器和 Things 数据，并与服务器做同步。
- 在移动端或桌面端复用 Remi 的聊天运行时，而不是自己维护 SSE / gRPC 流式状态机。
- 直接操作 Things 的 CRDT 文档，构建自定义编辑器、同步器或调试工具。
- 在服务端或工具链里复用 trigger rule 的 CEL 求值能力。

如果你只是做普通客户端集成，绝大多数情况只需要依赖 `remi-client-sdk`。

## 仓库结构

```text
remi-sdk/
├─ Cargo.toml
├─ proto/                    # protobuf 定义，git submodule
├─ remi-client-sdk/          # 主 SDK
├─ remi-things-crdt/         # Things CRDT 底层库
└─ rule-trigger-engine/      # Trigger rule 引擎
```

### `remi-client-sdk` 提供的主要能力

`remi-client-sdk` 的公开 API 大致分成下面几类：

- 传输层：`transport`，支持共享连接、复用 gRPC channel，以及 decenet / TCP 两种模式。
- 认证：`AuthClient`，支持登录、注册、登出、refresh token、恢复持久化 session。
- 本地运行时：`TriggerSdk`，负责本地 SQLite 存储、事件记录、trigger 注册与调度、Things 本地状态与广播通知。
- 远程客户端：`ThingsClient`、`TriggerClient`、`ChatClient`、`ProfileClient`、`AppKeysClient`。
- 聊天运行时：`ChatRuntime`，封装了长生命周期聊天 actor、流式消息缓存、interrupt 处理和 UI 订阅。
- Telemetry：`telemetry` 模块，负责上报监控事件。
- 地理位置与 URI：`LocationService`、`RemiUri`、媒体 MIME 推断等。
- Things 辅助：`things_sync`、`things_handlers`、`things_events`、`things_crdt`、`things_crdt_v2`。

### `remi-things-crdt` 提供的主要能力

这个 crate 更底层，不依赖网络。它主要暴露：

- 文档类型与内置字段：`CrdtDataType`、`ThingDatatype`、`ThingBuiltInFields`。
- 文档操作：`apply_root_op`、`apply_collection_op`、`apply_thing_markdown_op`。
- 视图提取：`extract_root_view`、`extract_collection_doc_view`、`extract_thing_markdown_view`。
- 文档压缩：`compact_root_doc`、`compact_collection_doc`、`compact_thing_markdown_doc`。
- UUID / schema / markdown 相关辅助函数。

适合以下场景：

- 只想操作 Things CRDT 文档，不想引入完整 SDK。
- 需要自己实现同步层、调试工具或数据迁移工具。
- 需要对 Automerge 文档做更细粒度的分析或回放。

### `rule-trigger-engine` 提供的主要能力

这个 crate 提供 trigger 配置的解析与求值：

- `TriggerConfig`：完整 trigger 配置。
- `Rule`：单条 CEL 规则。
- `EvaluationContext`：运行时求值上下文。
- `PreconditionPolicy`：precondition 的 gate 策略。
- `TriggerTiming`、`RepeatFrequency`：时序提取结果。
- `TriggerEvaluationReport`：详细求值报告。

它支持的典型 helper 包括：

- 事件类：`event_count(minutes, event_type)`、`event_exists(minutes, event_type)`、`event_exists_with_message(minutes, event_type, substring)`。
- 时间类：`in_time_range(start, end)`、`is_weekday([...])`、`current_hour()`。
- precondition 时序类：`cron(expr)`、`location_change()`、`network_change()`、`repeat_per_day(n)`、`repeat_per_week(n)`。

## 安装与依赖方式

目前这个仓库更适合通过 Git 依赖接入，而不是 crates.io。

### 下游应用依赖 `remi-client-sdk`

```toml
[dependencies]
remi-client-sdk = { git = "https://github.com/another-s347/remi-sdk.git", rev = "<pin-a-commit>", package = "remi-client-sdk" }
```

### 需要直接用 Things CRDT

```toml
[dependencies]
remi-things-crdt = { git = "https://github.com/another-s347/remi-sdk.git", rev = "<pin-a-commit>", package = "remi-things-crdt" }
```

### 需要直接用 Trigger 引擎

```toml
[dependencies]
rule-trigger-engine = { git = "https://github.com/another-s347/remi-sdk.git", rev = "<pin-a-commit>", package = "rule-trigger-engine" }
```

建议下游仓库固定 `rev`，不要直接跟随分支头部，避免协议或内部实现变化导致不可预期升级。

## 克隆与构建

由于仓库根目录包含 `proto/` submodule，并且部分依赖来自私有 git 仓库，建议这样获取代码：

```bash
git clone --recurse-submodules https://github.com/another-s347/remi-sdk.git
cd remi-sdk
git submodule update --init --recursive
```

如果你的环境对 Cargo 直连 git 不稳定，建议开启：

```bash
CARGO_NET_GIT_FETCH_WITH_CLI=true
```

仓库的 CI 也是按整个 workspace 统一构建和测试：

```bash
cargo build --workspace --verbose
cargo test --workspace --verbose
```

如果你没有对应私有依赖的访问权限，构建会失败，这不是 SDK 本身的代码错误。

## 共享传输模型

`remi-client-sdk` 的多数网络能力都建立在共享传输之上。典型做法是：

1. 先构造 transport 配置 JSON。
2. 调用 `configure_shared_transport` 建立共享连接。
3. 基于共享 transport 创建 `AuthClient` / `ProfileClient`。
4. 其他客户端通过 `new_with_shared_transport(...)` 复用同一条连接。

### 支持的传输模式

#### 1. TCP gRPC

适合开发、测试或部署在普通内网 / 容器环境里的场景。

关键字段：

- `transportMode = "tcp"`
- `tcpGrpcAddr = "host:port"`
- `requestTimeoutMs`
- `connectTimeoutMs`

#### 2. decenet

适合使用 Remi 的去中心化网络栈时。

关键字段：

- `endpoint`
- `localVirtualAddr`
- `remoteVirtualAddr`
- `localUdpBind`
- `remoteUdpAddr`
- `encryption`
- `introAttempts`
- `introRetryMs`
- `keyFile`

### 共享传输初始化示例

```rust
use remi_client_sdk::transport::configure_shared_transport;

let transport_config = serde_json::json!({
    "transportMode": "tcp",
    "tcpGrpcAddr": "127.0.0.1:50051",
    "requestTimeoutMs": 20000,
    "connectTimeoutMs": 5000
})
.to_string();

let transport = configure_shared_transport(&transport_config).await?;
```

## 常见使用方式

### 核心模型：Collection / Thing / Trigger

如果你主要关心 Remi 的数据组织方式，先记住这 3 个概念：

- Collection：顶层容器，通常对应一个列表、项目、分类或工作区。一个 collection 有自己的 `uuid`、`title`，也可以直接绑定一个 `trigger_uuid`。
- Thing：collection 内的具体条目。一个 thing 必须属于某个 collection，可以通过 `parent_uuid` 形成树状层级，还可以有自己的 `status`、`datatype`、`trigger_uuid`。
- Trigger：一份可复用的规则配置，包含 `precondition` 和 `condition`。trigger 本身不等于某个具体 thing，它可以被绑定到 collection 或 thing 上。

对大多数应用来说，关系可以理解为：

- 一个 collection 包含多个 things。
- 一个 thing 只属于一个 collection。
- 一个 trigger 可以绑定到 collection，也可以绑定到 thing。
- 删除 collection 时，里面的 things 会级联删除；删除父 thing 时，子 things 也会级联删除。

在当前 v3 CRDT 架构里，Things 数据不是放在一个单文档里，而是拆成了 3 类文档：

- Root document：记录当前用户拥有的 collection UUID 集合。
- Collection document：记录 collection 元数据，以及该 collection 下所有 thing 的元数据。
- ThingMarkdown document：记录单个 thing 的正文内容块，适合富文本 / markdown 增量编辑。

这个拆分的直接收益是：

- collection 级结构变化和 thing 内容编辑不会互相放大冲突。
- 同步时可以按 Root → Collection → ThingMarkdown 的优先级分批处理。
- UI 可以只拉取轻量 snapshot，而不是每次都把所有正文内容展开。

Trigger 绑定也有一个关键约定：

- UI 面向的权威绑定位置是 collection / thing CRDT 元数据里的 `trigger_uuid`。
- 本地 SQL `trigger_bindings` 表是辅助索引，用来做查找、清理和同步补充，而不是最终展示真相。

这意味着如果你在应用里做“把一个 trigger 绑定到某个 thing”，优先应该调用：

- `things_set_collection_trigger_uuid(...)`
- `things_set_thing_trigger_uuid(...)`

而不是直接操作内部索引表。

### 1. 登录并复用共享连接

这是纯 Rust 集成时最常见的起点。

```rust
use remi_client_sdk::{AuthClient, ProfileClient, ThingsClient, TriggerClient};
use remi_client_sdk::transport::configure_shared_transport;

let transport_config = serde_json::json!({
    "transportMode": "tcp",
    "tcpGrpcAddr": "127.0.0.1:50051",
    "requestTimeoutMs": 20000
})
.to_string();

let transport = configure_shared_transport(&transport_config).await?;

let auth = AuthClient::from_transport(transport.clone());
auth.login("user@example.com".into(), "secret".into()).await?;

let access_token = auth.get_access_token_auto_refresh().await?;

let profile = ProfileClient::from_transport(transport.clone());
let me = profile.get_profile().await?;

let mut things = ThingsClient::new_with_shared_transport(access_token.clone()).await?;
let mut triggers = TriggerClient::new_with_shared_transport(access_token).await?;

let _collections = things.list_collections(50, 0, None).await?;
let _server_triggers = triggers.list_triggers("device-1", None, 50, 0).await?;
```

如果你是通过 Flutter Rust Bridge 或类似桥接层接入，也可以使用 `auth` 模块里的全局辅助函数，例如：

- `configure_auth_client`
- `auth_login`
- `auth_restore_credentials`
- `auth_set_app_key`

这些全局 API 更适合移动端桥接，而不是纯 Rust 服务对象风格。

### 2. 本地 Trigger / Collection / Things 运行时

`TriggerSdk` 是本地优先的数据与事件运行时。它负责：

- 初始化本地 SQLite 存储。
- 记录通用事件。
- 注册 / 列出 / 执行本地 trigger。
- 维护 collection / things 的本地 CRDT 文档。
- 广播 Things / Trigger / Event 更新给 UI 或其他订阅者。
- 管理匿名数据认领、登出清空、本地同步前后的状态刷新。

如果你在做 App 端，这通常才是 collection / things 的主入口，而不是 `ThingsClient`。也就是说：

- 用户编辑 collection / thing：先写本地 `TriggerSdk`。
- UI 展示当前列表：读本地 snapshot。
- 后台同步：再通过 `TriggerClient + things_sync` 把 CRDT 文档推到服务端。

`TriggerSdk` 在 collection / things 这块最重要的能力可以分成 4 类：

- 快照读取：`things_list_snapshot(...)`、`things_list_snapshot_lite(...)`、`things_has_pending_changes(...)`
- 本地写入：`things_upsert_collection_json(...)`、`things_upsert_thing_json(...)`、`things_delete_collection(...)`、`things_delete_thing(...)`、`things_set_status(...)`
- trigger 绑定：`things_set_collection_trigger_uuid(...)`、`things_set_thing_trigger_uuid(...)`、`delete_trigger_and_bindings(...)`
- UI 订阅：`things_subscribe()`、`triggers_subscribe()`、`events_subscribe()`

对应的事件模型也比较明确：

- Things 事件：`SnapshotReplaced`、`DocumentChanged`、`DataWiped`
- Trigger 事件：`TriggerUpsert`、`TriggerDelete`、`TriggerFired`

其中 `DocumentChanged` 会携带 `document_kind`、`change_kind`、`document_uuid` 以及相关的 `collection_uuid` / `thing_uuid` / `entry_id`，让 UI 在运行期直接感知底层文档变化。

这使得 UI 不需要不断全量轮询，可以用“启动时读 snapshot，运行时订阅文档变更事件”的方式维护状态。

#### 本地 collection / things 最小示例

```rust
use remi_client_sdk::TriggerSdk;
use serde_json::json;
use uuid::Uuid;

let sdk = TriggerSdk::initialize("./remi.sqlite3")?;
let device_id = "device-1";

let collection_uuid = Uuid::new_v4().to_string();
let thing_uuid = Uuid::new_v4().to_string();

sdk.things_upsert_collection_json(
    device_id,
    &json!({
        "uuid": collection_uuid,
        "title": "Inbox"
    })
    .to_string(),
)?;

sdk.things_upsert_thing_json(
    device_id,
    &json!({
        "uuid": thing_uuid,
        "title": "Buy milk",
        "datatype": "markdown",
        "data": { "markdown": "- Buy milk" },
        "collection_uuid": collection_uuid
    })
    .to_string(),
)?;

let snapshot = sdk.things_list_snapshot(device_id)?;
println!("{}", serde_json::to_string_pretty(&snapshot)?);
```

这个例子里有两个设计点值得注意：

- collection 是容器；thing 只有在指定 `collection_uuid` 后才是合法的。
- thing 的正文内容和 thing 的元数据在底层是分文档存的，但对上层 API 来说你仍然可以把它当成一个逻辑实体来 upsert。

#### thing 的几个关键字段

- `title`：用户可见标题。
- `datatype`：决定内容是 markdown、text、location、image、todo 还是自定义类型。
- `data`：可选；当你只是改元数据时可以不传。
- `collection_uuid`：必填，决定它属于哪个 collection。
- `parent_uuid`：可选，用来形成子任务 / 子节点层级。
- `trigger_uuid`：可选，用来把 trigger 绑定到这个 thing。
- `status`：运行时状态字段，目前常见值是 `none`、`in-progress`、`stalled`、`done`。

#### collection / things 删除与级联语义

这部分在 README 里单独强调一下，因为它直接影响 UI 和同步设计：

- 删除 collection 会删除该 collection 下的所有 things，并发出 collection + thing 的级联删除事件。
- 删除 thing 会同时删除它的子 things。
- SDK 会记录 Things change log 和内容快照，为 undo / redo / 恢复能力留出基础。

#### trigger 绑定到 collection / thing

```rust
use remi_client_sdk::{NotificationCallback, TriggerRegistration, TriggerRule};
use uuid::Uuid;

let trigger_uuid = Uuid::new_v4().to_string();

sdk.register_trigger(TriggerRegistration {
    trigger_uuid: trigger_uuid.clone(),
    name: "Morning review".into(),
    version: "v1".into(),
    precondition: vec![TriggerRule {
        rule: "cron('0 9 * * *')".into(),
        description: "每天上午 9 点".into(),
    }],
    condition: vec![TriggerRule {
        rule: "event_exists(1440, 'AppOpened')".into(),
        description: "最近 24 小时打开过 App".into(),
    }],
})?;

sdk.things_set_collection_trigger_uuid(device_id, &collection_uuid, Some(&trigger_uuid))?;

let _ = sdk.run_due_triggers(&NotificationCallback)?;
```

实际宿主层通常还会配合：

- 定时器 / scheduler tick 中调用 `next_deadline(...)` 和 `run_due_triggers(...)`
- 网络变化时调用 `schedule_network_change_triggers(...)`
- 位置变化时调用 `schedule_location_change_triggers(...)`

这也是为什么 `TriggerSdk` 不只是 trigger registry，它本质上是本地规则执行运行时。

如果你只想看“注册 trigger + 写入事件”的最小组合，也可以参考下面这个更短的例子：

```rust
use chrono::Utc;
use remi_client_sdk::{EventPayload, TriggerRegistration, TriggerRule, TriggerSdk};
use serde_json::json;
use uuid::Uuid;

let sdk = TriggerSdk::initialize("./remi.sqlite3")?;

sdk.register_trigger(TriggerRegistration {
    trigger_uuid: Uuid::new_v4().to_string(),
    name: "Morning check".into(),
    version: "v1".into(),
    precondition: vec![TriggerRule {
        rule: "cron('0 9 * * *')".into(),
        description: "每天上午 9 点".into(),
    }],
    condition: vec![TriggerRule {
        rule: "event_exists(1440, 'AppOpened')".into(),
        description: "最近 24 小时打开过 App".into(),
    }],
})?;

sdk.record_event(EventPayload {
    event_type: "AppOpened".into(),
    timestamp: Utc::now(),
    metadata: json!({ "source": "desktop" }),
})?;
```

几个重要约定：

- 本地 trigger 调度默认按 `+08:00` 时区计算。
- `cron(...)` 使用 POSIX 5 字段格式：`minute hour day-of-month month day-of-week`。
- day-of-week 取值是 `0-6`，其中 `0 = Sunday`。
- `schedule_network_change_triggers(...)` 和 `schedule_location_change_triggers(...)` 用于把宿主层观察到的网络 / 位置变化映射成 trigger due 事件。
- `run_due_triggers(...)` 会执行当前到期的 trigger，并更新 `next_fire`。
- `run_trigger_now(...)` 适合调试、手动执行或测试场景。

### 3. Things 远程 CRUD

`ThingsClient` 负责直接访问服务端 Things RPC，适合：

- 后台服务或 CLI 直接做远程 CRUD。
- 不需要本地 CRDT 同步，只需要普通请求响应式 Things 操作。
- 做一次性导入、管理后台操作，或者服务端已有权威状态的场景。

如果你在写终端应用或运维工具，这一层很好用；但如果你在写桌面端 / 移动端主应用，通常不应该把它当成 collection / things 的唯一入口。更推荐的分层是：

- 本地编辑与 UI 状态：`TriggerSdk`
- 远端 CRUD 或补充查询：`ThingsClient`
- 同步与 trigger 分发：`TriggerClient`

```rust
use remi_client_sdk::ThingsClient;

let mut client = ThingsClient::new_with_shared_transport(access_token).await?;

let collection = client
    .create_collection("Inbox", None)
    .await?;

let _thing = client
    .create_thing(
        collection.uuid,
        "note",
        r#"{"text":"Buy milk"}"#,
        None,
        Some("Groceries".into()),
        None,
    )
    .await?;
```

这里的 `datatype` 和 `data_json` 由上层应用约定；SDK 本身主要负责传输、存储和同步，不强制绑定你的业务 schema。

`ThingsClient` 这层和本地 CRDT 的一个重要区别是：

- `ThingsClient` 面向 RPC 资源。
- `TriggerSdk` 面向本地实体状态和离线变更。

如果你要做“用户在离线状态下编辑列表，稍后自动同步”，那应该优先围绕 `TriggerSdk` 设计，而不是让 UI 直接依赖 `ThingsClient`。

### 4. Trigger 生命周期、远程发现与同步

`TriggerClient` 负责访问服务端 trigger 能力，包括：

- `list_triggers(...)`
- `download_trigger_rule_config(...)`
- `upload_trigger_rule_config_json(...)`
- `report_trigger_fired(...)`
- CRDT v3 文档同步接口

从完整生命周期看，一个 trigger 通常会经历下面这些阶段：

1. 创建定义：通过 `TriggerSdk::register_trigger(...)` 在本地保存 trigger 定义。
2. 绑定实体：把 `trigger_uuid` 绑定到 collection 或 thing。
3. 本地调度：scheduler 依据 `cron(...)`、`network_change()`、`location_change()` 等 precondition 决定何时到期。
4. 本地执行：`run_due_triggers(...)` 或 `run_trigger_now(...)` 评估条件并产出 `TriggerExecutionSummary`。
5. 跨设备同步：通过 `TriggerClient` 上传 trigger 定义、同步 Things CRDT 文档、向服务端报告 trigger fired 事件。

对 trigger 本身来说，最重要的是分清 `precondition` 和 `condition`：

- `precondition`：决定什么时候该触发调度，或者什么时候一个 trigger 值得进入评估阶段。
- `condition`：决定真正执行时是否满足通知 / 动作条件。

常见写法：

- `precondition` 放 `cron(...)`、`network_change()`、`location_change()`、`repeat_per_day(...)`
- `condition` 放 `event_exists(...)`、`event_exists_with_message(...)`、`in_time_range(...)` 等布尔表达式

如果你把它和 collection / things 放在一起理解，会更自然：

- collection 级 trigger：更适合“这个列表每天要回顾一次”“这个项目在网络变化时检查一次”
- thing 级 trigger：更适合“这个条目在某个条件下提醒我”“这个任务完成前按固定频率催办”

如果你正在做本地优先同步，常见模式是把 `TriggerSdk` 和 `TriggerClient` 组合起来：

```rust
use remi_client_sdk::{things_sync, TriggerClient};

let mut client = TriggerClient::new_with_shared_transport(access_token).await?;

let sync_result = things_sync::sync_v3_documents_with_server(
    &sdk,
    &mut client,
    "device-1",
)
.await?;

println!("synced {} documents", sync_result.documents_synced);
```

这个同步路径会优先处理本地脏文档，再拉取服务端缺失文档，适合多设备 CRDT 状态合并。

这里要特别注意一点：同步的不只是 trigger 定义，也包括 collection / things 的 CRDT 文档状态。所以如果你发现“远端 trigger 已经有了，但本地列表结构不对”，问题常常不在 trigger 本身，而在 collection / thing 文档是否同步完成。

### 5. 聊天能力

聊天相关 API 有两层：

- `ChatClient`：底层流式 RPC 客户端，适合你自己接管事件流。
- `ChatRuntime`：更高层的 actor，适合 UI 应用，已经封装了消息缓存、interrupt、状态广播和自动恢复逻辑。

#### 使用 `ChatRuntime`

```rust
use std::sync::Arc;

use remi_client_sdk::{
    ChatRuntime, ChatRuntimeBackend, ChatRuntimeConfig, TriggerSdk,
};

let sdk = Arc::new(TriggerSdk::initialize("./remi.sqlite3")?);
let runtime = ChatRuntime::start(sdk);

runtime
    .init(
        access_token,
        ChatRuntimeConfig {
            device_id: "device-1".into(),
            request_timeout_secs: 120,
            max_auto_resumes: 8,
            backend: ChatRuntimeBackend::RemoteServer,
        },
    )
    .await?;

let mut events = runtime.subscribe().await;

runtime
    .send_message(
        "session-1".into(),
        "帮我总结今天的活动".into(),
        None,
        None,
        None,
        None,
        None,
    )
    .await?;

let _status = runtime.get_status().await;
let _messages = runtime.get_messages("session-1").await;
```

#### 使用本地 WASM backend

如果你希望把聊天执行放到本地 WASM，可以把 backend 换成：

```rust
use std::path::PathBuf;

use remi_client_sdk::{
    ChatLocalWasmConfig, ChatLocalWasmSource, ChatRuntimeBackend, ChatRuntimeConfig,
};

let config = ChatRuntimeConfig {
    device_id: "device-1".into(),
    request_timeout_secs: 120,
    max_auto_resumes: 8,
    backend: ChatRuntimeBackend::LocalWasm(ChatLocalWasmConfig {
        source: ChatLocalWasmSource::File(PathBuf::from("./remi-agent-rs.wasm")),
        api_key: std::env::var("MOONSHOT_API_KEY")?,
        base_url: None,
        model: None,
    }),
};
```

如果你传入的是原始 `.wasm` 字节并且希望在本地即时编译，需要打开 `local-wasm-compiler` feature。若传入的是预编译产物，则不需要 JIT。

### 6. Telemetry 上报

Telemetry 模块走共享 transport，并复用当前 bearer token：

```rust
use chrono::Utc;
use remi_client_sdk::telemetry::{configure_telemetry_client, send_telemetry_report};

configure_telemetry_client(transport_config.clone()).await?;

let payload = serde_json::json!({
    "deviceId": "device-1",
    "generatedAt": Utc::now().to_rfc3339(),
    "eventCount": 1,
    "events": [
        {
            "type": "AppOpened",
            "timestamp": Utc::now().to_rfc3339(),
            "metadata": { "source": "desktop" }
        }
    ],
    "manual": false,
    "trigger": "startup"
})
.to_string();

let ack_json = send_telemetry_report(payload).await?;
```

### 7. Profile 与媒体上传

`ProfileClient` 提供：

- `get_profile()`
- `update_profile(...)`
- `get_avatar_upload_url(...)`
- `upload_avatar(...)`
- `get_media_upload_url(...)`
- `upload_media(...)`

这部分适合做用户头像、富媒体附件、个人资料等能力。上传时 SDK 会先获取签名 URL，再通过 HTTP PUT 上传文件内容。

### 8. 应用 API Key 管理

如果你在做第三方应用接入、服务到服务调用，通常要关心 `AppKeysClient` 和 `auth` 模块：

- `AppKeysClient::create_application(...)`
- `AppKeysClient::list_applications()`
- `AppKeysClient::create_api_key(...)`
- `AppKeysClient::list_api_keys(...)`
- `AppKeysClient::revoke_api_key(...)`
- `auth::auth_set_app_key(...)`
- `auth::auth_clear_app_key()`

需要注意：

- 应用 API key 以 `remi_app_` 为前缀。
- SDK 业务 RPC 的 bearer token 优先级是：显式 token / SDK app key / 用户 session token。
- 应用管理 RPC 仍然要求用户 JWT，不能直接拿 app key 去创建或撤销应用。

## 什么时候直接用 `remi-things-crdt`

当你不需要完整 SDK，只想操作 Things 文档时，可以直接依赖 `remi-things-crdt`。常见场景：

- 离线导入 / 导出工具。
- CRDT 冲突调试工具。
- 自定义数据可视化或文档检查器。
- 自己实现同步层，但希望复用 Remi 的文档结构。

这个 crate 不负责网络，不负责认证，也不负责服务端 RPC。

## 什么时候直接用 `rule-trigger-engine`

当你需要在别的地方复用同一套 trigger 语义时，直接依赖 `rule-trigger-engine` 比依赖完整 SDK 更轻：

- 服务端校验 trigger 配置是否合法。
- CLI / 测试工具做 trigger dry-run。
- 规则编辑器实时检查 CEL 是否可编译。
- 做 trigger 可解释性展示，输出详细 evaluation report。

### 最小 trigger config 示例

```json
{
  "name": "Morning check",
  "version": "v1",
  "precondition": [
    {
      "rule": "cron('0 9 * * *')",
      "description": "每天上午 9 点"
    }
  ],
  "condition": [
    {
      "rule": "event_exists(1440, 'AppOpened')",
      "description": "最近 24 小时打开过 App"
    }
  ]
}
```

## Feature Flags

`remi-client-sdk` 当前有两个显式 feature：

### `sentry-integration`

启用 SDK 中与 Sentry 相关的集成模块。只有在你的应用已经接入 Sentry，并且确实要把 SDK 侧的异常或状态打通到 Sentry 时再打开。

### `local-wasm-compiler`

为本地聊天 WASM backend 提供原始 `.wasm` 的即时编译能力。只有在你需要本地加载未预编译的 WASM 字节时才需要。

如果你只是使用远程聊天后端，或者使用预编译好的本地产物，一般不需要启用这个 feature。

## Proto 与代码生成

仓库根目录的 `proto/` 是共享 protobuf 定义，不建议手改生成产物。正确的工作方式是：

1. 修改 `proto/` submodule 中的 `.proto` 文件来源仓库。
2. 更新 submodule 指向。
3. 重新构建相关 crate，让 `build.rs` 触发代码生成。

换句话说，proto 是源定义，生成后的 Rust 代码不是源定义。

## 一些实践建议

- 如果你在做应用集成，从 `remi-client-sdk` 开始，不要先直接碰 `remi-things-crdt`。
- 如果你已经有自己的认证 / 存储层，优先使用对象式 API，例如 `AuthClient::from_transport(...)`、`ProfileClient::from_transport(...)`。
- 如果你在做 UI，优先考虑 `ChatRuntime`，不要自己重复实现聊天流缓存和 interrupt 状态机。
- 如果你在做多设备 Things 同步，优先走 `TriggerSdk + TriggerClient + things_sync::sync_v3_documents_with_server(...)` 这条路径。
- 如果你只想做规则校验或 dry-run，直接依赖 `rule-trigger-engine` 会更小、更清晰。

## 当前仓库定位

`remi-sdk` 的目标不是做一个纯粹最小的 protocol bindings 仓库，而是承载 Remi 客户端真正可用的 Rust 运行时能力：

- 既包含远程 RPC client。
- 也包含本地优先运行时和同步辅助。
- 既支持高层应用接入。
- 也保留底层 CRDT / rule engine 的可复用性。

如果你需要更细的 crate 级文档，后续可以在各个成员 crate 目录下继续补充 README，但从仓库入口来看，这个根 README 应该已经足够解释 `remi-sdk` 的主要功能和基本使用方式。
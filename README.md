# shop-queue

最小闭环的「门店排队取号」Clojure 原型。顾客远程/扫码取号并轮询或订阅 SSE，员工叫号/入座/过号/取消，SQLite 全量持久化。

## 排队数据的价值（后续数据分析的入口）

排队数据把「排队」这个原本转瞬即逝、不可见的过程，变成可度量的资产。`storage` 里
按事件留存的完整历史，加上 append-only 的 `operations` 事件流，是三类决策的原料。

数据落两类、查两处：

- **店铺状态（`stores`，每店一条 EDN）**：每张票的 `party-size`、`table-type`、
  `source`（remote/scan）、`token`、`status`、`created-at`、`called-at`、`:n`
  （顺序号），以及各桌型队列的 `counter` / `status`。
- **操作日志（`operations`，append-only）**：每条命令一条
  `{:ts :actor :actor-id :action :target :result :details :seq}`，`:seq` 全局单调
  递增、可回放。查询：`GET /stores/{store}/operations`（`actor` / `action` /
  `since` / `limit` / `offset`）、`GET /stores/{store}/tickets/{token}/operations`
  （单号时间线）。

**看现在（实时运营）**：各桌型当前排多长、下一个叫谁、预计等多久。顾客和店员看到
同一份状态，减少现场焦虑和扯皮。

**看过去（容量与规划）**：从历史事件能算出一批真正驱动决策的指标：

- 到达率按时段分布 → 排班、营业时段
- 各桌型服务时长 → 每桌型该配几张桌
- 等待时间分布 → 能对顾客承诺多少分钟、设定 SLA
- 过号率 → 要不要加提醒 / 调整重新叫号策略
- 人数分布（2/3/多人）→ 桌型配比
- 翻台率 → 单桌单位时间的容量与营收

**看未来（改进与仿真）**：`operations` 是 append-only 事件流，天然可回放。把真实的
到达 / 服务 / 过号分布喂给 `sim`，就能在不动现场的情况下回答「加两张多人桌会怎样」
「叫号策略换成 X 会怎样」——这正是仿真存在的意义：它验证「数据能算出什么」。

一句话：当前是占位参数的演示，数据价值还只是示意；一旦接入真实流量，上面每一行
都能变成一条可执行决策。

## 边界与约定（写死，防止误上生产）

**无鉴权，仅限原型。** 当前所有端点都没有鉴权：任何知道 URL 的人都能取号、
叫号、关号、入座、过号、取消，`token` 是唯一的不透明凭据。这只能用于本地
演示 / 原型验证，**绝不能直接用于生产**。上线前必须补齐身份与授权（区分
顾客 / 员工角色，员工端点加鉴权与审计），把「凭 URL 即可叫号 / 关号」的能力
收口到有权限的员工账号。

**时间统一为 epoch 毫秒（ms）。** 所有持久化的时间字段——票的 `:created-at` /
`:called-at`，以及操作记录的 `:ts`——都是 epoch 毫秒（整数）。时间只用于展示与
追溯，不参与任何排序、计数或状态判断。

**并发模型：进程内串行写 + 乐观并发检测。** 写路径由进程内一把锁串行化（`api.clj`
的 `write-lock`）；store 文档带 `:version`，`save-store!` 用版本号做 CAS，两个进程
读到同一版本、各自落盘时，后到者会得到 `409 store-conflict` 而不是互相覆盖。
`counters` 的原子自增与 `tokens` 的唯一约束保证「计数 / token 不撞号」。多实例仍建议
单写服务；乐观锁只负责「检测冲突、拒绝覆盖」，不负责自动合并或重试。

**排序一律靠顺序号，不靠时间。** 号码先后只由 `:n` 决定。`:n` 是可变队列位次：
新票由该桌型的 `:counter` 单调分配，`requeue` 重排时对等待票整体重编号；
`:counter` 只负责给新票发号、不随重排回退。已终态（called / seated / cancelled /
no-show）票保留历史 `:n`，不参与排序。等待队列、`parties-ahead`、`current-called`
一律按 `:n` 排序 / 计数，绝不使用 `:created-at` / `:called-at` / `:ts` 等时间字段。
操作记录按其 append-only 的 `:seq` 排序，查询参数 `since` 也是 epoch 毫秒。

## 运行

```bash
QUEUE_DB=queue.db PORT=8080 lein run
```

## 测试

```bash
lein test
```

## 部署 / 运维

打包、部署、重启、systemd 配置、备份与安全组等运维信息见 [DEPLOY.md](DEPLOY.md)。

## 实时推送与断线重连

顾客号状态有两个获取渠道，**状态以轮询 / 快照为准，SSE 只做提醒**：

- `GET /stores/{store}/tickets/{token}`：权威查询，返回完整状态
  （`status`、`parties-ahead`、`current-called` 等）。
- `GET /stores/{store}/events/{token}`：SSE 订阅，连接建立后先发一条
  `snapshot` 事件（含当前权威状态与 `seq`），随后推送该号的
  `called / seated / no-show / cancelled` 事件。

每个 SSE 事件都带全局单调递增的 `seq`（同时写入 SSE 的 `id:` 字段，便于
浏览器自动回传 `Last-Event-ID`）。客户端断线重连时：

1. 先 `GET /stores/{store}/tickets/{token}` 拿一次权威状态；
2. 再重新订阅 SSE，并带上 `Last-Event-ID: <上次收到的 seq/id>`。

服务端会在重连时先重放 `seq` 大于 `Last-Event-ID` 的该号事件（最多重放最近
100 条，更早的由 snapshot 覆盖），随后照常发一条 `snapshot`。因为 snapshot
始终反映最新权威状态，即使事件有缺口或重放不完整，客户端也以
snapshot / GET 的值为准，不会出现轮询与推送两端不一致。

## 备份与恢复

数据库是单个 SQLite 文件（WAL 模式）。边写边复制文件会得到不一致的副本，所以用
SQLite 的 `VACUUM INTO` 生成一致快照。项目自带一个备份命令：

```bash
# 用法一：参数方式
lein run -m shop-queue.backup queue.db backups

# 用法二：环境变量方式（同参数效果）
QUEUE_DB=queue.db BACKUP_DIR=backups lein run -m shop-queue.backup
```

命令会在 `backups/` 下生成 `queue-<时间戳>.db`（目标文件不存在才会成功，时间戳保证
每日多次执行不冲突）。cron 每日备份示例：

```cron
0 2 * * * cd /path/to/shop-queue && QUEUE_DB=queue.db BACKUP_DIR=backups lein run -m shop-queue.backup
```

恢复：停止服务，用备份文件覆盖 `queue.db`（并清掉 `queue.db-wal` / `queue.db-shm`），
再启动即可；也可以直接把 `QUEUE_DB` 指向备份文件。`VACUUM INTO` 产物是自包含数据库，
启动时 `load-all` 会直接读出与备份时刻一致的完整状态。

### 启动加载失败告警

启动时若 `load-all` 失败（如文档损坏），进程会打印 `FATAL: ...` 到 stderr、以非零
退出码结束，**不会**用空状态或种子状态静默上线。这条守护逻辑由
`shop-queue.recovery-test/startup-refuses-empty-state-on-load-failure` 覆盖。

### 恢复演练

`shop-queue.recovery-test/restart-reloads-consistent-state` 是一条可重复的恢复测试：
取号 → 叫号 → 入座 → 再取号并落盘，随后从同一文件重新加载，断言票状态、桌型、各队列
计数与落盘前完全一致。手工演练等价步骤为：

1. 造数：取号、叫号、入座。
2. `kill -9` 模拟断电（每次写命令在同一个 SQLite 事务里完成 counter / token 分配、
   store 文档落盘与 operation 追加，要么整体成功、要么整体回滚）。
3. 重启 `lein run`，查询各 token 状态与队列视图，核对与断电前一致。

## 模块

- `domain.clj` — 纯函数：状态机、桌型匹配、派生视图、`diff-events`。
- `storage.clj` — SQLite，每店一条 EDN 文档。
- `api.clj` — http-kit 路由 + SSE 订阅注册表 + 事件 seq / 重放（IO 边界）。
- `core.clj` — 入口（启动加载与失败守护）。
- `backup.clj` — `VACUUM INTO` 备份命令。

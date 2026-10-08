# shop-queue 项目上下文

门店排队取号系统的最小闭环原型。数据导向编程（DOP）：状态是普通数据，领域核心是纯函数。

## 技术栈

- Clojure 1.12 + http-kit（HTTP / SSE）+ cheshire（JSON）+ java.jdbc + SQLite
- 构建：Leiningen（`lein test` 跑测试，`lein deploy` = test + uberjar + 远程部署）

## 模块边界

- `src/shop_queue/domain.clj` —— 领域纯函数（状态机、派生数据），无 IO
- `src/shop_queue/storage.clj` —— SQLite 持久化边界（EDN 文档、操作日志）
- `src/shop_queue/api.clj` —— HTTP 路由、SSE、事务编排与内存态
- `src/shop_queue/core.clj` —— 入口

## 领域模型

- 多店：一个 SQLite 文件内按 `store-id` 建键存多个店铺。`store-id` 由服务端生成
  UUID（URL 内使用），展示名独立放在 `:name`；`status` 为 `active` / `archived`。
- 店铺生命周期：`POST /stores` 创建、`PATCH /stores/{id}` 改名/阈值、
  `PUT /stores/{id}/table-types` 改桌型、`DELETE /stores/{id}` 归档（软删除，保留
  persisted 文档与 operations，不做物理删除）。
- 桌型 `two` / `three` / `large`，每种桌型一条独立队列。
- 票状态机：`waiting → called → (seated | no-show)`，`waiting → cancelled`。
- 队列：`open` / `closed`；关闭后禁止新取号，已排队仍可叫号 / 入座。

## 必须遵守的约定

- 并发模型：单进程、单写者；一个 SQLite 文件同一时刻只允许一个写进程。
- 写命令在一个事务内完成（取号 = claim-token + next-counter + save-store + append-operation）。
- `append-operation!` 的 seq 用单条 `INSERT ... COALESCE((SELECT MAX(seq)...), -1) + 1` 原子分配。
- `:n` 是可变队列位次；`:counter` 取号后不回退；终态票保留历史 `:n`。
- token 高熵、不可枚举；不存在的 store / tt / ticket，或 token 不属于本店，一律统一 404。
- 已归档店铺从列表隐藏、所有店铺级请求统一 404，但数据仍保留在 SQLite 中。
- 排序一律靠 `:n`，不用时间排序；时间仅用 epoch 毫秒做展示与追溯。
- 无鉴权仅限原型，已写死为边界，不得误上生产。
- 重启后号码不重复、计数器连续（close 后再 open 不重置）。
- 启动 `load-all` 失败必须告警，不得带空状态静默上线。
- 备份与恢复见 DEPLOY.md；存储文档采用乐观并发版本号（CAS），冲突返回 409。
- SSE 队列订阅按 `[store-id table-type]` 隔离；ticket 事件仍按全局唯一 token 隔离。

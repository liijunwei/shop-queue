# shop-queue

最小闭环的「门店排队取号」Clojure 原型。顾客远程/扫码取号并轮询或订阅 SSE，员工叫号/入座/过号/取消，SQLite 全量持久化。

## 运行

```bash
QUEUE_DB=queue.db PORT=8080 lein run
```

## 测试

```bash
lein test
```

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
2. `kill -9` 模拟断电（SQLite WAL + 单条 `save-store!` 事务保证要么旧状态、要么新状态）。
3. 重启 `lein run`，查询各 token 状态与队列视图，核对与断电前一致。

## 模块

- `domain.clj` — 纯函数：状态机、桌型匹配、派生视图、`diff-events`。
- `storage.clj` — SQLite，每店一条 EDN 文档。
- `api.clj` — http-kit 路由 + SSE 订阅注册表（IO 边界）。
- `core.clj` — 入口（启动加载与失败守护）。
- `backup.clj` — `VACUUM INTO` 备份命令。

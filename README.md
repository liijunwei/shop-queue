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

## 模块

- `domain.clj` — 纯函数：状态机、桌型匹配、派生视图、`diff-events`。
- `storage.clj` — SQLite，每店一条 EDN 文档。
- `api.clj` — http-kit 路由 + SSE 订阅注册表（IO 边界）。
- `core.clj` — 入口。

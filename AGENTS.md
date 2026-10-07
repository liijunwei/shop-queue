# shop-queue 开发约定（给 agent）

## 通用

- 动手前先读 CONTEXT.md；改动涉及约定 / 边界时，同步更新 CONTEXT.md 和 README.md。
- 领域逻辑保持在 `domain.clj` 纯函数；IO / 副作用放在 `storage.clj` / `api.clj` 边界。
- 简单优先：单一职责、值语义，不引入过早抽象。

## 测试

- 先写 / 补测试，`lein test` 全绿才算完成。
- 并发 / 原子性相关改动必须有对应测试（事务回滚、seq 无缺口、CAS 409 等）。

## 提交

- 阶段提交：每个逻辑完整、可独立回滚的单元做一次 git 提交。
- 提交信息原子、说清做什么和为什么；不混入无关改动。
- 默认开 PR（标题或分支名含 `MACM-<n>`），除非明确要求本地改动。

## 部署

- 见 DEPLOY.md；`lein deploy` = test + uberjar + 远程部署。
- 涉及边界 / 并发模型 / 备份恢复的改动，必须同步更新 DEPLOY.md 或 CONTEXT.md。

# 部署与运维（shop-queue）

本文记录 shop-queue 的打包、部署、重启等关键运维信息，目标环境为腾讯云 CVM。

## 环境概览

| 项 | 值 |
| --- | --- |
| SSH 别名 | `tencent-openclaw` |
| 服务端口 | `8080` |
| 进程管理 | systemd，unit 名 `shop-queue` |
| 运行用户 | `shopqueue` |
| 部署目录 | `/opt/shop-queue` |
| 产物 | `/opt/shop-queue/shop-queue-standalone.jar` |
| 数据 | `/opt/shop-queue/queue.db`（SQLite，WAL） |
| 备份目录 | `/opt/shop-queue/backups/`（每日 02:00 自动备份） |

## 打包（本地）

仓库是 Leiningen 工程，在本机 `/Users/me/codeout/shop-queue` 执行：

```bash
cd /Users/me/codeout/shop-queue
lein test        # 先跑测试，全绿再打包
lein uberjar     # 产出 target/uberjar/shop-queue-0.1.0-SNAPSHOT-standalone.jar
```

产物名带版本号（`shop-queue-0.1.0-SNAPSHOT-standalone.jar`），服务器上统一命名为
`shop-queue-standalone.jar`，上传时重命名即可。

## 部署

先传 `/tmp` 再 `sudo mv` 到位，避免直接写 `/opt`（该目录属 `shopqueue`）：

```bash
cd /Users/me/codeout/shop-queue
scp target/uberjar/shop-queue-0.1.0-SNAPSHOT-standalone.jar \
    tencent-openclaw:/tmp/shop-queue-standalone.jar
ssh tencent-openclaw 'sudo mv /tmp/shop-queue-standalone.jar /opt/shop-queue/shop-queue-standalone.jar'
```

只替换 jar 不动 `queue.db`，重启后数据与排队状态保留。

## 重启 / 状态 / 日志

```bash
ssh tencent-openclaw 'sudo systemctl restart shop-queue'
ssh tencent-openclaw 'systemctl status shop-queue --no-pager'
ssh tencent-openclaw 'sudo journalctl -u shop-queue -f'
```

验证：

```bash
ssh tencent-openclaw 'curl -s http://localhost:8080/healthz'   # 期望 {"status":"ok","db":"writable"}
ssh tencent-openclaw 'curl -s http://localhost:8080/'          # 返回各店桌型与队列状态
```

## systemd unit

`/etc/systemd/system/shop-queue.service`：

```ini
[Unit]
Description=shop-queue store queue ticketing service
After=network.target

[Service]
Type=simple
User=shopqueue
Group=shopqueue
WorkingDirectory=/opt/shop-queue
Environment=QUEUE_DB=/opt/shop-queue/queue.db
Environment=PORT=8080
ExecStart=/usr/bin/java -jar /opt/shop-queue/shop-queue-standalone.jar
Restart=on-failure
RestartSec=2
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

修改 unit 后执行 `sudo systemctl daemon-reload && sudo systemctl restart shop-queue`。

## 数据与备份

- 数据库为单个 SQLite 文件（WAL 模式），路径由 `QUEUE_DB` 指定。
- 每日 02:00 用 `VACUUM INTO` 生成一致快照到 `/opt/shop-queue/backups/`。
- 手工备份与恢复方法见 `README.md` 的「备份与恢复」小节。

## 网络（安全组）

服务器只对公网放行 SSH(22) 与服务端口 TCP 8080。服务监听 `0.0.0.0:8080`。
若公网访问 8080 不通，先确认腾讯云安全组已放行该实例的入站规则 TCP 8080。

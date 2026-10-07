# shop-queue 本地 / 远程部署
SSH_HOST   := tencent-openclaw
REMOTE_DIR := /opt/shop-queue
REMOTE_JAR := shop-queue-standalone.jar
UBERJAR    := target/uberjar/shop-queue-0.1.0-SNAPSHOT-standalone.jar
QUEUE_DB   ?= queue.db
PORT       ?= 8080

.PHONY: deploy deploy-remote deploy-local build test run restart

# 默认目标：远程部署（打包 → 上传 → 替换 → 重启 → 验证）
deploy: deploy-remote

# 远程部署
deploy-remote: build
	@echo "==> upload $(UBERJAR)"
	scp $(UBERJAR) $(SSH_HOST):/tmp/$(REMOTE_JAR)
	@echo "==> replace & restart"
	ssh $(SSH_HOST) 'sudo mv /tmp/$(REMOTE_JAR) $(REMOTE_DIR)/$(REMOTE_JAR) && sudo systemctl restart shop-queue'
	@echo "==> verify"
	ssh $(SSH_HOST) 'curl -s http://localhost:8080/healthz'; echo

# 本地部署：打包后在本地用 uberjar 运行（前台）
deploy-local: build
	@echo "==> run local uberjar on :$(PORT) (QUEUE_DB=$(QUEUE_DB))"
	QUEUE_DB=$(QUEUE_DB) PORT=$(PORT) java -jar $(UBERJAR)

build: test
	lein uberjar

test:
	lein test

# 本地开发运行（不重新打包）
run:
	QUEUE_DB=$(QUEUE_DB) PORT=$(PORT) lein run

# 仅远程重启（不重新打包/上传）
restart:
	ssh $(SSH_HOST) 'sudo systemctl restart shop-queue'
	ssh $(SSH_HOST) 'curl -s http://localhost:8080/healthz'; echo

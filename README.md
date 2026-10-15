# vnc_schdule_api

FastAPI 调度服务。PostgreSQL 保存任务、执行轮次、Worker 租约和 Outbox；Redis Stream 只负责任务通知。服务名 `schdule` 按项目约定保留。

## 本地启动

需要 Python 3.10+、PostgreSQL 和 Redis：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
export VNC_DATABASE_URL='postgresql+asyncpg://<用户>:<密码>@127.0.0.1:5432/vnc_schedule'
export VNC_REDIS_URL='redis://127.0.0.1:6379/0'
.venv/bin/alembic upgrade head
.venv/bin/uvicorn src.main:app --host 0.0.0.0 --port 8088
```

数据库密码只通过环境变量提供，不写入源码、示例配置或 Git。

开发 Compose 使用 PostgreSQL `15432`、Redis `16379` 和 API `8088`：

```bash
export VNC_DEV_DB_PASSWORD='<开发数据库密码>'
docker compose up -d --build
docker compose exec api alembic upgrade head
```

## 创建和查询任务

```bash
curl -X POST http://127.0.0.1:8088/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-run-001' \
  -d '{
    "command":["scripts/run_case.sh","--case","demo"],
    "cwd":"projects/demo",
    "timeout_seconds":7200
  }'

curl http://127.0.0.1:8088/api/v1/tasks/<task_id>
curl 'http://127.0.0.1:8088/api/v1/tasks?limit=50&offset=0'
curl -X POST http://127.0.0.1:8088/api/v1/tasks/<task_id>/cancel
```

`command` 是参数数组，不经过 Shell；`cwd` 和可选 `input_path` 都相对于 Worker 的共享存储根目录。

## Worker 操作

Worker 接口部署在可信内网，不使用 Token：

```bash
curl http://127.0.0.1:8088/api/v1/workers
```

Worker 正常退出会携带当前 `session_id` 调用 shutdown。Worker 崩溃且旧租约阻止重新注册时，先确认物理机上的旧进程已经退出，再按 `.env` 中配置的 Worker IP 强制注销：

```bash
curl -X POST \
  http://127.0.0.1:8088/api/v1/workers/192.168.10.21/unregister
```

强制注销会将尚未确认结果的任务标记为 `lost`，不会自动重跑。

## 调度与恢复

- 创建任务和 Outbox 事件在同一数据库事务提交。
- Worker 获得数据库授权后才能启动任务。
- 结果先写入 PostgreSQL，成功后才 ACK Redis。
- 默认任务超时 7200 秒，从授权开始计算；排队时间不计入。
- 默认租约 30 秒，由注册响应下发给 Worker。
- 取消指令通过心跳响应持续下发，直到任务完成清理。
- 对账循环补投长期 queued 任务，清理 abandoned pending 消息，并将失联执行标记为 `lost`。

接口文档：`/docs`；OpenAPI：`/openapi.json`。

## 测试

迁移专用测试数据库后执行：

```bash
export VNC_DATABASE_URL='postgresql+asyncpg://<用户>:<密码>@127.0.0.1:5432/vnc_schedule_test'
.venv/bin/alembic upgrade head
.venv/bin/python -m pytest -q
```

不要让不同测试实例同时使用同一个测试数据库。真实 systemd、TurboVNC 和 Squish 行为由 Worker 的 CentOS 7 验收清单确认。

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
  -d '{
    "command":["scripts/run_case.sh","--case","demo"],
    "cwd":"/home/snskap/projects/demo",
    "timeout_seconds":7200
  }'

curl http://127.0.0.1:8088/api/v1/tasks/<task_id>
curl 'http://127.0.0.1:8088/api/v1/tasks?limit=50&offset=0'
curl -X POST http://127.0.0.1:8088/api/v1/tasks/<task_id>/cancel
```

`command` 是参数数组，不经过 Shell；`cwd` 是 Worker 物理机上的绝对目录。可选 `input_path` 相对于 `cwd`，也可以传绝对路径。不需要配置共享目录；多 Worker 共用队列时，应在各机器准备相同的任务路径和文件。

每次 POST 创建一个新任务，不再使用 `Idempotency-Key`。队列重复通知仍由执行授权防止重复启动。结果 `result_path` 指向执行机器上 `VNC_LOG_DIR/tasks/<task_id>/<attempt_id>/task-output.json`，其内容分别列出带日期的 stdout/stderr 文件。

升级前停止旧版本服务，再执行 `alembic upgrade head`，迁移 `0004_drop_creation_key` 删除旧字段。旧相对 cwd 的排队任务请取消后用绝对路径重新创建；API 与 Worker 应同步更新。

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

接口文档：`/docs`；OpenAPI：`/openapi.json`。Swagger UI 5.33.3 的 JS、CSS
和图标已保存在 `src/static/`，运行时不会访问公网 CDN。未使用的 `/redoc`
入口已关闭，避免浏览器尝试加载外网资源。

## 测试

迁移专用测试数据库后执行：

```bash
export VNC_DATABASE_URL='postgresql+asyncpg://<用户>:<密码>@127.0.0.1:5432/vnc_schedule_test'
.venv/bin/alembic upgrade head
.venv/bin/python -m pytest -q
```

不要让不同测试实例同时使用同一个测试数据库。真实 systemd、TurboVNC 和 Squish 行为由 Worker 的 CentOS 7 验收清单确认。

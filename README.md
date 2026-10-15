# vnc_schdule_api

基于 FastAPI、PostgreSQL 和 Redis Stream 的 VNC 测试任务调度服务。PostgreSQL 保存任务、执行轮次、Worker 租约及待投递事件；Redis 只负责通知。服务名中的 `schdule` 按约定保留。

## 本地启动

需要 Python 3.10+、PostgreSQL 和 Redis。先创建专用数据库 `vnc_schedule`、测试数据库 `vnc_schedule_test`，不要清空已有数据库或 Redis。使用独立 Redis 前缀，例如 `vnc_schedule`。

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
export VNC_DATABASE_URL='postgresql+asyncpg://<用户>:<密码>@127.0.0.1:5432/vnc_schedule'
export VNC_INTERNAL_TOKEN='<至少 12 字符的内部令牌>'
export VNC_REDIS_URL='redis://127.0.0.1:6379/0'
.venv/bin/alembic upgrade head
.venv/bin/uvicorn src.main:app --host 127.0.0.1 --port 8088
```

密码和令牌只从环境变量注入，不要提交 `.env`。完整配置见 `.env.example`。本机已使用 PostgreSQL 14 和 Redis 验证。开发 Compose 另用 PostgreSQL `15432`、Redis `16379`、API `8088`，不会占用本机的 `5432` 和 `6379`：

```bash
export VNC_DEV_DB_PASSWORD='<开发数据库密码>'
export VNC_INTERNAL_TOKEN='<内部令牌>'
docker compose up -d --build
docker compose exec api alembic upgrade head
```

## HTTP 接口与协议

路由统一以 `/api/v1` 开头，交互文档见 `/docs`，原始 OpenAPI 见 `/openapi.json`。Worker 协议模型由本仓库 `src/schemas/protocol.py` 维护；Worker 仓库的端到端测试会对真实 API 进行协议兼容验证。

| 方法与路径 | 用途 |
| --- | --- |
| `POST /tasks` | 创建任务；支持 `Idempotency-Key` 请求头 |
| `GET /tasks/{id}`、`GET /tasks?limit=50&offset=0` | 查询详情与分页列表 |
| `POST /tasks/{id}/cancel` | 接受取消请求，返回 HTTP 202 |
| `POST /workers/register` | 注册稳定 `worker_id` 和本次 `session_id` |
| `POST /workers/{id}/heartbeat` | 续租、上报 Slot、领取待处理取消指令 |
| `POST /workers/{id}/claim` | 对任务和 Slot 取得数据库执行授权 |
| `POST /workers/{id}/tasks/{task_id}/result` | 提交执行结果 |
| `GET /workers` | 查询 Worker 在线状态与 Slot 快照 |

Worker 接口需要 `Authorization: Bearer <VNC_INTERNAL_TOKEN>`。注册请求还需提交本机 `slot_ids`，授权时会拒绝未注册的 Slot。创建示例：

```bash
curl -X POST http://127.0.0.1:8088/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-run-001' \
  -d '{"command":["/usr/bin/true"],"timeout_seconds":7200}'
```

`command` 是参数数组，不经 Shell 解释；只有可信上游才能调用创建接口。任务输入路径是共享存储根目录下的相对路径。任务详情保存 `result_summary` 和报告相对路径；报告路径是 `task_id/attempt_id/task-output.json`，清单中列出按日期及 100 MB 分片的原始输出文件。报告、截图和服务日志需分别设置清理策略。

## 调度、取消与恢复

创建任务与 Outbox 事件在同一数据库事务提交。后台循环将事件写入 Redis Stream，消息含任务 ID、事件 ID、投递轮次和协议版本；参数始终以数据库为准。Redis 写入成功但数据库尚未记录成功时，允许再次投递。执行授权、Session 和 attempt 校验阻止重复运行及旧结果覆盖。结果先入库，再由 Worker 确认 Stream 消息。

默认任务超时 7200 秒，在授权时开始计时；排队时间不计入。默认心跳 5 秒、租约 30 秒、自动重试 0 次。运行中任务取消后先显示 `cancel_requested`，Worker 停止并清理进程后才显示 `cancelled`；等待中的任务可直接取消。Worker 失联时，服务端将正在执行的任务标为 `lost`，含义是执行结果未知，不能据此认定进程已经退出，也不会自动重跑。

对账循环会限频补发长期等待的任务，并恢复 Redis 丢失后的待执行通知。已获授权的任务进入失联恢复流程，不作为待执行任务重新投递。Stream 首版不自动裁剪；运维裁剪时必须先核对 Consumer Group 的待处理消息及数据库任务状态。

## 测试与部署

将 `VNC_DATABASE_URL` 指向已执行迁移的专用测试数据库，再运行 `.venv/bin/python -m pytest -q`。Worker 仓库的端到端测试还需要 `VNC_E2E_DATABASE_URL` 指向同一个测试数据库、可用 Redis 和同级模拟脚本。不要与其他测试实例共用该测试数据库同时执行。

生产目标是 CentOS 7，需单独准备 Python 3.10+、兼容的 OpenSSL、PostgreSQL/Redis 客户端依赖和匹配 CPU/Python ABI 的离线 wheel。迁移须先于 API 启动。API 的多阶段 Dockerfile 以非 root 用户运行。真实 VNC 脚本格式、Squish、systemd、共享挂载及长期空闲防锁屏须在内网机器验收，本机模拟测试不能视为真实桌面验收。

# vnc_schdule_api

VNC GUI 测试调度 API。FastAPI 提供接口，PostgreSQL 保存任务与执行授权，Redis Stream 发送任务通知。项目名 `schdule` 保留现有拼写。

## 基础结构

```text
src/core/       配置、数据库连接与日志
src/models/     tasks、workers、outbox_events
src/schemas/    HTTP 协议
src/routers/    任务与 Worker 接口
src/services/   授权、结果、调度对账与文件上传
src/static/     内网 Swagger UI 资源及许可证
src/main.py     应用入口
alembic/        数据库迁移
tests/         协议及业务测试
```

## 启动

使用 Python 3.10+，提前准备 PostgreSQL 和 Redis；在本仓库根目录执行：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
# 编辑 .env，填写真实数据库连接和 Redis 地址；不要提交 .env。
.venv/bin/alembic upgrade head
.venv/bin/uvicorn src.main:app --host 0.0.0.0 --port 8088
```

`/health` 返回进程状态；`/docs` 为离线接口文档。开发 Compose 用法：

```bash
# 先在当前终端设置 VNC_DEV_DB_PASSWORD；不要把密码写进命令记录或 Git。
docker compose up -d --build
docker compose exec api alembic upgrade head
```

Compose 暴露数据库 15432、Redis 16379 和 API 8088（均绑定本机）。真实 Worker 位于其他机器时，需要另外配置受控的网络访问。上传日志还需将日志存储挂载给 API，并配置 VNC_FILE_MGMT_URL。

任务示例：

```bash
curl -X POST http://127.0.0.1:8088/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{"command":["/bin/echo","hello"],"cwd":"/tmp","timeout_seconds":7200}'
```

`command` 不经过 Shell，`cwd` 是 Worker 的绝对路径。查询结果包括 `job_info` 日志路径及文件 ID。

测试需使用已迁移的专用测试数据库设置 `VNC_DATABASE_URL`，再运行 `.venv/bin/python -m pytest -q`；不要指向业务库。

系统需求、架构、部署和验收统一维护在 [vnc_build/docs](https://github.com/hhh17950/vnc_build/tree/main/docs)。

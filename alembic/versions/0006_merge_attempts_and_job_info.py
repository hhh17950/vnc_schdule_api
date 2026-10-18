"""合并 attempt 到 tasks，新增 job_info，移除 input_path 与 task_attempts。"""
from alembic import op
import sqlalchemy as sa

revision = "0006_merge_attempts_and_job_info"
down_revision = "0005_drop_result_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("worker_id", sa.String(120), nullable=True))
    op.add_column("tasks", sa.Column("session_id", sa.String(36), nullable=True))
    op.add_column("tasks", sa.Column("slot_id", sa.String(120), nullable=True))
    op.add_column("tasks", sa.Column("started_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("tasks", sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("tasks", sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("tasks", sa.Column("job_info", sa.JSON(), nullable=True))

    # 状态归一：lost 统一为 unknown。
    op.execute("UPDATE tasks SET status = 'unknown' WHERE status = 'lost'")
    # 把仍在执行的 attempt 信息合并回任务，保留诊断字段；随后归为 unknown。
    op.execute(
        """
        UPDATE tasks t
        SET worker_id = a.worker_id,
            session_id = a.session_id,
            slot_id = a.slot_id,
            started_at = a.started_at,
            deadline_at = a.deadline_at,
            error = COALESCE(t.error, a.error)
        FROM task_attempts a
        WHERE t.id = a.task_id
          AND t.status IN ('running', 'cancel_requested', 'timeout_requested')
          AND a.finished_at IS NULL
        """
    )
    op.execute(
        "UPDATE tasks SET status = 'unknown', finished_at = now() "
        "WHERE status IN ('running', 'cancel_requested', 'timeout_requested')"
    )
    op.drop_column("tasks", "input_path")
    op.drop_column("tasks", "current_attempt_id")
    op.drop_table("task_attempts")


def downgrade() -> None:
    op.add_column("tasks", sa.Column("input_path", sa.String(1000), nullable=True))
    op.add_column("tasks", sa.Column("current_attempt_id", sa.String(36), nullable=True))
    op.create_table(
        "task_attempts",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("number", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(120), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("slot_id", sa.String(120), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("error", sa.Text()),
        sa.UniqueConstraint("task_id", "number"),
    )
    op.drop_column("tasks", "job_info")
    op.drop_column("tasks", "finished_at")
    op.drop_column("tasks", "deadline_at")
    op.drop_column("tasks", "started_at")
    op.drop_column("tasks", "slot_id")
    op.drop_column("tasks", "session_id")
    op.drop_column("tasks", "worker_id")

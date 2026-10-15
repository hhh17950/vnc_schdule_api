"""Initial task and worker tables."""

from alembic import op
import sqlalchemy as sa

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tasks",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("idempotency_key", sa.String(200), unique=True),
        sa.Column("command", sa.JSON(), nullable=False),
        sa.Column("input_path", sa.String(1000)),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False),
        sa.Column("current_attempt_id", sa.String(36)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_enqueued_at", sa.DateTime(timezone=True)),
        sa.Column("result_path", sa.String(1000)),
        sa.Column("error", sa.Text()),
    )
    op.create_index("ix_tasks_status", "tasks", ["status"])
    op.create_table(
        "workers",
        sa.Column("id", sa.String(120), primary_key=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column("slots", sa.JSON(), nullable=False),
        sa.Column("capabilities", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
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
        sa.Column("result_path", sa.String(1000)),
        sa.Column("error", sa.Text()),
        sa.UniqueConstraint("task_id", "number"),
    )
    op.create_index("ix_task_attempts_task_id", "task_attempts", ["task_id"])
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_outbox_due", "outbox_events", ["sent_at", "next_attempt_at"])


def downgrade() -> None:
    op.drop_table("outbox_events")
    op.drop_table("task_attempts")
    op.drop_table("workers")
    op.drop_table("tasks")

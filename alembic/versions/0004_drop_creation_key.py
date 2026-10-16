"""移除任务创建幂等键；保留执行授权的去重机制。"""
from alembic import op
import sqlalchemy as sa

revision = "0004_drop_creation_key"
down_revision = "0003_worker_contract"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("tasks", "idempotency_key")


def downgrade() -> None:
    op.add_column("tasks", sa.Column("idempotency_key", sa.String(200), nullable=True))
    op.create_unique_constraint("tasks_idempotency_key_key", "tasks", ["idempotency_key"])

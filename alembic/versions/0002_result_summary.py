"""Store a compact result summary alongside report paths."""

from alembic import op
import sqlalchemy as sa

revision = "0002_result_summary"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("result_summary", sa.JSON(), nullable=True))
    op.add_column("task_attempts", sa.Column("result_summary", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("task_attempts", "result_summary")
    op.drop_column("tasks", "result_summary")

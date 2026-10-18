"""Remove result paths and summaries; the API only reports task status."""

from alembic import op
import sqlalchemy as sa

revision = "0005_drop_result_fields"
down_revision = "0004_drop_creation_key"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("task_attempts", "result_summary")
    op.drop_column("task_attempts", "result_path")
    op.drop_column("tasks", "result_summary")
    op.drop_column("tasks", "result_path")


def downgrade() -> None:
    op.add_column("tasks", sa.Column("result_path", sa.String(1000), nullable=True))
    op.add_column("tasks", sa.Column("result_summary", sa.JSON(), nullable=True))
    op.add_column("task_attempts", sa.Column("result_path", sa.String(1000), nullable=True))
    op.add_column("task_attempts", sa.Column("result_summary", sa.JSON(), nullable=True))

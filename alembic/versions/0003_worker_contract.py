"""Add task cwd and explicit worker slot identifiers."""

from alembic import op
import sqlalchemy as sa

revision = "0003_worker_contract"
down_revision = "0002_result_summary"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("cwd", sa.String(1000), nullable=True))
    op.execute("UPDATE tasks SET cwd = '.' WHERE cwd IS NULL")
    op.alter_column("tasks", "cwd", nullable=False)

    op.add_column("workers", sa.Column("slot_ids", sa.JSON(), nullable=True))
    op.execute(
        "UPDATE workers SET slot_ids = COALESCE(capabilities -> 'slot_ids', '[]'::json) "
        "WHERE slot_ids IS NULL"
    )
    op.alter_column("workers", "slot_ids", nullable=False)
    op.drop_column("workers", "capabilities")


def downgrade() -> None:
    op.add_column("workers", sa.Column("capabilities", sa.JSON(), nullable=True))
    op.execute(
        "UPDATE workers SET capabilities = json_build_object('slot_ids', slot_ids) "
        "WHERE capabilities IS NULL"
    )
    op.alter_column("workers", "capabilities", nullable=False)
    op.drop_column("workers", "slot_ids")
    op.drop_column("tasks", "cwd")

"""Task lease columns

Revision ID: 0003_tasks_lease
Revises: 0002_workers
Create Date: 2026-09-19

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0003_tasks_lease"
down_revision = "0002_workers"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("lease_owner", sa.String(100), nullable=True))
    op.add_column(
        "tasks", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_index("ix_tasks_lease_expires_at", "tasks", ["lease_expires_at"])


def downgrade() -> None:
    op.drop_index("ix_tasks_lease_expires_at", table_name="tasks")
    op.drop_column("tasks", "lease_expires_at")
    op.drop_column("tasks", "lease_owner")
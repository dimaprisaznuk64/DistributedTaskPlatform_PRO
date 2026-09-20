"""DLQ retry fields on tasks

Revision ID: 0006_dlq_retry
Revises: 0005_refresh_tokens
Create Date: 2026-09-20

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0006_dlq_retry"
down_revision = "0005_refresh_tokens"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "tasks",
        sa.Column("dlq_requeue_count", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column("tasks", sa.Column("dlq_retry_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("tasks", "dlq_retry_at")
    op.drop_column("tasks", "dlq_requeue_count")
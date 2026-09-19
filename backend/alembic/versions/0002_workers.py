"""Workers heartbeat table

Revision ID: 0002_workers
Revises: 0001_initial
Create Date: 2026-09-19

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002_workers"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workers",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("worker_id", sa.String(100), nullable=False),
        sa.Column("hostname", sa.String(255), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_tasks", sa.Integer(), nullable=False),
        sa.Column("failed_tasks", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_workers_worker_id", "workers", ["worker_id"], unique=True)
    op.create_index("ix_workers_status", "workers", ["status"])


def downgrade() -> None:
    op.drop_index("ix_workers_status", table_name="workers")
    op.drop_index("ix_workers_worker_id", table_name="workers")
    op.drop_table("workers")
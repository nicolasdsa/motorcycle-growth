"""Create lessons, progress and saved answers."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260719_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "lessons",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("topic", sa.String(length=200), nullable=False),
        sa.Column("slug", sa.String(length=240), nullable=False),
        sa.Column("target_role", sa.String(length=32), nullable=False),
        sa.Column("target_level", sa.String(length=16), nullable=False),
        sa.Column("depth", sa.String(length=16), nullable=False),
        sa.Column("focus", sa.String(length=64), nullable=True),
        sa.Column("language", sa.String(length=16), nullable=False),
        sa.Column("is_favorite", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("content_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("specification", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )
    op.create_index("ix_lessons_topic", "lessons", ["topic"])
    op.create_index("ix_lessons_slug", "lessons", ["slug"], unique=True)
    op.create_table(
        "progress",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("lesson_id", sa.String(length=36), nullable=False),
        sa.Column("completed_section_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("percentage", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_section_id", sa.String(length=120), nullable=True),
        sa.Column("activity_state", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["lesson_id"], ["lessons.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lesson_id"),
    )
    op.create_index("ix_progress_lesson_id", "progress", ["lesson_id"], unique=True)
    op.create_table(
        "saved_answers",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("lesson_id", sa.String(length=36), nullable=False),
        sa.Column("question_id", sa.String(length=120), nullable=False),
        sa.Column("answer", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["lesson_id"], ["lessons.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lesson_id", "question_id"),
    )
    op.create_index("ix_saved_answers_lesson_id", "saved_answers", ["lesson_id"])


def downgrade() -> None:
    op.drop_index("ix_saved_answers_lesson_id", table_name="saved_answers")
    op.drop_table("saved_answers")
    op.drop_index("ix_progress_lesson_id", table_name="progress")
    op.drop_table("progress")
    op.drop_index("ix_lessons_slug", table_name="lessons")
    op.drop_index("ix_lessons_topic", table_name="lessons")
    op.drop_table("lessons")

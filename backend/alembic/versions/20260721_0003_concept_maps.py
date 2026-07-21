"""Populate a coherent concept map in existing lesson summaries."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from app.generation.builder import build_lesson
from app.schemas.lesson import LessonCreate

revision: str = "20260721_0003"
down_revision: str | None = "20260719_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    lessons = sa.table(
        "lessons",
        sa.column("id", sa.String),
        sa.column("topic", sa.String),
        sa.column("target_role", sa.String),
        sa.column("target_level", sa.String),
        sa.column("depth", sa.String),
        sa.column("focus", sa.String),
        sa.column("language", sa.String),
        sa.column("content_version", sa.Integer),
        sa.column("specification", sa.JSON),
    )
    rows = bind.execute(sa.select(lessons)).mappings()
    for row in rows:
        specification = dict(row["specification"])
        if specification.get("summary", {}).get("concept_map"):
            continue
        request = LessonCreate(
            topic=row["topic"], target_role=row["target_role"], target_level=row["target_level"],
            depth=row["depth"], focus=row["focus"], language=row["language"],
        )
        specification.setdefault("summary", {})["concept_map"] = build_lesson(request, row["id"]).summary.concept_map.model_dump(mode="json")
        bind.execute(
            lessons.update().where(lessons.c.id == row["id"]).values(
                specification=specification,
                content_version=row["content_version"] + 1,
            )
        )


def downgrade() -> None:
    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT id, specification FROM lessons")).mappings()
    for row in rows:
        specification = dict(row["specification"])
        summary = specification.get("summary")
        if not isinstance(summary, dict) or "concept_map" not in summary:
            continue
        summary.pop("concept_map")
        bind.execute(
            sa.text("UPDATE lessons SET specification = CAST(:specification AS jsonb) WHERE id = :id"),
            {"id": row["id"], "specification": sa.JSON().bind_processor(bind.dialect)(specification)},
        )

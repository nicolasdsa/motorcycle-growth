import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Lesson(Base):
    __tablename__ = "lessons"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    topic: Mapped[str] = mapped_column(String(200), index=True)
    slug: Mapped[str] = mapped_column(String(240), unique=True, index=True)
    target_role: Mapped[str] = mapped_column(String(32))
    target_level: Mapped[str] = mapped_column(String(16))
    depth: Mapped[str] = mapped_column(String(16))
    focus: Mapped[str | None] = mapped_column(String(64), nullable=True)
    language: Mapped[str] = mapped_column(String(16), default="pt-BR")
    is_favorite: Mapped[bool] = mapped_column(Boolean, default=False)
    content_version: Mapped[int] = mapped_column(Integer, default=1)
    specification: Mapped[dict] = mapped_column(JSON().with_variant(JSONB, "postgresql"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    progress: Mapped["Progress | None"] = relationship(
        back_populates="lesson", cascade="all, delete-orphan", uselist=False
    )
    answers: Mapped[list["SavedAnswer"]] = relationship(
        back_populates="lesson", cascade="all, delete-orphan"
    )


class Progress(Base):
    __tablename__ = "progress"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    lesson_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("lessons.id", ondelete="CASCADE"), unique=True, index=True
    )
    completed_section_ids: Mapped[list[str]] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), default=list
    )
    percentage: Mapped[int] = mapped_column(Integer, default=0)
    last_section_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    activity_state: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), default=dict
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    lesson: Mapped[Lesson] = relationship(back_populates="progress")


class SavedAnswer(Base):
    __tablename__ = "saved_answers"
    __table_args__ = (UniqueConstraint("lesson_id", "question_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    lesson_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("lessons.id", ondelete="CASCADE"), index=True
    )
    question_id: Mapped[str] = mapped_column(String(120))
    answer: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    lesson: Mapped[Lesson] = relationship(back_populates="answers")


class Asset(Base):
    __tablename__ = "assets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    mime_type: Mapped[str] = mapped_column(String(64))
    filename: Mapped[str | None] = mapped_column(String(240), nullable=True)
    alt_text: Mapped[str] = mapped_column(Text)
    content: Mapped[bytes] = mapped_column(LargeBinary)
    size_bytes: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

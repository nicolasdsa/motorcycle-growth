import re
import unicodedata
import uuid
from base64 import b64decode
from binascii import Error as Base64Error

from app.generation.builder import build_lesson, regenerate_section
from app.models.lesson import Asset, Lesson, Progress, SavedAnswer
from app.repositories.lessons import LessonRepository
from app.schemas.lesson import (
    AnswerRead,
    AnswerUpdate,
    AssetCreate,
    AssetRead,
    LessonCreate,
    LessonDetail,
    LessonListItem,
    ProgressRead,
    ProgressUpdate,
    LessonSpecification,
)
from app.validation.lesson import validate_lesson, validate_section


class LessonNotFoundError(LookupError):
    pass


class InvalidReferenceError(ValueError):
    pass


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", normalized).strip("-").lower()
    return slug or "aula"


class LessonService:
    def __init__(self, repository: LessonRepository):
        self.repository = repository

    def create(self, request: LessonCreate) -> LessonDetail:
        lesson_id = str(uuid.uuid4())
        specification = build_lesson(request, lesson_id)
        validate_lesson(specification)
        lesson = Lesson(
            id=lesson_id,
            topic=request.topic.strip(),
            slug=f"{slugify(request.topic)}-{lesson_id[:8]}",
            target_role=request.target_role,
            target_level=request.target_level,
            depth=request.depth,
            focus=request.focus,
            language=request.language,
            specification=specification.model_dump(mode="json"),
        )
        self.repository.add(lesson)
        return self._detail(lesson)

    def create_from_specification(self, specification: LessonSpecification) -> LessonDetail:
        """Persist a lesson authored by a trusted authoring tool after full validation."""
        validate_lesson(specification)
        self._validate_asset_references(specification)
        target = specification.target
        lesson_id = str(uuid.uuid4())
        lesson = Lesson(
            id=lesson_id,
            topic=specification.topic,
            slug=f"{slugify(specification.topic)}-{lesson_id[:8]}",
            target_role=target.role,
            target_level=target.level,
            depth=target.depth,
            focus=target.focus,
            language=target.language,
            specification=specification.model_dump(mode="json"),
        )
        self.repository.add(lesson)
        return self._detail(lesson)

    def update_from_specification(self, lesson_id: str, specification: LessonSpecification) -> LessonDetail:
        """Replace an authored specification after the same validation used at import."""
        validate_lesson(specification)
        self._validate_asset_references(specification)
        lesson = self._get_model(lesson_id)
        target = specification.target
        lesson.topic = specification.topic
        lesson.target_role = target.role
        lesson.target_level = target.level
        lesson.depth = target.depth
        lesson.focus = target.focus
        lesson.language = target.language
        lesson.specification = specification.model_dump(mode="json")
        lesson.content_version += 1
        self.repository.save(lesson)
        return self._detail(lesson)

    def create_asset(self, payload: AssetCreate) -> AssetRead:
        try:
            content = b64decode(payload.content_base64, validate=True)
        except Base64Error as exc:
            raise ValueError("Imagem base64 inválida") from exc
        max_size = 5 * 1024 * 1024
        if not content:
            raise ValueError("Imagem vazia")
        if len(content) > max_size:
            raise ValueError("Imagem excede o limite de 5 MB")
        asset = self.repository.add_asset(
            Asset(
                mime_type=payload.mime_type,
                filename=payload.filename,
                alt_text=payload.alt_text,
                content=content,
                size_bytes=len(content),
            )
        )
        return self._asset_read(asset)

    def get_asset(self, asset_id: str) -> Asset:
        asset = self.repository.get_asset(asset_id)
        if not asset:
            raise LessonNotFoundError(asset_id)
        return asset

    def list(self, query: str | None, favorite: bool | None) -> list[LessonListItem]:
        return [self._item(lesson) for lesson in self.repository.list(query, favorite)]

    def get(self, lesson_id: str) -> LessonDetail:
        return self._detail(self._get_model(lesson_id))

    def delete(self, lesson_id: str) -> None:
        self.repository.delete(self._get_model(lesson_id))

    def favorite(self, lesson_id: str, is_favorite: bool) -> LessonDetail:
        lesson = self._get_model(lesson_id)
        lesson.is_favorite = is_favorite
        self.repository.save(lesson)
        return self._detail(lesson)

    def regenerate(self, lesson_id: str, section_id: str) -> LessonDetail:
        lesson = self._get_model(lesson_id)
        current = self._detail(lesson).specification
        if section_id not in {section.id for section in current.sections}:
            raise InvalidReferenceError("Seção não pertence à aula")
        updated = regenerate_section(current, section_id)
        changed = next(section for section in updated.sections if section.id == section_id)
        validate_section(changed)
        validate_lesson(updated)
        lesson.specification = updated.model_dump(mode="json")
        lesson.content_version += 1
        self.repository.save(lesson)
        return self._detail(lesson)

    def get_progress(self, lesson_id: str) -> ProgressRead:
        self._get_model(lesson_id)
        progress = self.repository.get_progress(lesson_id)
        if not progress:
            return ProgressRead(lesson_id=lesson_id, percentage=0)
        return self._progress_read(progress)

    def save_progress(self, lesson_id: str, update: ProgressUpdate) -> ProgressRead:
        lesson = self._get_model(lesson_id)
        section_ids = self._progress_section_ids(lesson.specification)
        completed = list(dict.fromkeys(update.completed_section_ids))
        unknown = set(completed) - section_ids
        if unknown:
            raise InvalidReferenceError(f"Seções desconhecidas: {', '.join(sorted(unknown))}")
        if update.last_section_id and update.last_section_id not in section_ids:
            raise InvalidReferenceError("Última seção não pertence à aula")
        percentage = round(len(completed) / len(section_ids) * 100) if section_ids else 0
        progress = self.repository.get_progress(lesson_id) or Progress(lesson_id=lesson_id)
        progress.completed_section_ids = completed
        progress.last_section_id = update.last_section_id
        progress.activity_state = update.activity_state
        progress.percentage = percentage
        self.repository.save_progress(progress)
        return self._progress_read(progress)

    def save_answer(self, lesson_id: str, question_id: str, update: AnswerUpdate) -> AnswerRead:
        lesson = self._get_model(lesson_id)
        question_ids = {item["id"] for item in lesson.specification["questions"]}
        if question_id not in question_ids:
            raise InvalidReferenceError("Pergunta não pertence à aula")
        answer = self.repository.get_answer(lesson_id, question_id) or SavedAnswer(
            lesson_id=lesson_id, question_id=question_id
        )
        answer.answer = update.answer
        self.repository.save_answer(answer)
        return AnswerRead(
            lesson_id=lesson_id,
            question_id=question_id,
            answer=answer.answer,
            updated_at=answer.updated_at,
        )

    def _get_model(self, lesson_id: str) -> Lesson:
        lesson = self.repository.get(lesson_id)
        if not lesson:
            raise LessonNotFoundError(lesson_id)
        return lesson

    def _item(self, lesson: Lesson) -> LessonListItem:
        specification = lesson.specification
        progress = self.repository.get_progress(lesson.id)
        return LessonListItem(
            id=lesson.id,
            topic=lesson.topic,
            slug=lesson.slug,
            target_role=lesson.target_role,
            target_level=lesson.target_level,
            depth=lesson.depth,
            focus=lesson.focus,
            language=lesson.language,
            is_favorite=lesson.is_favorite,
            content_version=lesson.content_version,
            created_at=lesson.created_at,
            updated_at=lesson.updated_at,
            progress_percentage=progress.percentage if progress else 0,
            domain=specification["lesson_plan"]["domain"],
            description=specification["description"],
        )

    def _detail(self, lesson: Lesson) -> LessonDetail:
        item = self._item(lesson)
        return LessonDetail(**item.model_dump(), specification=lesson.specification)

    def _validate_asset_references(self, specification: LessonSpecification) -> None:
        visuals = [section_visual for section in specification.sections for section_visual in section.visualizations]
        if specification.mental_model.initial_diagram:
            visuals.append(specification.mental_model.initial_diagram)
        missing = [
            visual.asset_id
            for visual in visuals
            if visual.asset_id and not self.repository.get_asset(visual.asset_id)
        ]
        if missing:
            raise InvalidReferenceError(f"Assets desconhecidos: {', '.join(sorted(set(missing)))}")

    @staticmethod
    def _progress_section_ids(specification: dict) -> set[str]:
        """Keep persisted progress aligned with every navigable lesson section."""
        ids = {"opening", "mental-model", "glossary", "tradeoffs", "interview", "summary"}
        ids.update(section["id"] for section in specification["sections"])
        if specification.get("questions"):
            ids.add("questions")
        if specification.get("interactive_activity"):
            ids.add("activity")
        return ids

    @staticmethod
    def _asset_read(asset: Asset) -> AssetRead:
        return AssetRead(
            id=asset.id,
            mime_type=asset.mime_type,
            filename=asset.filename,
            alt_text=asset.alt_text,
            size_bytes=asset.size_bytes,
            url=f"/api/assets/{asset.id}",
            created_at=asset.created_at,
        )

    @staticmethod
    def _progress_read(progress: Progress) -> ProgressRead:
        return ProgressRead(
            lesson_id=progress.lesson_id,
            completed_section_ids=progress.completed_section_ids,
            last_section_id=progress.last_section_id,
            activity_state=progress.activity_state,
            percentage=progress.percentage,
            updated_at=progress.updated_at,
        )

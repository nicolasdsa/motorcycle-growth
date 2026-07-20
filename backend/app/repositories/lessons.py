from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.models.lesson import Asset, Lesson, Progress, SavedAnswer


class LessonRepository:
    def __init__(self, db: Session):
        self.db = db

    def add(self, lesson: Lesson) -> Lesson:
        self.db.add(lesson)
        self.db.commit()
        self.db.refresh(lesson)
        return lesson

    def get(self, lesson_id: str) -> Lesson | None:
        return self.db.get(Lesson, lesson_id)

    def get_by_slug(self, slug: str) -> Lesson | None:
        return self.db.scalar(select(Lesson).where(Lesson.slug == slug))

    def list(self, query: str | None = None, favorite: bool | None = None) -> list[Lesson]:
        statement = select(Lesson).order_by(Lesson.updated_at.desc())
        if query:
            pattern = f"%{query.strip()}%"
            statement = statement.where(or_(Lesson.topic.ilike(pattern), Lesson.slug.ilike(pattern)))
        if favorite is not None:
            statement = statement.where(Lesson.is_favorite == favorite)
        return list(self.db.scalars(statement).all())

    def delete(self, lesson: Lesson) -> None:
        self.db.delete(lesson)
        self.db.commit()

    def save(self, lesson: Lesson) -> Lesson:
        self.db.add(lesson)
        self.db.commit()
        self.db.refresh(lesson)
        return lesson

    def get_progress(self, lesson_id: str) -> Progress | None:
        return self.db.scalar(select(Progress).where(Progress.lesson_id == lesson_id))

    def save_progress(self, progress: Progress) -> Progress:
        self.db.add(progress)
        self.db.commit()
        self.db.refresh(progress)
        return progress

    def get_answer(self, lesson_id: str, question_id: str) -> SavedAnswer | None:
        return self.db.scalar(
            select(SavedAnswer).where(
                SavedAnswer.lesson_id == lesson_id, SavedAnswer.question_id == question_id
            )
        )

    def save_answer(self, answer: SavedAnswer) -> SavedAnswer:
        self.db.add(answer)
        self.db.commit()
        self.db.refresh(answer)
        return answer

    def get_asset(self, asset_id: str) -> Asset | None:
        return self.db.get(Asset, asset_id)

    def add_asset(self, asset: Asset) -> Asset:
        self.db.add(asset)
        self.db.commit()
        self.db.refresh(asset)
        return asset

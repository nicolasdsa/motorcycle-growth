from fastapi import Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.repositories.lessons import LessonRepository
from app.services.lessons import LessonService


async def get_lesson_service(db: Session = Depends(get_db)) -> LessonService:
    return LessonService(LessonRepository(db))

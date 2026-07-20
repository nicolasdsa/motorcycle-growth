from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from app.api.dependencies import get_lesson_service
from app.schemas.lesson import (
    AnswerRead,
    AnswerUpdate,
    FavoriteUpdate,
    LessonCreate,
    LessonDetail,
    LessonListItem,
    ProgressRead,
    ProgressUpdate,
)
from app.services.lessons import InvalidReferenceError, LessonNotFoundError, LessonService
from app.validation.lesson import LessonValidationError


router = APIRouter(prefix="/api")
Service = Annotated[LessonService, Depends(get_lesson_service)]


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/lessons", response_model=LessonDetail, status_code=status.HTTP_201_CREATED)
async def create_lesson(payload: LessonCreate, service: Service) -> LessonDetail:
    try:
        return service.create(payload)
    except LessonValidationError as exc:
        raise HTTPException(status_code=422, detail={"message": "Especificação inválida", "errors": exc.errors}) from exc


@router.get("/lessons", response_model=list[LessonListItem])
async def list_lessons(
    service: Service,
    q: Annotated[str | None, Query(max_length=200)] = None,
    favorite: bool | None = None,
) -> list[LessonListItem]:
    return service.list(q, favorite)


@router.get("/lessons/{lesson_id}", response_model=LessonDetail)
async def get_lesson(lesson_id: str, service: Service) -> LessonDetail:
    return _handle_not_found(lambda: service.get(lesson_id))


@router.delete("/lessons/{lesson_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_lesson(lesson_id: str, service: Service) -> Response:
    _handle_not_found(lambda: service.delete(lesson_id))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch("/lessons/{lesson_id}/favorite", response_model=LessonDetail)
async def favorite_lesson(lesson_id: str, payload: FavoriteUpdate, service: Service) -> LessonDetail:
    return _handle_not_found(lambda: service.favorite(lesson_id, payload.is_favorite))


@router.post("/lessons/{lesson_id}/sections/{section_id}/regenerate", response_model=LessonDetail)
async def regenerate_lesson_section(lesson_id: str, section_id: str, service: Service) -> LessonDetail:
    return _handle_errors(lambda: service.regenerate(lesson_id, section_id))


@router.get("/lessons/{lesson_id}/progress", response_model=ProgressRead)
async def get_progress(lesson_id: str, service: Service) -> ProgressRead:
    return _handle_not_found(lambda: service.get_progress(lesson_id))


@router.put("/lessons/{lesson_id}/progress", response_model=ProgressRead)
async def save_progress(lesson_id: str, payload: ProgressUpdate, service: Service) -> ProgressRead:
    return _handle_errors(lambda: service.save_progress(lesson_id, payload))


@router.put("/lessons/{lesson_id}/answers/{question_id}", response_model=AnswerRead)
async def save_answer(
    lesson_id: str, question_id: str, payload: AnswerUpdate, service: Service
) -> AnswerRead:
    return _handle_errors(lambda: service.save_answer(lesson_id, question_id, payload))


def _handle_not_found(operation):
    try:
        return operation()
    except LessonNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Aula não encontrada") from exc


def _handle_errors(operation):
    try:
        return operation()
    except LessonNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Aula não encontrada") from exc
    except (InvalidReferenceError, KeyError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LessonValidationError as exc:
        raise HTTPException(status_code=422, detail={"message": "Especificação inválida", "errors": exc.errors}) from exc

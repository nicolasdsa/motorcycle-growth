from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.api.dependencies import get_lesson_service
from app.config import get_settings
from app.generation.catalog import REGISTERED_ACTIVITIES, REGISTERED_VISUALIZATIONS
from app.schemas.lesson import (
    AssetCreate,
    AssetRead,
    AuthoringCatalog,
    LessonDetail,
    LessonSpecification,
)
from app.services.lessons import InvalidReferenceError, LessonNotFoundError, LessonService
from app.validation.lesson import LessonValidationError


authoring_router = APIRouter(prefix="/api/authoring", tags=["authoring"])
assets_router = APIRouter(prefix="/api/assets", tags=["assets"])
Service = Annotated[LessonService, Depends(get_lesson_service)]


async def require_authoring_token(
    x_authoring_token: Annotated[str | None, Header()] = None,
) -> None:
    """Allow localhost development by default; require a token when configured."""
    configured_token = get_settings().authoring_token
    if configured_token and x_authoring_token != configured_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token de autoria inválido")


@authoring_router.get("/catalog", response_model=AuthoringCatalog)
async def get_authoring_catalog(_: None = Depends(require_authoring_token)) -> AuthoringCatalog:
    return AuthoringCatalog(
        schema_version="1.0",
        visualizations=sorted(item.value for item in REGISTERED_VISUALIZATIONS),
        activities=sorted(item.value for item in REGISTERED_ACTIVITIES),
        max_asset_size_bytes=5 * 1024 * 1024,
    )


@authoring_router.post("/assets", response_model=AssetRead, status_code=status.HTTP_201_CREATED)
async def create_asset(
    payload: AssetCreate,
    service: Service,
    _: None = Depends(require_authoring_token),
) -> AssetRead:
    try:
        return service.create_asset(payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@authoring_router.post("/lessons", response_model=LessonDetail, status_code=status.HTTP_201_CREATED)
async def create_authored_lesson(
    payload: LessonSpecification,
    service: Service,
    _: None = Depends(require_authoring_token),
) -> LessonDetail:
    try:
        return service.create_from_specification(payload)
    except InvalidReferenceError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LessonValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Especificação inválida", "errors": exc.errors},
        ) from exc


@authoring_router.put("/lessons/{lesson_id}", response_model=LessonDetail)
async def update_authored_lesson(
    lesson_id: str,
    payload: LessonSpecification,
    service: Service,
    _: None = Depends(require_authoring_token),
) -> LessonDetail:
    try:
        return service.update_from_specification(lesson_id, payload)
    except InvalidReferenceError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LessonNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Aula não encontrada") from exc
    except LessonValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Especificação inválida", "errors": exc.errors},
        ) from exc


@assets_router.get("/{asset_id}")
async def read_asset(asset_id: str, service: Service) -> Response:
    try:
        asset = service.get_asset(asset_id)
    except LessonNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Asset não encontrado") from exc
    return Response(
        content=asset.content,
        media_type=asset.mime_type,
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.api.dependencies import get_lesson_service
from app.database import Base
from app.main import app
from app.repositories.lessons import LessonRepository
from app.services.lessons import LessonService


@pytest.fixture
def db_session() -> Session:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)


@pytest_asyncio.fixture
async def client(db_session: Session):
    async def override_service() -> LessonService:
        return LessonService(LessonRepository(db_session))

    app.dependency_overrides[get_lesson_service] = override_service
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as test_client:
        yield test_client
    app.dependency_overrides.clear()

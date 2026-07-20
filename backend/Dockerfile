FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app
RUN pip install --no-cache-dir poetry==2.3.4
COPY pyproject.toml poetry.lock ./
RUN poetry install --only main --no-root --no-interaction
COPY alembic.ini ./
COPY alembic ./alembic
COPY app ./app
EXPOSE 8000
CMD ["sh", "-c", "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"]


import pytest
from base64 import b64encode
from httpx import AsyncClient

from app.generation.builder import build_lesson
from app.schemas.lesson import LessonCreate


async def create_lesson(client: AsyncClient) -> dict:
    response = await client.post(
        "/api/lessons",
        json={
            "topic": "Load balancing",
            "target_role": "Backend",
            "target_level": "Pleno",
            "depth": "aula normal",
            "focus": "system design",
            "language": "pt-BR",
        },
    )
    assert response.status_code == 201
    return response.json()


@pytest.mark.asyncio
async def test_full_lesson_progress_and_answer_flow(client: AsyncClient) -> None:
    lesson = await create_lesson(client)
    lesson_id = lesson["id"]
    assert lesson["specification"]["summary"]["concept_map"]["nodes"]
    section_id = lesson["specification"]["sections"][0]["id"]
    question_id = lesson["specification"]["questions"][0]["id"]

    listing = await client.get("/api/lessons?q=load")
    assert listing.status_code == 200
    assert listing.json()[0]["domain"] == "distributed-systems"

    progress = await client.put(
        f"/api/lessons/{lesson_id}/progress",
        json={
            "completed_section_ids": [section_id],
            "last_section_id": section_id,
            "activity_state": {"traffic": 1200},
        },
    )
    assert progress.status_code == 200
    assert progress.json()["percentage"] == 8

    answer = await client.put(
        f"/api/lessons/{lesson_id}/answers/{question_id}",
        json={"answer": "Distribui tráfego entre destinos elegíveis."},
    )
    assert answer.status_code == 200

    favorite = await client.patch(
        f"/api/lessons/{lesson_id}/favorite", json={"is_favorite": True}
    )
    assert favorite.json()["is_favorite"] is True

    regenerated = await client.post(
        f"/api/lessons/{lesson_id}/sections/{section_id}/regenerate"
    )
    assert regenerated.status_code == 200
    assert regenerated.json()["content_version"] == 2
    assert len(regenerated.json()["specification"]["sections"]) == 4

    restored = await client.get(f"/api/lessons/{lesson_id}/progress")
    assert restored.json()["activity_state"] == {"traffic": 1200}


@pytest.mark.asyncio
async def test_rejects_unknown_progress_reference(client: AsyncClient) -> None:
    lesson = await create_lesson(client)
    response = await client.put(
        f"/api/lessons/{lesson['id']}/progress",
        json={"completed_section_ids": ["missing"], "activity_state": {}},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_delete_returns_not_found_afterwards(client: AsyncClient) -> None:
    lesson = await create_lesson(client)
    assert (await client.delete(f"/api/lessons/{lesson['id']}")).status_code == 204
    assert (await client.get(f"/api/lessons/{lesson['id']}")).status_code == 404


@pytest.mark.asyncio
async def test_authoring_import_validates_assets_and_persists_specification(
    client: AsyncClient,
) -> None:
    catalog = await client.get("/api/authoring/catalog")
    assert catalog.status_code == 200
    assert "request-flow" in catalog.json()["visualizations"]

    uploaded = await client.post(
        "/api/authoring/assets",
        json={
            "filename": "pixel.png",
            "mime_type": "image/png",
            "alt_text": "Imagem de teste com um pixel transparente.",
            "content_base64": b64encode(b"tiny-png-content").decode(),
        },
    )
    assert uploaded.status_code == 201
    asset = uploaded.json()
    assert (await client.get(asset["url"])).content == b"tiny-png-content"

    spec = build_lesson(LessonCreate(topic="Load balancing"), "authoring-spec").model_dump(mode="json")
    spec["mental_model"]["initial_diagram"]["asset_id"] = asset["id"]
    authored = await client.post("/api/authoring/lessons", json=spec)
    assert authored.status_code == 201
    assert authored.json()["specification"]["mental_model"]["initial_diagram"]["asset_id"] == asset["id"]

    spec["mental_model"]["initial_diagram"]["asset_id"] = "missing-asset"
    rejected = await client.post("/api/authoring/lessons", json=spec)
    assert rejected.status_code == 422

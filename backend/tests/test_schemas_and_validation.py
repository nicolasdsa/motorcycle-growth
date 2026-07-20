from copy import deepcopy

import pytest

from app.generation.builder import build_lesson
from app.generation.classifier import classify_topic
from app.schemas.lesson import Breadth, Domain, LessonCreate, LessonSpecification
from app.validation.lesson import LessonValidationError, validate_lesson


def make_spec() -> LessonSpecification:
    return build_lesson(LessonCreate(topic="Load balancing"), "lesson-id")


def test_load_balancing_spec_is_complete_and_valid() -> None:
    spec = make_spec()
    validate_lesson(spec)
    assert spec.lesson_plan.visual_support == "specialized"
    assert len(spec.sections) == 4
    assert spec.tradeoffs and spec.sources and spec.questions
    assert spec.interactive_activity.type == "simulation-playground"


def test_classifier_distinguishes_specific_and_domain() -> None:
    domain, breadth, _ = classify_topic("Por que Quick Sort pode atingir O(n²)?")
    assert domain == Domain.ALGORITHMS
    assert breadth == Breadth.SPECIFIC


def test_generic_fallback_remains_valid() -> None:
    spec = build_lesson(LessonCreate(topic="Garbage collection", target_level="Sênior"), "id")
    validate_lesson(spec)
    assert spec.lesson_plan.visual_support == "generic"
    assert spec.target.level == "Sênior"


def test_rejects_broken_visual_relation() -> None:
    payload = make_spec().model_dump(mode="json")
    payload["mental_model"]["initial_diagram"]["relations"][0]["target"] = "missing"
    with pytest.raises(LessonValidationError, match="Relação quebrada"):
        validate_lesson(LessonSpecification.model_validate(payload))


def test_rejects_executable_payload() -> None:
    spec = make_spec()
    payload = deepcopy(spec.model_dump(mode="json"))
    payload["interactive_activity"]["config"]["handler"] = "javascript:alert(1)"
    unsafe = LessonSpecification.model_validate(payload)
    with pytest.raises(LessonValidationError, match="executável proibido"):
        validate_lesson(unsafe)


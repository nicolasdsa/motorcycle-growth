import re
from typing import Any

from app.generation.catalog import (
    REGISTERED_ACTIVITIES,
    REGISTERED_VISUALIZATIONS,
    SPECIALIZED_VISUALIZATIONS,
)
from app.schemas.lesson import LessonSection, LessonSpecification, VisualSupport


class LessonValidationError(ValueError):
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


FORBIDDEN_KEYS = {"html", "javascript", "script", "eval", "componentcode", "vue", "handler"}
FORBIDDEN_PATTERNS = (
    re.compile(r"<\s*script", re.IGNORECASE),
    re.compile(r"javascript\s*:", re.IGNORECASE),
    re.compile(r"\bon(?:click|load|error|mouseover)\s*=", re.IGNORECASE),
    re.compile(r"\beval\s*\(", re.IGNORECASE),
)


def _scan(value: Any, path: str, errors: list[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if str(key).lower() in FORBIDDEN_KEYS:
                errors.append(f"Campo executável proibido em {path}.{key}")
            _scan(child, f"{path}.{key}", errors)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _scan(child, f"{path}[{index}]", errors)
    elif isinstance(value, str):
        for pattern in FORBIDDEN_PATTERNS:
            if pattern.search(value):
                errors.append(f"Conteúdo executável proibido em {path}")
                break


def _collect_ids(spec: LessonSpecification) -> tuple[set[str], list[str]]:
    ids: list[str] = [spec.id]
    ids.extend(term.id for term in spec.glossary)
    ids.extend(section.id for section in spec.sections)
    ids.extend(block.id for section in spec.sections for block in section.blocks)
    ids.extend(visual.id for section in spec.sections for visual in section.visualizations)
    if spec.mental_model.initial_diagram:
        ids.append(spec.mental_model.initial_diagram.id)
    ids.extend(example.id for example in spec.examples)
    ids.extend(item.id for item in spec.tradeoffs)
    ids.extend(item.id for item in spec.edge_cases)
    ids.extend(item.id for item in spec.questions)
    ids.extend(item.id for item in spec.sources)
    ids.append(spec.interactive_activity.id)
    seen: set[str] = set()
    duplicates: list[str] = []
    for item_id in ids:
        if item_id in seen:
            duplicates.append(item_id)
        seen.add(item_id)
    return seen, duplicates


def validate_section(section: LessonSection) -> None:
    errors: list[str] = []
    for visual in section.visualizations:
        if visual.type not in REGISTERED_VISUALIZATIONS:
            errors.append(f"Visualização não registrada: {visual.type}")
    _scan(section.model_dump(mode="json"), f"section.{section.id}", errors)
    if errors:
        raise LessonValidationError(errors)


def validate_lesson(spec: LessonSpecification) -> None:
    errors: list[str] = []
    _, duplicates = _collect_ids(spec)
    if duplicates:
        errors.append(f"IDs duplicados: {', '.join(sorted(set(duplicates)))}")

    visuals = []
    if spec.mental_model.initial_diagram:
        visuals.append(spec.mental_model.initial_diagram)
    visuals.extend(visual for section in spec.sections for visual in section.visualizations)
    unknown = sorted({str(visual.type) for visual in visuals if visual.type not in REGISTERED_VISUALIZATIONS})
    if unknown:
        errors.append(f"Visualizações não registradas: {', '.join(unknown)}")

    if spec.interactive_activity.type not in REGISTERED_ACTIVITIES:
        errors.append(f"Atividade não registrada: {spec.interactive_activity.type}")
    if spec.lesson_plan.visual_support == VisualSupport.SPECIALIZED and not any(
        visual.type in SPECIALIZED_VISUALIZATIONS for visual in visuals
    ):
        errors.append("Suporte specialized requer uma visualização especializada registrada")
    if spec.lesson_plan.visual_support == VisualSupport.TEXTUAL and visuals:
        errors.append("Suporte textual não pode declarar visualizações")

    element_ids = {
        element.id for visual in visuals for element in visual.elements
    }
    for visual in visuals:
        local_ids = {element.id for element in visual.elements}
        for relation in visual.relations:
            if relation.source not in local_ids or relation.target not in local_ids:
                errors.append(f"Relação quebrada em {visual.id}: {relation.source} → {relation.target}")
    del element_ids

    _scan(spec.model_dump(mode="json"), "lesson", errors)
    if errors:
        raise LessonValidationError(errors)

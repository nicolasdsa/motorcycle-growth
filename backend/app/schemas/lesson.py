from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class Breadth(StrEnum):
    BROAD = "broad"
    FOCUSED = "focused"
    SPECIFIC = "specific"


class Domain(StrEnum):
    ALGORITHMS = "algorithms"
    DATA_STRUCTURES = "data-structures"
    DATABASES = "databases"
    DISTRIBUTED_SYSTEMS = "distributed-systems"
    SOFTWARE_ENGINEERING = "software-engineering"
    SOFTWARE_ARCHITECTURE = "software-architecture"
    BACKEND = "backend"
    FRONTEND = "frontend"
    NETWORKING = "networking"
    OPERATING_SYSTEMS = "operating-systems"
    CONCURRENCY = "concurrency"
    SECURITY = "security"
    INFRASTRUCTURE = "infrastructure"
    CLOUD = "cloud"
    DEVOPS = "devops"
    OBSERVABILITY = "observability"
    TESTING = "testing"
    LANGUAGE_RUNTIME = "language-runtime"
    COMPUTER_SCIENCE = "computer-science"
    SYSTEM_DESIGN = "system-design"
    GENERAL = "general"


class Archetype(StrEnum):
    CONCEPT = "concept-explanation"
    ALGORITHM = "algorithm-walkthrough"
    DATA_STRUCTURE = "data-structure-exploration"
    ARCHITECTURE = "architecture-evolution"
    TECHNOLOGY = "technology-overview"
    PROTOCOL = "protocol-flow"
    COMPARATIVE = "comparative-analysis"
    REFACTORING = "progressive-refactoring"
    DATABASE = "database-scenario"
    CONCURRENCY = "concurrency-scenario"
    DEBUGGING = "debugging-diagnosis"
    CODE_REVIEW = "code-review"
    SYSTEM_DESIGN = "system-design"
    INTERVIEW = "interview-drill"


class VisualSupport(StrEnum):
    SPECIALIZED = "specialized"
    GENERIC = "generic"
    TEXTUAL = "textual"


class VisualizationType(StrEnum):
    STEP_BY_STEP = "step-by-step"
    ANNOTATED_DIAGRAM = "annotated-diagram"
    COMPARISON_TABLE = "comparison-table"
    DECISION_MATRIX = "decision-matrix"
    TIMELINE = "timeline"
    SEQUENCE_DIAGRAM = "sequence-diagram"
    STATE_MACHINE = "state-machine"
    FLOWCHART = "flowchart"
    BEFORE_AFTER = "before-after"
    CONCEPT_MAP = "concept-map"
    METRIC_CHART = "metric-chart"
    CODE_WALKTHROUGH = "code-walkthrough"
    CODE_DIFF = "code-diff"
    INTERACTIVE_QUIZ = "interactive-quiz"
    FLASHCARDS = "flashcards"
    CALLOUT = "callout-example"
    ARRAY = "array-visualizer"
    SORTING = "sorting-visualizer"
    BINARY_SEARCH = "binary-search-visualizer"
    COMPLEXITY = "complexity-growth-chart"
    TABLE_STATE = "table-state"
    TRANSACTION_TIMELINE = "transaction-timeline"
    CONCURRENT_TRANSACTION = "concurrent-transaction-view"
    LOCK = "lock-visualizer"
    MVCC = "mvcc-version-view"
    INDEX_TREE = "index-tree"
    REQUEST_FLOW = "request-flow"
    SERVER_CLUSTER = "server-cluster"
    LOAD_DISTRIBUTION = "load-distribution"
    QUEUE_FLOW = "queue-flow"
    CACHE_FLOW = "cache-flow"
    RETRY_FLOW = "retry-flow"
    DEPENDENCY = "dependency-diagram"
    CHANGE_IMPACT = "change-impact-map"
    ARCHITECTURE_LAYER = "architecture-layer-view"
    PROTOCOL_SEQUENCE = "protocol-sequence"
    AUTHENTICATION_FLOW = "authentication-flow"
    DNS_FLOW = "dns-resolution-flow"
    TCP_STATE = "tcp-state-machine"
    TRUST_BOUNDARY = "trust-boundary-map"


class ActivityType(StrEnum):
    SIMULATION = "simulation-playground"
    ALGORITHM = "algorithm-execution"
    TRANSACTION = "transaction-lab"
    REFACTORING = "refactoring-challenge"
    ARCHITECTURE = "architecture-decision-challenge"
    DEBUGGING = "debugging-scenario"
    CODE_COMPLETION = "code-completion"
    CODE_REVIEW = "code-review-exercise"
    CLASSIFICATION = "concept-classification"
    COMPARISON = "comparison-explorer"
    PARAMETERS = "parameter-explorer"
    INTERVIEW = "interview-simulator"
    QUIZ = "guided-quiz"
    STEPPER = "interactive-stepper"


class LessonPlan(BaseModel):
    domain: Domain
    secondary_domains: list[Domain] = Field(default_factory=list, max_length=5)
    breadth: Breadth
    archetypes: list[Archetype] = Field(min_length=1, max_length=5)
    visual_support: VisualSupport
    prerequisites: list[str] = Field(default_factory=list, max_length=12)
    learning_objectives: list[str] = Field(min_length=1, max_length=12)
    excluded_topics: list[str] = Field(default_factory=list, max_length=12)


class TargetAudience(BaseModel):
    role: str = Field(min_length=1, max_length=32)
    level: str = Field(min_length=1, max_length=16)
    depth: str = Field(min_length=1, max_length=16)
    focus: str | None = Field(default=None, max_length=64)
    language: str = Field(default="pt-BR", min_length=2, max_length=16)


class Introduction(BaseModel):
    interview_context: str = Field(min_length=10, max_length=1200)
    opening_question: str = Field(min_length=10, max_length=500)
    concrete_scenario: str = Field(min_length=10, max_length=1600)
    learning_objectives: list[str] = Field(min_length=1, max_length=10)


class DiagramElement(BaseModel):
    id: str = Field(min_length=1, max_length=80)
    label: str = Field(min_length=1, max_length=120)
    kind: str = Field(default="node", max_length=40)
    description: str = Field(default="", max_length=500)


class DiagramRelation(BaseModel):
    source: str = Field(min_length=1, max_length=80)
    target: str = Field(min_length=1, max_length=80)
    label: str = Field(default="", max_length=120)


class Visualization(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    type: VisualizationType
    title: str = Field(min_length=1, max_length=160)
    teaching_goal: str = Field(min_length=5, max_length=600)
    elements: list[DiagramElement] = Field(default_factory=list, max_length=30)
    relations: list[DiagramRelation] = Field(default_factory=list, max_length=50)
    initial_state: dict[str, Any] = Field(default_factory=dict)
    steps: list[dict[str, Any]] = Field(default_factory=list, max_length=30)
    captions: list[str] = Field(default_factory=list, max_length=20)
    controls: list[str] = Field(default_factory=list, max_length=10)
    accessible_description: str = Field(min_length=10, max_length=2000)
    asset_id: str | None = Field(default=None, min_length=1, max_length=36)
    data: dict[str, Any] = Field(default_factory=dict)


class MentalModel(BaseModel):
    problem: str = Field(min_length=10, max_length=1200)
    guarantees: list[str] = Field(min_length=1, max_length=10)
    non_guarantees: list[str] = Field(min_length=1, max_length=10)
    analogy: str = Field(min_length=10, max_length=800)
    technical_definition: str = Field(min_length=10, max_length=1600)
    initial_diagram: Visualization | None = None


class GlossaryTerm(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    term: str = Field(min_length=1, max_length=100)
    simple_definition: str = Field(min_length=5, max_length=600)
    technical_definition: str = Field(min_length=5, max_length=1000)
    example: str = Field(min_length=3, max_length=800)
    related_terms: list[str] = Field(default_factory=list, max_length=8)
    interview_relevance: str = Field(min_length=5, max_length=600)
    common_misconception: str = Field(min_length=5, max_length=600)


class ContentBlock(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    kind: Literal["paragraph", "bullets", "callout", "code", "quote"]
    title: str | None = Field(default=None, max_length=160)
    text: str | None = Field(default=None, max_length=5000)
    items: list[str] = Field(default_factory=list, max_length=20)
    language: str | None = Field(default=None, max_length=40)


class LessonSection(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    title: str = Field(min_length=1, max_length=180)
    eyebrow: str = Field(default="", max_length=80)
    scenario: str = Field(default="", max_length=1200)
    observed_problem: str = Field(default="", max_length=1200)
    hypothesis: str = Field(default="", max_length=1200)
    mechanism: str = Field(min_length=10, max_length=2400)
    result: str = Field(default="", max_length=1200)
    benefit: str = Field(default="", max_length=1000)
    limitation: str = Field(default="", max_length=1000)
    transition: str = Field(default="", max_length=800)
    blocks: list[ContentBlock] = Field(min_length=1, max_length=20)
    visualizations: list[Visualization] = Field(default_factory=list, max_length=4)


class Example(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    title: str = Field(min_length=1, max_length=160)
    scenario: str = Field(min_length=5, max_length=1200)
    steps: list[str] = Field(min_length=1, max_length=15)
    result: str = Field(min_length=5, max_length=1000)
    note: str = Field(default="", max_length=800)


class Alternative(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    how_it_works: str = Field(min_length=5, max_length=1000)
    benefits: list[str] = Field(min_length=1, max_length=10)
    disadvantages: list[str] = Field(min_length=1, max_length=10)
    complexity: str = Field(min_length=1, max_length=500)
    performance_impact: str = Field(min_length=1, max_length=500)
    maintenance_impact: str = Field(min_length=1, max_length=500)
    operational_impact: str = Field(min_length=1, max_length=500)
    cost_impact: str = Field(min_length=1, max_length=500)
    use_when: list[str] = Field(min_length=1, max_length=8)
    avoid_when: list[str] = Field(min_length=1, max_length=8)
    risks: list[str] = Field(default_factory=list, max_length=8)
    inadequate_signs: list[str] = Field(default_factory=list, max_length=8)


class Tradeoff(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    decision: str = Field(min_length=5, max_length=500)
    axes: list[str] = Field(min_length=1, max_length=8)
    alternatives: list[Alternative] = Field(min_length=2, max_length=6)
    contextual_recommendation: str = Field(min_length=10, max_length=1200)


class EdgeCase(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    scenario: str = Field(min_length=5, max_length=600)
    effect: str = Field(min_length=5, max_length=800)
    detection: str = Field(min_length=5, max_length=800)
    mitigation: str = Field(min_length=5, max_length=800)


class InterviewGuide(BaseModel):
    evaluates: list[str] = Field(min_length=1, max_length=12)
    clarifying_questions: list[str] = Field(default_factory=list, max_length=12)
    answer_30_seconds: str = Field(min_length=20, max_length=1800)
    answer_2_minutes: str = Field(min_length=50, max_length=5000)
    deep_dive_prompts: list[str] = Field(min_length=1, max_length=15)
    seniority_expectations: dict[str, list[str]]


class InterviewQuestion(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    category: Literal[
        "fundamentals", "application", "comparison", "diagnosis", "edge-cases", "implementation", "deep-dive"
    ]
    difficulty: Literal["easy", "medium", "hard"]
    prompt: str = Field(min_length=5, max_length=1000)
    expected_answer: str = Field(min_length=5, max_length=2500)
    essential_points: list[str] = Field(min_length=1, max_length=10)
    differentiators: list[str] = Field(default_factory=list, max_length=8)
    superficial_signals: list[str] = Field(default_factory=list, max_length=8)
    common_errors: list[str] = Field(default_factory=list, max_length=8)
    follow_ups: list[str] = Field(default_factory=list, max_length=8)


class InteractiveActivity(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    type: ActivityType
    title: str = Field(min_length=1, max_length=180)
    teaching_goal: str = Field(min_length=10, max_length=1000)
    instructions: list[str] = Field(min_length=1, max_length=12)
    config: dict[str, Any] = Field(default_factory=dict)
    accessible_description: str = Field(min_length=10, max_length=1600)


class ConceptMapNode(BaseModel):
    id: str = Field(min_length=1, max_length=80)
    label: str = Field(min_length=1, max_length=48)
    summary: str = Field(min_length=5, max_length=500)
    mnemonic: str = Field(min_length=2, max_length=240)
    kind: Literal["core", "concept", "decision", "risk", "metric"]


class ConceptMapEdge(BaseModel):
    source: str = Field(min_length=1, max_length=80)
    target: str = Field(min_length=1, max_length=80)
    label: str | None = Field(default=None, max_length=120)


class ConceptMap(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    title: str = Field(min_length=1, max_length=160)
    teaching_goal: str = Field(min_length=10, max_length=600)
    nodes: list[ConceptMapNode] = Field(min_length=6, max_length=16)
    edges: list[ConceptMapEdge] = Field(min_length=5, max_length=28)
    accessible_description: str = Field(min_length=10, max_length=2000)


class Summary(BaseModel):
    key_points: list[str] = Field(min_length=1, max_length=12)
    interview_checklist: list[str] = Field(min_length=1, max_length=12)
    next_topics: list[str] = Field(default_factory=list, max_length=8)
    concept_map: ConceptMap


class Source(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    title: str = Field(min_length=1, max_length=240)
    url: HttpUrl
    type: Literal["official-docs", "rfc", "paper", "book", "engineering-article", "project-docs"]
    organization_or_authors: str = Field(min_length=1, max_length=240)
    year: int | None = Field(default=None, ge=1900, le=2100)
    supports: list[str] = Field(min_length=1, max_length=15)


class LessonSpecification(BaseModel):
    schema_version: Literal["1.0"] = "1.0"
    id: str = Field(min_length=1, max_length=100)
    topic: str = Field(min_length=1, max_length=200)
    title: str = Field(min_length=1, max_length=240)
    description: str = Field(min_length=10, max_length=800)
    lesson_plan: LessonPlan
    target: TargetAudience
    introduction: Introduction
    mental_model: MentalModel
    glossary: list[GlossaryTerm] = Field(min_length=2, max_length=30)
    sections: list[LessonSection] = Field(min_length=2, max_length=30)
    examples: list[Example] = Field(default_factory=list, max_length=12)
    tradeoffs: list[Tradeoff] = Field(min_length=1, max_length=10)
    edge_cases: list[EdgeCase] = Field(min_length=1, max_length=15)
    interview_guide: InterviewGuide
    questions: list[InterviewQuestion] = Field(min_length=2, max_length=30)
    interactive_activity: InteractiveActivity
    summary: Summary
    sources: list[Source] = Field(min_length=1, max_length=30)
    limitations: list[str] = Field(min_length=1, max_length=12)

    @field_validator("topic", "title")
    @classmethod
    def strip_text(cls, value: str) -> str:
        return value.strip()


class LessonCreate(BaseModel):
    topic: str = Field(min_length=2, max_length=200)
    target_role: Literal["Backend", "Frontend", "Full Stack", "DevOps", "SRE", "Mobile", "Data", "Machine Learning", "Geral"] = "Backend"
    target_level: Literal["Júnior", "Pleno", "Sênior"] = "Pleno"
    depth: Literal["resumo", "aula normal", "aprofundada"] = "aula normal"
    focus: str | None = Field(default=None, max_length=64)
    language: str = Field(default="pt-BR", min_length=2, max_length=16)


class LessonListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    topic: str
    slug: str
    target_role: str
    target_level: str
    depth: str
    focus: str | None
    language: str
    is_favorite: bool
    content_version: int
    created_at: datetime
    updated_at: datetime
    progress_percentage: int = 0
    domain: str = "general"
    description: str = ""


class LessonDetail(LessonListItem):
    specification: LessonSpecification


class FavoriteUpdate(BaseModel):
    is_favorite: bool


class ProgressUpdate(BaseModel):
    completed_section_ids: list[str] = Field(default_factory=list, max_length=50)
    last_section_id: str | None = Field(default=None, max_length=100)
    activity_state: dict[str, Any] = Field(default_factory=dict)


class ProgressRead(ProgressUpdate):
    lesson_id: str
    percentage: int = Field(ge=0, le=100)
    updated_at: datetime | None = None


class AnswerUpdate(BaseModel):
    answer: str = Field(max_length=10000)


class AnswerRead(AnswerUpdate):
    lesson_id: str
    question_id: str
    updated_at: datetime


class AssetCreate(BaseModel):
    """Imagem original em base64, destinada ao fluxo interno de autoria."""

    filename: str | None = Field(default=None, max_length=240)
    mime_type: Literal["image/png", "image/jpeg", "image/webp", "image/gif"]
    alt_text: str = Field(min_length=10, max_length=2000)
    content_base64: str = Field(min_length=4, max_length=7_000_000)


class AssetRead(BaseModel):
    id: str
    mime_type: str
    filename: str | None
    alt_text: str
    size_bytes: int
    url: str
    created_at: datetime


class AuthoringCatalog(BaseModel):
    schema_version: str
    visualizations: list[str]
    activities: list[str]
    max_asset_size_bytes: int

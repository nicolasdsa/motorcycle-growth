export type LessonCreate = {
  topic: string
  target_role: string
  target_level: string
  depth: string
  focus: string | null
  language: string
}

export type DiagramElement = { id: string; label: string; kind: string; description: string }
export type DiagramRelation = { source: string; target: string; label: string }

export type ConceptMapNode = {
  id: string
  label: string
  summary: string
  mnemonic: string
  kind: 'core' | 'concept' | 'decision' | 'risk' | 'metric'
}

export type ConceptMapEdge = {
  source: string
  target: string
  label?: string
}

export type ConceptMap = {
  id: string
  title: string
  teaching_goal: string
  nodes: ConceptMapNode[]
  edges: ConceptMapEdge[]
  accessible_description: string
}

export type Visualization = {
  id: string
  type: string
  title: string
  teaching_goal: string
  elements: DiagramElement[]
  relations: DiagramRelation[]
  initial_state: Record<string, unknown>
  steps: Record<string, unknown>[]
  captions: string[]
  controls: string[]
  accessible_description: string
  asset_id: string | null
  data: Record<string, unknown>
}

export type ContentBlock = {
  id: string
  kind: 'paragraph' | 'bullets' | 'callout' | 'code' | 'quote'
  title?: string
  text?: string
  items: string[]
  language?: string
}

export type LessonSection = {
  id: string
  title: string
  eyebrow: string
  scenario: string
  observed_problem: string
  hypothesis: string
  mechanism: string
  result: string
  benefit: string
  limitation: string
  transition: string
  blocks: ContentBlock[]
  visualizations: Visualization[]
}

export type InterviewQuestion = {
  id: string
  category: string
  difficulty: string
  prompt: string
  expected_answer: string
  essential_points: string[]
  differentiators: string[]
  superficial_signals: string[]
  common_errors: string[]
  follow_ups: string[]
}

export type InteractiveActivity = {
  id: string
  type: string
  title: string
  teaching_goal: string
  instructions: string[]
  config: Record<string, unknown>
  accessible_description: string
}

export type LessonSpecification = {
  schema_version: string
  id: string
  topic: string
  title: string
  description: string
  lesson_plan: {
    domain: string
    secondary_domains: string[]
    breadth: string
    archetypes: string[]
    visual_support: string
    prerequisites: string[]
    learning_objectives: string[]
    excluded_topics: string[]
  }
  target: { role: string; level: string; depth: string; focus: string | null; language: string }
  introduction: {
    interview_context: string
    opening_question: string
    concrete_scenario: string
    learning_objectives: string[]
  }
  mental_model: {
    problem: string
    guarantees: string[]
    non_guarantees: string[]
    analogy: string
    technical_definition: string
    initial_diagram: Visualization | null
  }
  glossary: Array<{
    id: string
    term: string
    simple_definition: string
    technical_definition: string
    example: string
    related_terms: string[]
    interview_relevance: string
    common_misconception: string
  }>
  sections: LessonSection[]
  examples: Array<{ id: string; title: string; scenario: string; steps: string[]; result: string; note: string }>
  tradeoffs: Array<{
    id: string
    decision: string
    axes: string[]
    alternatives: Array<{
      name: string
      how_it_works: string
      benefits: string[]
      disadvantages: string[]
      complexity: string
      performance_impact: string
      maintenance_impact: string
      operational_impact: string
      cost_impact: string
      use_when: string[]
      avoid_when: string[]
      risks: string[]
      inadequate_signs: string[]
    }>
    contextual_recommendation: string
  }>
  edge_cases: Array<{ id: string; scenario: string; effect: string; detection: string; mitigation: string }>
  interview_guide: {
    evaluates: string[]
    clarifying_questions: string[]
    answer_30_seconds: string
    answer_2_minutes: string
    deep_dive_prompts: string[]
    seniority_expectations: Record<string, string[]>
  }
  questions: InterviewQuestion[]
  interactive_activity: InteractiveActivity
  summary: { key_points: string[]; interview_checklist: string[]; next_topics: string[]; concept_map: ConceptMap }
  sources: Array<{ id: string; title: string; url: string; type: string; organization_or_authors: string; year?: number; supports: string[] }>
  limitations: string[]
}

export type LessonListItem = {
  id: string
  topic: string
  slug: string
  target_role: string
  target_level: string
  depth: string
  focus: string | null
  language: string
  is_favorite: boolean
  content_version: number
  created_at: string
  updated_at: string
  progress_percentage: number
  domain: string
  description: string
}

export type LessonDetail = LessonListItem & { specification: LessonSpecification }

export type Progress = {
  lesson_id: string
  completed_section_ids: string[]
  last_section_id: string | null
  activity_state: Record<string, unknown>
  percentage: number
  updated_at?: string
}

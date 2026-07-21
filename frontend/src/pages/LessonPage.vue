<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import ActivityRenderer from '../activities/ActivityRenderer.vue'
import AppIcon from '../components/AppIcon.vue'
import ConceptMap from '../lesson/ConceptMap.vue'
import GlossaryGrid from '../lesson/GlossaryGrid.vue'
import QuestionPractice from '../lesson/QuestionPractice.vue'
import { api } from '../services/api'
import type { LessonDetail, Progress } from '../types/lesson'
import VisualizationRenderer from '../visualizations/VisualizationRenderer.vue'

const route = useRoute()
const lesson = ref<LessonDetail | null>(null)
const progress = ref<Progress | null>(null)
const loading = ref(true)
const error = ref('')
const activeSection = ref('opening')
const mobileNavOpen = ref(false)
let saveTimer: ReturnType<typeof setTimeout> | null = null

const spec = computed(() => lesson.value?.specification ?? null)
const completed = computed(() => new Set(progress.value?.completed_section_ids ?? []))
const navItems = computed(() => {
  if (!spec.value) return []
  return [
    { id: 'opening', label: 'Abertura' },
    { id: 'mental-model', label: 'Modelo mental' },
    { id: 'glossary', label: 'Termos essenciais' },
    ...spec.value.sections.map((section) => ({ id: section.id, label: section.title })),
    { id: 'tradeoffs', label: 'Trade-offs' },
    { id: 'interview', label: 'Como discutir' },
    { id: 'questions', label: 'Perguntas' },
    { id: 'activity', label: 'Laboratório' },
    { id: 'summary', label: 'Resumo e fontes' },
  ]
})
const autoCompletableIds = computed(() => new Set(navItems.value
  .filter((item) => item.id !== 'questions' && item.id !== 'activity')
  .map((item) => item.id)))

async function load() {
  loading.value = true
  error.value = ''
  try {
    const id = String(route.params.id)
    const [lessonData, progressData] = await Promise.all([api.getLesson(id), api.getProgress(id)])
    lesson.value = lessonData
    progress.value = progressData
    await nextTick()
    setupObserver()
  } catch (err) {
    error.value = err instanceof Error ? err.message : 'Não foi possível carregar esta aula.'
  } finally {
    loading.value = false
  }
}

function setupObserver() {
  updateActiveSection()
  window.addEventListener('scroll', updateActiveSection, { passive: true })
}

function queueProgressSave() {
  if (saveTimer) clearTimeout(saveTimer)
  saveTimer = setTimeout(async () => {
    if (!progress.value || !lesson.value) return
    progress.value = await api.saveProgress(lesson.value.id, {
      completed_section_ids: progress.value.completed_section_ids,
      last_section_id: progress.value.last_section_id,
      activity_state: progress.value.activity_state,
    })
  }, 350)
}

function completeOnVisit(sectionId: string) {
  if (!autoCompletableIds.value.has(sectionId)) return
  markComplete(sectionId)
}

function markComplete(sectionId: string) {
  if (!progress.value || completed.value.has(sectionId)) return
  progress.value = {
    ...progress.value,
    completed_section_ids: [...progress.value.completed_section_ids, sectionId],
    last_section_id: sectionId,
  }
  queueProgressSave()
}

function updateActiveSection() {
  const marker = window.innerHeight * 0.3
  let current = navItems.value[0]
  for (const item of navItems.value) {
    const element = document.getElementById(item.id)
    if (element && element.getBoundingClientRect().top <= marker) current = item
    else break
  }
  if (!current) return
  activeSection.value = current.id
  completeOnVisit(current.id)
}

function scrollTo(id: string) {
  document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  mobileNavOpen.value = false
}

async function toggleComplete(sectionId: string) {
  if (!progress.value || !lesson.value) return
  const ids = new Set(progress.value.completed_section_ids)
  if (ids.has(sectionId)) ids.delete(sectionId)
  else ids.add(sectionId)
  progress.value = await api.saveProgress(lesson.value.id, {
    completed_section_ids: [...ids],
    last_section_id: sectionId,
    activity_state: progress.value.activity_state,
  })
}

function saveActivityState(state: Record<string, unknown>) {
  if (!progress.value || !lesson.value) return
  progress.value.activity_state = state
  if (String(state.answer ?? '').trim()) markComplete('activity')
  queueProgressSave()
}

async function saveAnswer(questionId: string, answer: string) {
  if (!lesson.value) return
  await api.saveAnswer(lesson.value.id, questionId, answer)
  if (answer.trim()) markComplete('questions')
}

async function toggleFavorite() {
  if (!lesson.value) return
  lesson.value = await api.favorite(lesson.value.id, !lesson.value.is_favorite)
}

onMounted(load)
onBeforeUnmount(() => { window.removeEventListener('scroll', updateActiveSection); if (saveTimer) clearTimeout(saveTimer) })
</script>

<template>
  <div v-if="loading" class="lesson-loading shell"><span class="spinner"></span><p>Organizando conceitos, decisões e atividade…</p></div>
  <div v-else-if="error" class="empty-state page-section shell"><h1>Esta trilha saiu do caminho</h1><p role="alert">{{ error }}</p><RouterLink to="/biblioteca" class="primary-button primary-button--compact">Voltar à biblioteca</RouterLink></div>
  <div v-else-if="lesson && spec && progress" class="lesson-page">
    <button class="lesson-nav-toggle" type="button" @click="mobileNavOpen = !mobileNavOpen"><AppIcon name="menu" /> Conteúdo · {{ progress.percentage }}%</button>
    <aside class="lesson-sidebar" :class="{ open: mobileNavOpen }">
      <div class="lesson-sidebar__head"><RouterLink to="/biblioteca">← Biblioteca</RouterLink><button class="icon-button" type="button" :class="{ active: lesson.is_favorite }" :aria-label="lesson.is_favorite ? 'Remover dos favoritos' : 'Adicionar aos favoritos'" @click="toggleFavorite"><AppIcon name="heart" /></button></div>
      <p class="micro-label">Sua trilha</p><h2>{{ spec.topic }}</h2>
      <div class="sidebar-progress"><div><span>Progresso</span><strong>{{ progress.percentage }}%</strong></div><div class="progress-track"><span :style="{ width: `${progress.percentage}%` }"></span></div></div>
      <nav aria-label="Seções da aula"><button v-for="(item, index) in navItems" :key="item.id" type="button" :class="{ active: activeSection === item.id, done: completed.has(item.id) }" @click="scrollTo(item.id)"><span>{{ String(index + 1).padStart(2, '0') }}</span><i></i><strong>{{ item.label }}</strong><AppIcon v-if="completed.has(item.id)" name="check" /></button></nav>
    </aside>

    <article class="lesson-content">
      <header id="opening" class="lesson-hero observed-section">
        <div class="lesson-hero__meta"><span>{{ spec.lesson_plan.domain }}</span><span>{{ spec.target.role }}</span><span>{{ spec.target.level }}</span><span>{{ spec.target.depth }}</span></div>
        <h1>{{ spec.title }}</h1><p class="lesson-deck">{{ spec.description }}</p>
        <blockquote><span>“</span><p>{{ spec.introduction.opening_question }}</p></blockquote>
        <div class="scenario-card"><p class="micro-label">Nosso cenário</p><p>{{ spec.introduction.concrete_scenario }}</p></div>
        <div class="opening-grid"><div><h2>Por que cai em entrevistas?</h2><p>{{ spec.introduction.interview_context }}</p></div><div><h2>Ao final, você será capaz de</h2><ul><li v-for="objective in spec.introduction.learning_objectives" :key="objective"><AppIcon name="check" />{{ objective }}</li></ul></div></div>
      </header>

      <section id="mental-model" class="content-section observed-section">
        <p class="eyebrow"><span></span> Antes da definição</p><h2>Construa o modelo mental</h2><p class="section-lead">{{ spec.mental_model.problem }}</p>
        <div class="guarantee-grid"><article><p class="micro-label">O que busca garantir</p><ul><li v-for="item in spec.mental_model.guarantees" :key="item"><AppIcon name="check" />{{ item }}</li></ul></article><article class="not-guarantee"><p class="micro-label">O que não garante</p><ul><li v-for="item in spec.mental_model.non_guarantees" :key="item"><span>×</span>{{ item }}</li></ul></article></div>
        <div class="definition-card"><div><span>Analogia</span><p>{{ spec.mental_model.analogy }}</p></div><div><span>Definição técnica</span><p>{{ spec.mental_model.technical_definition }}</p></div></div>
        <VisualizationRenderer v-if="spec.mental_model.initial_diagram" :visual="spec.mental_model.initial_diagram" />
      </section>

      <section id="glossary" class="content-section observed-section"><p class="eyebrow"><span></span> Linguagem compartilhada</p><h2>Termos essenciais</h2><p class="section-lead">Abra um termo para ir da definição intuitiva à precisão técnica.</p><GlossaryGrid :terms="spec.glossary" /></section>

      <section v-for="section in spec.sections" :id="section.id" :key="section.id" class="content-section story-section observed-section">
        <div class="story-heading"><div><p class="eyebrow"><span></span>{{ section.eyebrow }}</p><h2>{{ section.title }}</h2></div><button class="complete-button" type="button" :class="{ done: completed.has(section.id) }" @click="toggleComplete(section.id)"><span><AppIcon name="check" /></span>{{ completed.has(section.id) ? 'Concluída' : 'Marcar concluída' }}</button></div>
        <p class="section-lead">{{ section.scenario }}</p>
        <div v-if="section.observed_problem || section.hypothesis" class="reasoning-pair"><div><span>Problema observado</span><p>{{ section.observed_problem }}</p></div><div><span>Hipótese</span><p>{{ section.hypothesis }}</p></div></div>
        <div class="mechanism"><span>Mecanismo</span><p>{{ section.mechanism }}</p></div>
        <template v-for="block in section.blocks" :key="block.id"><p v-if="block.kind === 'paragraph'" class="body-copy">{{ block.text }}</p><div v-else-if="block.kind === 'callout'" class="content-callout"><AppIcon name="spark"/><div><strong>{{ block.title }}</strong><p>{{ block.text }}</p></div></div><div v-else-if="block.kind === 'bullets'" class="bullet-panel"><h3>{{ block.title }}</h3><ul><li v-for="item in block.items" :key="item">{{ item }}</li></ul></div><pre v-else-if="block.kind === 'code'"><code>{{ block.text }}</code></pre></template>
        <VisualizationRenderer v-for="visual in section.visualizations" :key="visual.id" :visual="visual" />
        <div class="outcome-grid"><div><span>Resultado</span><p>{{ section.result }}</p></div><div><span>Benefício</span><p>{{ section.benefit }}</p></div><div><span>Limite</span><p>{{ section.limitation }}</p></div></div>
        <p v-if="section.transition" class="transition-copy">{{ section.transition }} <AppIcon name="arrow" /></p>
      </section>

      <section id="tradeoffs" class="content-section observed-section"><p class="eyebrow"><span></span> Decidir é escolher custos</p><h2>Trade-offs, não receitas</h2><p class="section-lead">Compare pelas restrições do cenário. Nenhuma coluna vence fora de contexto.</p><article v-for="tradeoff in spec.tradeoffs" :key="tradeoff.id" class="tradeoff-block"><h3>{{ tradeoff.decision }}</h3><div class="axis-list"><span v-for="axis in tradeoff.axes" :key="axis">{{ axis }}</span></div><div class="alternatives-grid"><div v-for="alternative in tradeoff.alternatives" :key="alternative.name" class="alternative-card"><h4>{{ alternative.name }}</h4><p>{{ alternative.how_it_works }}</p><dl><dt>Ganhos</dt><dd><ul><li v-for="item in alternative.benefits" :key="item">{{ item }}</li></ul></dd><dt>Custos</dt><dd><ul><li v-for="item in alternative.disadvantages" :key="item">{{ item }}</li></ul></dd><dt>Use quando</dt><dd>{{ alternative.use_when.join(' · ') }}</dd><dt>Evite quando</dt><dd>{{ alternative.avoid_when.join(' · ') }}</dd></dl></div></div><p class="recommendation"><strong>Leitura contextual:</strong> {{ tradeoff.contextual_recommendation }}</p></article><h3 class="subsection-title">Falhas que mudam a decisão</h3><div class="edge-grid"><article v-for="edge in spec.edge_cases" :key="edge.id"><span>Caso extremo</span><h4>{{ edge.scenario }}</h4><p>{{ edge.effect }}</p><dl><dt>Detecte</dt><dd>{{ edge.detection }}</dd><dt>Mitigue</dt><dd>{{ edge.mitigation }}</dd></dl></article></div></section>

      <section id="interview" class="content-section interview-section observed-section"><p class="eyebrow"><span></span> Perspectiva de entrevista</p><h2>Como discutir isso em uma entrevista</h2><div class="evaluates"><span>O entrevistador avalia</span><ul><li v-for="item in spec.interview_guide.evaluates" :key="item">{{ item }}</li></ul></div><div class="answer-timing"><article><span>30 segundos</span><p>{{ spec.interview_guide.answer_30_seconds }}</p></article><article><span>2 minutos</span><p>{{ spec.interview_guide.answer_2_minutes }}</p></article></div><div class="interview-grid"><div><h3>Perguntas antes de responder</h3><ul><li v-for="item in spec.interview_guide.clarifying_questions" :key="item">{{ item }}</li></ul></div><div><h3>Se pedirem aprofundamento</h3><ul><li v-for="item in spec.interview_guide.deep_dive_prompts" :key="item">{{ item }}</li></ul></div></div><div class="seniority-panel"><h3>A régua muda com a senioridade</h3><div><article v-for="(items, level) in spec.interview_guide.seniority_expectations" :key="level" :class="{ current: level === spec.target.level }"><span>{{ level }}<i v-if="level === spec.target.level">Seu nível</i></span><ul><li v-for="item in items" :key="item">{{ item }}</li></ul></article></div></div></section>

      <section id="questions" class="content-section observed-section"><p class="eyebrow"><span></span> Recupere, não reconheça</p><h2>Perguntas de prática</h2><p class="section-lead">Responda antes de revelar os pontos. A dificuldade está em organizar raciocínio, não em repetir o texto.</p><QuestionPractice :questions="spec.questions" @save="saveAnswer" /></section>

      <section id="activity" class="content-section activity-section observed-section"><ActivityRenderer :activity="spec.interactive_activity" :questions="spec.questions" :initial-state="progress.activity_state" @state-change="saveActivityState" @save-answer="saveAnswer" /></section>

      <section id="summary" class="content-section summary-section observed-section"><p class="eyebrow"><span></span> Feche o ciclo</p><h2>Leve estas ideias com você</h2><div class="summary-grid"><div><h3>Pontos-chave</h3><ol><li v-for="(item, index) in spec.summary.key_points" :key="item"><span>{{ index + 1 }}</span>{{ item }}</li></ol></div><div><h3>Checklist de entrevista</h3><ul><li v-for="item in spec.summary.interview_checklist" :key="item"><AppIcon name="check"/>{{ item }}</li></ul></div></div><div class="sources"><h3>Fontes e limites</h3><a v-for="source in spec.sources" :key="source.id" :href="source.url" target="_blank" rel="noreferrer"><span>{{ source.type }}</span><strong>{{ source.title }}</strong><small>{{ source.organization_or_authors }} ↗</small></a><details><summary>Limitações desta explicação</summary><ul><li v-for="item in spec.limitations" :key="item">{{ item }}</li></ul></details></div><ConceptMap :map="spec.summary.concept_map" /><div class="next-topics"><span>Continue a trilha</span><RouterLink v-for="topic in spec.summary.next_topics" :key="topic" :to="{ name: 'home', query: { topic } }">{{ topic }} <AppIcon name="arrow"/></RouterLink></div></section>
    </article>
  </div>
</template>

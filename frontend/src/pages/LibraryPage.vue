<script setup lang="ts">
import { onMounted, ref } from 'vue'
import AppIcon from '../components/AppIcon.vue'
import { api } from '../services/api'
import type { LessonListItem } from '../types/lesson'

const lessons = ref<LessonListItem[]>([])
const query = ref('')
const loading = ref(true)
const error = ref('')

async function load() {
  loading.value = true
  try {
    lessons.value = await api.listLessons(query.value)
  } catch (err) {
    error.value = err instanceof Error ? err.message : 'Não foi possível carregar sua biblioteca.'
  } finally {
    loading.value = false
  }
}

async function toggleFavorite(lesson: LessonListItem) {
  const updated = await api.favorite(lesson.id, !lesson.is_favorite)
  lesson.is_favorite = updated.is_favorite
}

async function remove(lesson: LessonListItem) {
  if (!window.confirm(`Excluir a aula “${lesson.topic}”?`)) return
  await api.deleteLesson(lesson.id)
  lessons.value = lessons.value.filter((item) => item.id !== lesson.id)
}

onMounted(load)
</script>

<template>
  <section class="library-page shell page-section">
    <div class="page-heading">
      <div><p class="eyebrow"><span></span> Continue de onde parou</p><h1>Minha biblioteca</h1><p>Aulas salvas, progresso e decisões que merecem uma segunda passada.</p></div>
      <RouterLink to="/" class="primary-button primary-button--compact">Criar nova aula <AppIcon name="arrow" /></RouterLink>
    </div>
    <form class="search-box" role="search" @submit.prevent="load"><AppIcon name="search" /><label class="sr-only" for="library-search">Pesquisar aulas</label><input id="library-search" v-model="query" placeholder="Pesquisar por assunto…" /><button type="submit">Buscar</button></form>
    <p v-if="error" class="form-error" role="alert">{{ error }}</p>
    <div v-if="loading" class="lesson-grid" aria-label="Carregando aulas"><div v-for="n in 3" :key="n" class="lesson-card skeleton"></div></div>
    <div v-else-if="lessons.length" class="lesson-grid">
      <article v-for="lesson in lessons" :key="lesson.id" class="lesson-card">
        <div class="lesson-card__top"><span class="domain-pill">{{ lesson.domain }}</span><button class="icon-button" type="button" :class="{ active: lesson.is_favorite }" :aria-label="lesson.is_favorite ? 'Remover dos favoritos' : 'Adicionar aos favoritos'" @click="toggleFavorite(lesson)"><AppIcon name="heart" /></button></div>
        <h2><RouterLink :to="`/aulas/${lesson.id}`">{{ lesson.topic }}</RouterLink></h2>
        <p>{{ lesson.description }}</p>
        <div class="lesson-meta"><span>{{ lesson.target_role }}</span><span>{{ lesson.target_level }}</span><span>{{ lesson.depth }}</span></div>
        <div class="progress-label"><span>Progresso</span><strong>{{ lesson.progress_percentage }}%</strong></div>
        <div class="progress-track"><span :style="{ width: `${lesson.progress_percentage}%` }"></span></div>
        <div class="lesson-card__actions"><RouterLink :to="`/aulas/${lesson.id}`">{{ lesson.progress_percentage ? 'Continuar aula' : 'Começar aula' }} <AppIcon name="arrow" /></RouterLink><button type="button" @click="remove(lesson)">Excluir</button></div>
      </article>
    </div>
    <div v-else class="empty-state"><span class="empty-icon"><AppIcon name="book" /></span><h2>Sua próxima descoberta começa aqui</h2><p>Nenhuma aula encontrada. Escolha um assunto técnico e construa a primeira trilha.</p><RouterLink to="/" class="primary-button primary-button--compact">Criar primeira aula <AppIcon name="arrow" /></RouterLink></div>
  </section>
</template>


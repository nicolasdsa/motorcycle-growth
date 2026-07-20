<script setup lang="ts">
import type { LessonSpecification } from '../types/lesson'

defineProps<{ terms: LessonSpecification['glossary'] }>()
</script>

<template>
  <div class="glossary-grid">
    <details v-for="term in terms" :key="term.id" class="term-card">
      <summary><span>{{ term.term.slice(0, 2).toUpperCase() }}</span><strong>{{ term.term }}</strong><i>+</i></summary>
      <div><p>{{ term.simple_definition }}</p><dl><dt>Definição técnica</dt><dd>{{ term.technical_definition }}</dd><dt>Exemplo</dt><dd>{{ term.example }}</dd><dt>Em entrevistas</dt><dd>{{ term.interview_relevance }}</dd><dt>Equívoco comum</dt><dd>{{ term.common_misconception }}</dd></dl></div>
    </details>
  </div>
</template>


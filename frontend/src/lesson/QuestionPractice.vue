<script setup lang="ts">
import { reactive } from 'vue'
import type { InterviewQuestion } from '../types/lesson'

defineProps<{ questions: InterviewQuestion[] }>()
const emit = defineEmits<{ save: [questionId: string, answer: string] }>()
const answers = reactive<Record<string, string>>({})
const revealed = reactive<Record<string, boolean>>({})

const categoryLabels: Record<string, string> = {
  fundamentals: 'Fundamentos', application: 'Aplicação', comparison: 'Comparação',
  diagnosis: 'Diagnóstico', 'edge-cases': 'Casos extremos', implementation: 'Implementação', 'deep-dive': 'Aprofundamento',
}
</script>

<template>
  <div class="question-list">
    <article v-for="(question, index) in questions" :key="question.id" class="question-card">
      <header><span>{{ String(index + 1).padStart(2, '0') }}</span><div><p>{{ categoryLabels[question.category] ?? question.category }} · {{ question.difficulty }}</p><h3>{{ question.prompt }}</h3></div></header>
      <label class="field"><span>Sua resposta</span><textarea v-model="answers[question.id]" rows="4" placeholder="Estruture seu raciocínio antes de revelar a resposta…"></textarea></label>
      <div class="question-actions"><button type="button" class="secondary-button" @click="revealed[question.id] = !revealed[question.id]; emit('save', question.id, answers[question.id] ?? '')">{{ revealed[question.id] ? 'Ocultar pontos' : 'Revelar pontos esperados' }}</button></div>
      <div v-if="revealed[question.id]" class="answer-panel"><p>{{ question.expected_answer }}</p><h4>Essencial</h4><ul><li v-for="point in question.essential_points" :key="point">{{ point }}</li></ul><template v-if="question.follow_ups.length"><h4>Continuação</h4><ul><li v-for="item in question.follow_ups" :key="item">{{ item }}</li></ul></template></div>
    </article>
  </div>
</template>


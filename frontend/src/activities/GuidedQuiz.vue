<script setup lang="ts">
import { ref, watch } from 'vue'
import type { InteractiveActivity, InterviewQuestion } from '../types/lesson'

const props = defineProps<{ activity: InteractiveActivity; question: InterviewQuestion; initialState?: Record<string, unknown> }>()
const emit = defineEmits<{ stateChange: [value: Record<string, unknown>]; saveAnswer: [answer: string] }>()
const answer = ref(String(props.initialState?.answer ?? ''))
const revealed = ref(Boolean(props.initialState?.revealed ?? false))
watch([answer, revealed], () => emit('stateChange', { answer: answer.value, revealed: revealed.value }))
</script>

<template>
  <div class="guided-quiz">
    <p class="micro-label">Atividade principal</p><h2>{{ activity.title }}</h2><p>{{ activity.teaching_goal }}</p>
    <div class="quiz-prompt"><span>Pergunta</span><strong>{{ question.prompt }}</strong></div>
    <label class="field"><span>Sua resposta</span><textarea v-model="answer" rows="6" placeholder="Pense em problema, mecanismo, premissas e limites…"></textarea></label>
    <div class="quiz-actions"><button class="primary-button primary-button--compact" type="button" @click="revealed = true; emit('saveAnswer', answer)">Comparar resposta</button></div>
    <div v-if="revealed" class="answer-panel" aria-live="polite"><p class="micro-label">Pontos esperados</p><p>{{ question.expected_answer }}</p><ul><li v-for="point in question.essential_points" :key="point">{{ point }}</li></ul></div>
  </div>
</template>


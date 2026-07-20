<script setup lang="ts">
import type { InteractiveActivity, InterviewQuestion } from '../types/lesson'
import GuidedQuiz from './GuidedQuiz.vue'
import LoadBalancerLab from './LoadBalancerLab.vue'

defineProps<{ activity: InteractiveActivity; questions: InterviewQuestion[]; initialState?: Record<string, unknown> }>()
defineEmits<{ stateChange: [value: Record<string, unknown>]; saveAnswer: [questionId: string, answer: string] }>()
</script>

<template>
  <LoadBalancerLab v-if="activity.type === 'simulation-playground'" :activity="activity" :initial-state="initialState" @state-change="$emit('stateChange', $event)" />
  <GuidedQuiz v-else-if="questions.length" :activity="activity" :question="questions[0]" :initial-state="initialState" @state-change="$emit('stateChange', $event)" @save-answer="$emit('saveAnswer', questions[0].id, $event)" />
  <div v-else class="visual-fallback"><h2>{{ activity.title }}</h2><p>{{ activity.accessible_description }}</p></div>
</template>


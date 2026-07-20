<script setup lang="ts">
import { computed } from 'vue'
import type { Visualization } from '../types/lesson'

type FlowStep = { label: string; description: string; lane?: string }

const props = defineProps<{ visual: Visualization }>()
const steps = computed<FlowStep[]>(() => props.visual.steps.map((step, index) => ({
  label: String(step.label ?? step.title ?? `Passo ${index + 1}`),
  description: String(step.description ?? ''),
  lane: typeof step.lane === 'string' ? step.lane : undefined,
})))
const lanes = computed(() => {
  const configured = props.visual.data.lanes
  if (Array.isArray(configured) && configured.every((lane) => typeof lane === 'string')) return configured as string[]
  return [...new Set(steps.value.map((step) => step.lane).filter((lane): lane is string => Boolean(lane)))]
})
const sharedSteps = computed(() => steps.value.filter((step) => !step.lane))
function laneSteps(lane: string) { return steps.value.filter((step) => step.lane === lane) }
</script>

<template>
  <div class="concurrent-flow" :class="{ 'concurrent-flow--linear': lanes.length === 1 }" role="img" :aria-label="visual.accessible_description">
    <div class="concurrent-flow__lanes">
      <section v-for="lane in lanes" :key="lane" class="concurrent-flow__lane">
        <header>{{ lane }}</header>
        <ol><li v-for="(step, index) in laneSteps(lane)" :key="`${lane}-${step.label}`"><span>{{ index + 1 }}</span><div><strong>{{ step.label }}</strong><p>{{ step.description }}</p></div></li></ol>
      </section>
    </div>
    <ol v-if="sharedSteps.length" class="concurrent-flow__shared"><li v-for="(step, index) in sharedSteps" :key="step.label"><span>{{ index + 1 }}</span><div><strong>{{ step.label }}</strong><p>{{ step.description }}</p></div></li></ol>
  </div>
</template>

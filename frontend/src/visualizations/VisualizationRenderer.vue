<script setup lang="ts">
import { computed } from 'vue'
import type { Visualization } from '../types/lesson'
import ConcurrentDecisionFlow from './ConcurrentDecisionFlow.vue'

const props = defineProps<{ visual: Visualization }>()

const supported = new Set([
  'request-flow', 'server-cluster', 'load-distribution', 'annotated-diagram',
  'step-by-step', 'comparison-table', 'decision-matrix', 'timeline',
  'code-walkthrough', 'callout-example',
])
const isSupported = computed(() => supported.has(props.visual.type))
const chartValues = computed(() => {
  const values = props.visual.data.values
  return Array.isArray(values) ? values as Array<{ label: string; value: number }> : []
})
const maxValue = computed(() => Math.max(1, ...chartValues.value.map((item) => item.value)))
</script>

<template>
  <figure class="visual-card" :aria-labelledby="`${visual.id}-title`">
    <div class="visual-card__heading">
      <div><p class="micro-label">Visual · {{ visual.type }}</p><h3 :id="`${visual.id}-title`">{{ visual.title }}</h3></div>
      <span class="visual-status">Estado inicial</span>
    </div>
    <p class="visual-goal">{{ visual.teaching_goal }}</p>

    <div v-if="isSupported && (visual.type === 'request-flow' || visual.type === 'annotated-diagram')" class="flow-visual" aria-hidden="true">
      <template v-for="(element, index) in visual.elements" :key="element.id">
        <div class="flow-node" :class="`flow-node--${element.kind}`"><span class="node-dot"></span><strong>{{ element.label }}</strong><small>{{ element.description }}</small></div>
        <div v-if="index < visual.elements.length - 1" class="flow-arrow"><span></span><i>›</i></div>
      </template>
    </div>

    <div v-else-if="isSupported && visual.type === 'server-cluster'" class="cluster-visual" aria-hidden="true">
      <div v-for="element in visual.elements" :key="element.id" class="server-unit" :class="{ 'server-unit--failed': element.kind === 'failed' }">
        <div class="server-lights"><span></span><span></span><span></span></div><strong>{{ element.label }}</strong><small>{{ element.description }}</small>
      </div>
      <div v-if="visual.captions.length" class="metric-strip"><span v-for="caption in visual.captions" :key="caption">{{ caption }}</span></div>
    </div>

    <div v-else-if="isSupported && visual.type === 'load-distribution'" class="bar-visual" aria-hidden="true">
      <div v-for="item in chartValues" :key="item.label" class="bar-row"><span>{{ item.label }}</span><div><i :style="{ width: `${item.value / maxValue * 100}%` }"></i></div><strong>{{ item.value }} req/s</strong></div>
    </div>

    <ConcurrentDecisionFlow v-else-if="isSupported && (visual.type === 'step-by-step' || visual.type === 'timeline') && visual.data.layout === 'concurrency-flow'" :visual="visual" />

    <ol v-else-if="isSupported && (visual.type === 'step-by-step' || visual.type === 'timeline')" class="visual-steps">
      <li v-for="(step, index) in visual.steps" :key="index"><span>{{ index + 1 }}</span><p>{{ Object.values(step).join(' · ') }}</p></li>
    </ol>

    <div v-else class="visual-fallback">
      <p>Esta visualização ainda não tem um renderizador especializado. O conteúdo textual permanece disponível abaixo.</p>
    </div>

    <img
      v-if="visual.asset_id"
      class="visual-asset"
      :src="`/api/assets/${visual.asset_id}`"
      :alt="visual.accessible_description"
    />

    <details class="accessible-description">
      <summary>Ouvir / ler descrição do diagrama</summary>
      <p>{{ visual.accessible_description }}</p>
      <ul v-if="visual.captions.length"><li v-for="caption in visual.captions" :key="caption">{{ caption }}</li></ul>
    </details>
  </figure>
</template>

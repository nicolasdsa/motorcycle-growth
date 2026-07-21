<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { forceCenter, forceCollide, forceLink, forceManyBody, forceSimulation, type Simulation } from 'd3-force'
import type { ConceptMap, ConceptMapEdge, ConceptMapNode } from '../types/lesson'

type PositionedNode = ConceptMapNode & { x: number; y: number; vx?: number; vy?: number; fx?: number | null; fy?: number | null }
type PositionedEdge = Omit<ConceptMapEdge, 'source' | 'target'> & { source: PositionedNode; target: PositionedNode }

const props = defineProps<{ map: ConceptMap }>()
const container = ref<HTMLElement | null>(null)
const width = ref(760)
const height = ref(440)
const nodes = ref<PositionedNode[]>([])
const edges = ref<PositionedEdge[]>([])
const activeId = ref<string | null>(null)
const layoutVersion = ref(0)
const reducedMotion = ref(false)
let simulation: Simulation<PositionedNode, undefined> | null = null
let resizeObserver: ResizeObserver | null = null
let dragId: string | null = null

const radius = 28
const padding = 38
const nodeById = computed(() => new Map(nodes.value.map((node) => [node.id, node])))
const activeNode = computed(() => activeId.value ? nodeById.value.get(activeId.value) ?? null : null)
const neighborIds = computed(() => {
  if (!activeId.value) return new Set<string>()
  const ids = new Set([activeId.value])
  for (const edge of edges.value) {
    if (edge.source.id === activeId.value) ids.add(edge.target.id)
    if (edge.target.id === activeId.value) ids.add(edge.source.id)
  }
  return ids
})

function clamp(node: PositionedNode) {
  node.x = Math.max(padding, Math.min(width.value - padding, node.x))
  node.y = Math.max(padding, Math.min(height.value - padding, node.y))
}

function initialise() {
  simulation?.stop()
  const centerX = width.value / 2
  const centerY = height.value / 2
  const orbit = Math.min(width.value, height.value) * 0.31
  nodes.value = props.map.nodes.map((node, index) => {
    const angle = (Math.PI * 2 * index) / props.map.nodes.length - Math.PI / 2
    return { ...node, x: centerX + Math.cos(angle) * orbit, y: centerY + Math.sin(angle) * orbit }
  })
  const lookup = new Map(nodes.value.map((node) => [node.id, node]))
  edges.value = props.map.edges.flatMap((edge) => {
    const source = lookup.get(edge.source)
    const target = lookup.get(edge.target)
    return source && target ? [{ ...edge, source, target }] : []
  })
  startSimulation()
}

function startSimulation() {
  simulation?.stop()
  if (reducedMotion.value) {
    layoutVersion.value++
    return
  }
  simulation = forceSimulation(nodes.value)
    .force('link', forceLink<PositionedNode, PositionedEdge>(edges.value).id((node) => node.id).distance(118).strength(0.65))
    .force('charge', forceManyBody().strength(-280))
    .force('center', forceCenter(width.value / 2, height.value / 2))
    .force('collide', forceCollide<PositionedNode>(radius + 11))
    .alphaDecay(0.06)
    .on('tick', () => {
      nodes.value.forEach(clamp)
      layoutVersion.value++
      if (simulation && simulation.alpha() < 0.025) simulation.stop()
    })
}

function restartSimulation() {
  if (reducedMotion.value) {
    layoutVersion.value++
    return
  }
  if (!simulation) startSimulation()
  else simulation.alpha(0.55).restart()
}

function updateDimensions() {
  const rect = container.value?.getBoundingClientRect()
  if (!rect?.width) return
  width.value = Math.max(280, Math.round(rect.width))
  height.value = width.value < 460 ? 360 : 440
  nodes.value.forEach(clamp)
  restartSimulation()
}

function pointFromEvent(event: PointerEvent) {
  const rect = container.value?.getBoundingClientRect()
  if (!rect || !rect.width || !rect.height) return { x: event.clientX, y: event.clientY }
  return {
    x: (event.clientX - rect.left) * (width.value / rect.width),
    y: (event.clientY - rect.top) * (height.value / rect.height),
  }
}

function selectNode(id: string) {
  activeId.value = id
}

function clearSelection() {
  activeId.value = null
}

function onNodePointerDown(event: PointerEvent, node: PositionedNode) {
  event.stopPropagation()
  selectNode(node.id)
  dragId = node.id
  const point = pointFromEvent(event)
  node.fx = point.x
  node.fy = point.y
  node.x = point.x
  node.y = point.y
  clamp(node)
  ;(event.currentTarget as SVGGElement).setPointerCapture?.(event.pointerId)
  restartSimulation()
}

function onNodePointerMove(event: PointerEvent) {
  if (!dragId) return
  const node = nodeById.value.get(dragId)
  if (!node) return
  const point = pointFromEvent(event)
  node.fx = point.x
  node.fy = point.y
  clamp(node)
  layoutVersion.value++
}

function onNodePointerUp() {
  if (!dragId) return
  const node = nodeById.value.get(dragId)
  if (node) {
    node.fx = null
    node.fy = null
  }
  dragId = null
  restartSimulation()
}

function onKeydown(event: KeyboardEvent, node: PositionedNode) {
  if (event.key === 'Enter' || event.key === ' ') {
    event.preventDefault()
    activeId.value = activeId.value === node.id ? null : node.id
  }
  if (event.key === 'Escape') clearSelection()
}

function isDimmed(node: PositionedNode) {
  return !!activeId.value && !neighborIds.value.has(node.id)
}

function isActiveEdge(edge: PositionedEdge) {
  return !!activeId.value && (edge.source.id === activeId.value || edge.target.id === activeId.value)
}

watch(() => props.map, () => nextTick(initialise), { deep: true })

onMounted(() => {
  reducedMotion.value = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches ?? false
  updateDimensions()
  initialise()
  if (typeof ResizeObserver !== 'undefined') {
    resizeObserver = new ResizeObserver(updateDimensions)
    if (container.value) resizeObserver.observe(container.value)
  }
})

onBeforeUnmount(() => {
  simulation?.stop()
  resizeObserver?.disconnect()
})
</script>

<template>
  <section ref="container" class="concept-map" :aria-labelledby="`${map.id}-title`">
    <div class="concept-map__heading">
      <div><p class="micro-label">Mapa de conexões</p><h3 :id="`${map.id}-title`">{{ map.title }}</h3></div>
      <p>{{ map.teaching_goal }}</p>
    </div>
    <p :id="`${map.id}-description`" class="sr-only">{{ map.accessible_description }} Use Tab para percorrer os conceitos. Ao focalizar um conceito, seus vizinhos diretos são destacados.</p>
    <svg
      class="concept-map__graph"
      :viewBox="`0 0 ${width} ${height}`"
      role="group"
      :aria-labelledby="`${map.id}-title ${map.id}-description`"
      @pointerdown.self="clearSelection"
    >
      <rect width="100%" height="100%" class="concept-map__backdrop" @pointerdown="clearSelection" />
      <g :key="layoutVersion" class="concept-map__edges" aria-hidden="true">
        <line
          v-for="(edge, index) in edges"
          :key="`${edge.source.id}-${edge.target.id}-${index}`"
          :x1="edge.source.x" :y1="edge.source.y" :x2="edge.target.x" :y2="edge.target.y"
          :class="{ active: isActiveEdge(edge), dim: activeId && !isActiveEdge(edge) }"
        />
      </g>
      <g
        v-for="node in nodes"
        :key="node.id"
        class="concept-map__node"
        :class="[`concept-map__node--${node.kind}`, { active: activeId === node.id, connected: activeId && neighborIds.has(node.id), dim: isDimmed(node) }]"
        :transform="`translate(${node.x}, ${node.y})`"
        role="button"
        tabindex="0"
        :aria-label="`${node.label}. ${node.summary}. Macete: ${node.mnemonic}`"
        :aria-pressed="activeId === node.id"
        @pointerenter="selectNode(node.id)"
        @pointerleave="!dragId && clearSelection()"
        @focus="selectNode(node.id)"
        @blur="clearSelection"
        @keydown="onKeydown($event, node)"
        @pointerdown="onNodePointerDown($event, node)"
        @pointermove="onNodePointerMove"
        @pointerup="onNodePointerUp"
        @pointercancel="onNodePointerUp"
      >
        <circle :r="radius" />
        <text text-anchor="middle" dominant-baseline="middle">{{ node.label }}</text>
      </g>
    </svg>
    <aside v-if="activeNode" class="concept-map__tooltip" role="status">
      <span>{{ activeNode.kind }}</span><strong>{{ activeNode.label }}</strong><p>{{ activeNode.summary }}</p><small><b>Macete:</b> {{ activeNode.mnemonic }}</small>
    </aside>
    <p class="concept-map__hint">Arraste os nós para explorar relações. Toque ou use Tab em um conceito para destacá-lo; toque fora para limpar.</p>
    <details class="accessible-description"><summary>Descrição textual do mapa</summary><p>{{ map.accessible_description }}</p><ul><li v-for="node in map.nodes" :key="node.id"><strong>{{ node.label }}:</strong> {{ node.summary }} Macete: {{ node.mnemonic }}</li></ul></details>
  </section>
</template>

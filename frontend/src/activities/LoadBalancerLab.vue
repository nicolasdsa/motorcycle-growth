<script setup lang="ts">
import { computed, reactive, watch } from 'vue'
import AppIcon from '../components/AppIcon.vue'
import type { InteractiveActivity } from '../types/lesson'

type ServerConfig = { id: string; name: string; capacity: number }
type LabState = { traffic: number; algorithm: string; capacities: Record<string, number>; failed: Record<string, boolean> }

const props = defineProps<{ activity: InteractiveActivity; initialState?: Record<string, unknown> }>()
const emit = defineEmits<{ stateChange: [value: Record<string, unknown>] }>()
const configServers = (props.activity.config.servers as ServerConfig[]) ?? []
const restored = (props.initialState ?? {}) as Partial<LabState>
const state = reactive<LabState>({
  traffic: Number(restored.traffic ?? props.activity.config.traffic ?? 900),
  algorithm: String(restored.algorithm ?? props.activity.config.algorithm ?? 'round-robin'),
  capacities: restored.capacities ?? Object.fromEntries(configServers.map((server) => [server.id, server.capacity])),
  failed: restored.failed ?? Object.fromEntries(configServers.map((server) => [server.id, false])),
})

const maxTraffic = Number(props.activity.config.maxTraffic ?? 2400)
const healthy = computed(() => configServers.filter((server) => !state.failed[server.id]))
const allocation = computed(() => {
  const result = Object.fromEntries(configServers.map((server) => [server.id, 0]))
  if (!healthy.value.length) return result
  if (state.algorithm === 'round-robin') {
    const share = state.traffic / healthy.value.length
    healthy.value.forEach((server) => { result[server.id] = share })
  } else {
    // Equal-cost requests: selecting the lowest active/capacity score approximates weighted least-connections.
    const totalCapacity = healthy.value.reduce((sum, server) => sum + state.capacities[server.id], 0)
    healthy.value.forEach((server) => {
      result[server.id] = totalCapacity ? state.traffic * state.capacities[server.id] / totalCapacity : 0
    })
  }
  return result
})
const totalCapacity = computed(() => healthy.value.reduce((sum, server) => sum + state.capacities[server.id], 0))
const queued = computed(() => Math.max(0, state.traffic - totalCapacity.value))
const utilization = computed(() => totalCapacity.value ? state.traffic / totalCapacity.value : Infinity)
const latency = computed(() => {
  if (!Number.isFinite(utilization.value)) return 'indisponível'
  if (utilization.value <= 0.65) return '~45 ms'
  if (utilization.value <= 0.85) return '~90 ms'
  if (utilization.value <= 1) return '~220 ms'
  return '> 1 s'
})
const statusText = computed(() => {
  if (!healthy.value.length) return 'Sem destinos saudáveis: nenhuma requisição pode ser atendida.'
  if (queued.value > 0) return `Sobrecarga: ${Math.round(queued.value)} req/s excedem a capacidade saudável.`
  return `Há ${Math.round(totalCapacity.value - state.traffic)} req/s de margem estimada.`
})

function reset() {
  state.traffic = Number(props.activity.config.traffic ?? 900)
  state.algorithm = 'round-robin'
  state.capacities = Object.fromEntries(configServers.map((server) => [server.id, server.capacity]))
  state.failed = Object.fromEntries(configServers.map((server) => [server.id, false]))
}

watch(state, () => emit('stateChange', JSON.parse(JSON.stringify(state))), { deep: true })
</script>

<template>
  <div class="lab" aria-labelledby="lab-title">
    <div class="lab-header"><div><p class="micro-label">Atividade principal</p><h2 id="lab-title">{{ activity.title }}</h2><p>{{ activity.teaching_goal }}</p></div><button type="button" class="secondary-button" @click="reset">Reiniciar</button></div>
    <div class="lab-body">
      <aside class="lab-controls" aria-label="Controles do laboratório">
        <label class="range-field"><span><strong>Tráfego</strong><output>{{ state.traffic }} req/s</output></span><input v-model.number="state.traffic" type="range" min="0" :max="maxTraffic" step="50" /></label>
        <fieldset><legend>Algoritmo</legend><label class="radio-card"><input v-model="state.algorithm" type="radio" value="round-robin"/><span><strong>Round robin</strong><small>Divide a quantidade igualmente.</small></span></label><label class="radio-card"><input v-model="state.algorithm" type="radio" value="least-connections"/><span><strong>Least connections ponderado</strong><small>Usa capacidade como aproximação para trabalhos de custo igual.</small></span></label></fieldset>
        <fieldset><legend>Capacidade e saúde</legend><div v-for="server in configServers" :key="server.id" class="server-control"><div><span class="health-dot" :class="{ off: state.failed[server.id] }"></span><strong>{{ server.name }}</strong><button type="button" :aria-pressed="state.failed[server.id]" @click="state.failed[server.id] = !state.failed[server.id]">{{ state.failed[server.id] ? 'Recuperar' : 'Provocar falha' }}</button></div><label><span>Capacidade</span><input v-model.number="state.capacities[server.id]" type="number" min="100" max="1000" step="50" :disabled="state.failed[server.id]"/><span>req/s</span></label></div></fieldset>
      </aside>
      <div class="lab-stage">
        <div class="lab-metrics"><div><span>Capacidade saudável</span><strong>{{ totalCapacity }}<small> req/s</small></strong></div><div><span>Fila estimada</span><strong :class="{ danger: queued > 0 }">{{ Math.round(queued) }}<small> req/s</small></strong></div><div><span>Latência relativa</span><strong>{{ latency }}</strong></div></div>
        <div class="traffic-scene" aria-hidden="true"><div class="client-cloud"><span v-for="n in 6" :key="n"></span><small>requisições</small></div><div class="moving-line"><i></i><i></i><i></i></div><div class="lb-node"><AppIcon name="spark"/><strong>LB</strong><small>{{ state.algorithm === 'round-robin' ? 'cíclico' : 'menor ocupação' }}</small></div><div class="fan-lines"><i></i><i></i><i></i></div><div class="lab-servers"><div v-for="server in configServers" :key="server.id" class="lab-server" :class="{ failed: state.failed[server.id], overloaded: allocation[server.id] > state.capacities[server.id] }"><div class="server-lights"><span></span><span></span><span></span></div><strong>{{ server.name }}</strong><span>{{ Math.round(allocation[server.id]) }} / {{ state.capacities[server.id] }}</span><div><i :style="{ width: `${Math.min(100, allocation[server.id] / state.capacities[server.id] * 100)}%` }"></i></div><small>{{ state.failed[server.id] ? 'fora da rotação' : allocation[server.id] > state.capacities[server.id] ? 'saturado' : 'saudável' }}</small></div></div></div>
        <p class="lab-result" role="status"><span :class="{ warning: queued > 0 || !healthy.length }"></span>{{ statusText }}</p>
        <details class="accessible-description"><summary>Como este modelo calcula o resultado?</summary><p>Round robin divide requisições igualmente entre servidores saudáveis. A opção ponderada divide conforme a capacidade configurada, aproximando least connections quando requisições têm custo igual. Fila é demanda menos capacidade saudável. A latência é apenas uma faixa didática baseada em utilização — não é benchmark.</p></details>
      </div>
    </div>
  </div>
</template>

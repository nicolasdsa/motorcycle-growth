<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import AppIcon from '../components/AppIcon.vue'
import { api } from '../services/api'
import type { LessonCreate } from '../types/lesson'

const router = useRouter()
const form = reactive<LessonCreate>({
  topic: 'Load balancing',
  target_role: 'Backend',
  target_level: 'Pleno',
  depth: 'aula normal',
  focus: 'system design',
  language: 'pt-BR',
})
const loading = ref(false)
const error = ref('')

const topicHint = computed(() => {
  if (form.topic.trim().length < 2) return 'Informe ao menos 2 caracteres.'
  if (form.topic.includes('?') || /^(como|por que|quando)/i.test(form.topic)) return 'Parece específico: a trilha vai direto ao mecanismo e aos casos extremos.'
  if (/algoritmos|solid|bancos de dados/i.test(form.topic)) return 'Parece amplo: a trilha vai organizar subdivisões e comparações.'
  return 'Parece focado: bom tamanho para uma aula progressiva.'
})

async function submit() {
  if (form.topic.trim().length < 2) return
  loading.value = true
  error.value = ''
  try {
    const lesson = await api.createLesson({ ...form, topic: form.topic.trim() })
    await router.push({ name: 'lesson', params: { id: lesson.id } })
  } catch (err) {
    error.value = err instanceof Error ? err.message : 'Não foi possível criar a aula.'
  } finally {
    loading.value = false
  }
}

const suggestions = ['Load balancing', 'ACID', 'Quick Sort', 'SOLID', 'OAuth 2.0']
</script>

<template>
  <div class="home-page">
    <section class="hero shell">
      <div class="hero-copy">
        <p class="eyebrow"><span></span> Preparação técnica, com intuição</p>
        <h1>Não decore respostas.<br><em>Entenda por dentro.</em></h1>
        <p class="hero-lead">Transforme qualquer assunto técnico em uma aula visual e progressiva — do primeiro modelo mental aos trade-offs que fazem diferença na entrevista.</p>
        <div class="topic-chips" aria-label="Sugestões de assunto">
          <button v-for="item in suggestions" :key="item" type="button" @click="form.topic = item">{{ item }}</button>
        </div>
        <div class="how-it-works" aria-label="Como funciona">
          <div><span>01</span><p><strong>Escolha o contexto</strong>Adapte à vaga e senioridade.</p></div>
          <div><span>02</span><p><strong>Construa a intuição</strong>Evolua por problemas reais.</p></div>
          <div><span>03</span><p><strong>Pratique decisões</strong>Teste falhas e explique escolhas.</p></div>
        </div>
      </div>

      <form class="create-card" aria-labelledby="create-title" @submit.prevent="submit">
        <div class="create-card__head">
          <span class="create-icon"><AppIcon name="spark" /></span>
          <div><p class="micro-label">Nova trilha</p><h2 id="create-title">O que você quer dominar?</h2></div>
        </div>
        <label class="field field--topic">
          <span>Assunto técnico</span>
          <input v-model="form.topic" name="topic" required minlength="2" maxlength="200" autocomplete="off" placeholder="Ex.: ACID, Quick Sort, OAuth 2.0…" />
          <small>{{ topicHint }}</small>
        </label>
        <div class="field-grid">
          <label class="field"><span>Cargo desejado</span><select v-model="form.target_role"><option v-for="role in ['Backend','Frontend','Full Stack','DevOps','SRE','Mobile','Data','Machine Learning','Geral']" :key="role">{{ role }}</option></select></label>
          <label class="field"><span>Senioridade</span><select v-model="form.target_level"><option>Júnior</option><option>Pleno</option><option>Sênior</option></select></label>
          <label class="field"><span>Profundidade</span><select v-model="form.depth"><option>resumo</option><option>aula normal</option><option>aprofundada</option></select></label>
          <label class="field"><span>Idioma</span><select v-model="form.language"><option value="pt-BR">Português (BR)</option><option value="en">English</option></select></label>
        </div>
        <label class="field"><span>Foco opcional</span><select v-model="form.focus"><option :value="null">Sem foco específico</option><option v-for="focus in ['entrevista conceitual','entrevista prática','algoritmos','system design','banco de dados','arquitetura','infraestrutura','fundamentos','diagnóstico','implementação']" :key="focus">{{ focus }}</option></select></label>
        <p v-if="error" class="form-error" role="alert">{{ error }}</p>
        <button class="primary-button" type="submit" :disabled="loading || form.topic.trim().length < 2">
          <span>{{ loading ? 'Construindo sua trilha…' : 'Construir minha trilha' }}</span>
          <AppIcon v-if="!loading" name="arrow" />
          <span v-else class="spinner" aria-hidden="true"></span>
        </button>
        <p class="form-note">A especificação é validada antes de chegar à sua tela.</p>
      </form>
    </section>

    <section class="value-band">
      <div class="shell value-grid">
        <article><span class="value-number">01</span><h2>Um problema concreto</h2><p>Cada aula começa com uma situação que você poderia encontrar no trabalho — ou na entrevista.</p></article>
        <article><span class="value-number">02</span><h2>Uma história que evolui</h2><p>A solução inicial encontra limites. Novos requisitos aparecem. Você acompanha o porquê de cada mudança.</p></article>
        <article><span class="value-number">03</span><h2>Uma decisão que é sua</h2><p>No laboratório final, altere premissas, provoque falhas e defenda a escolha com evidência.</p></article>
      </div>
    </section>
  </div>
</template>


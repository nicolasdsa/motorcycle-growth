<script setup lang="ts">
import AppIcon from './components/AppIcon.vue'
import { useTheme } from './composables/useTheme'

const { theme, toggleTheme } = useTheme()
</script>

<template>
  <header class="app-header">
    <RouterLink to="/" class="brand" aria-label="Trilha, página inicial">
      <span class="brand-mark"><AppIcon name="spark" /></span>
      <span>trilha<span class="brand-dot">.</span></span>
    </RouterLink>
    <nav class="main-nav" aria-label="Navegação principal">
      <RouterLink to="/">Criar aula</RouterLink>
      <RouterLink to="/biblioteca">Minha biblioteca</RouterLink>
    </nav>
    <button class="icon-button" type="button" :aria-label="theme === 'light' ? 'Ativar tema escuro' : 'Ativar tema claro'" @click="toggleTheme">
      <AppIcon :name="theme === 'light' ? 'moon' : 'sun'" />
    </button>
  </header>
  <main id="main-content">
    <RouterView />
  </main>
  <footer class="app-footer">
    <div class="brand brand--small"><span class="brand-mark"><AppIcon name="spark" /></span><span>trilha<span class="brand-dot">.</span></span></div>
    <p>Entenda por dentro. Explique com clareza.</p>
  </footer>
</template>


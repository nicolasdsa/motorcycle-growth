import { onMounted, ref } from 'vue'

const theme = ref<'light' | 'dark'>('light')

function applyTheme(next: 'light' | 'dark') {
  theme.value = next
  document.documentElement.dataset.theme = next
  localStorage.setItem('trilha-theme', next)
}

export function useTheme() {
  onMounted(() => {
    const stored = localStorage.getItem('trilha-theme') as 'light' | 'dark' | null
    const preferred = matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
    applyTheme(stored ?? preferred)
  })
  return {
    theme,
    toggleTheme: () => applyTheme(theme.value === 'light' ? 'dark' : 'light'),
  }
}


<script setup lang="ts">
defineProps<{ name: 'spark' | 'sun' | 'moon' | 'book' | 'arrow' | 'search' | 'heart' | 'check' | 'menu' }>()
</script>

<template>
  <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
    <path v-if="name === 'spark'" d="M12 2 14.3 8.2 21 10l-6.7 1.8L12 18l-2.3-6.2L3 10l6.7-1.8L12 2Z" />
    <template v-else-if="name === 'sun'"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.9 4.9l1.4 1.4m11.4 11.4 1.4 1.4M2 12h2m16 0h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></template>
    <path v-else-if="name === 'moon'" d="M20.5 15.2A8.5 8.5 0 0 1 8.8 3.5a8.5 8.5 0 1 0 11.7 11.7Z" />
    <template v-else-if="name === 'book'"><path d="M4 5.5A2.5 2.5 0 0 1 6.5 3H11v16H6.5A2.5 2.5 0 0 0 4 21.5v-16Z"/><path d="M20 5.5A2.5 2.5 0 0 0 17.5 3H13v16h4.5a2.5 2.5 0 0 1 2.5 2.5v-16Z"/></template>
    <path v-else-if="name === 'arrow'" d="m9 18 6-6-6-6" />
    <template v-else-if="name === 'search'"><circle cx="11" cy="11" r="7"/><path d="m20 20-4-4"/></template>
    <path v-else-if="name === 'heart'" d="M20.8 4.6a5.5 5.5 0 0 0-7.8 0L12 5.7l-1.1-1.1a5.5 5.5 0 0 0-7.8 7.8l1.1 1.1L12 21l7.8-7.5 1.1-1.1a5.5 5.5 0 0 0-.1-7.8Z" />
    <path v-else-if="name === 'check'" d="m5 12 4 4L19 6" />
    <template v-else-if="name === 'menu'"><path d="M4 7h16M4 12h16M4 17h16"/></template>
  </svg>
</template>


import { createRouter, createWebHistory } from 'vue-router'
import HomePage from '../pages/HomePage.vue'
import LessonPage from '../pages/LessonPage.vue'
import LibraryPage from '../pages/LibraryPage.vue'

export default createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'home', component: HomePage },
    { path: '/biblioteca', name: 'library', component: LibraryPage },
    { path: '/aulas/:id', name: 'lesson', component: LessonPage },
  ],
  scrollBehavior(to, _from, savedPosition) {
    if (savedPosition) return savedPosition
    if (to.hash) return { el: to.hash, behavior: 'smooth' }
    return { top: 0 }
  },
})


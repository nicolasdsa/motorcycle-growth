import type { LessonCreate, LessonDetail, LessonListItem, Progress } from '../types/lesson'

const API_URL = import.meta.env.VITE_API_URL ?? '/api'

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_URL}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  })
  if (!response.ok) {
    const body = await response.json().catch(() => null)
    const message = body?.detail?.message ?? body?.detail ?? 'Não foi possível concluir a solicitação.'
    throw new Error(typeof message === 'string' ? message : 'A especificação recebida é inválida.')
  }
  if (response.status === 204) return undefined as T
  return response.json() as Promise<T>
}

export const api = {
  createLesson: (payload: LessonCreate) =>
    request<LessonDetail>('/lessons', { method: 'POST', body: JSON.stringify(payload) }),
  listLessons: (query = '') =>
    request<LessonListItem[]>(`/lessons${query ? `?q=${encodeURIComponent(query)}` : ''}`),
  getLesson: (id: string) => request<LessonDetail>(`/lessons/${id}`),
  deleteLesson: (id: string) => request<void>(`/lessons/${id}`, { method: 'DELETE' }),
  favorite: (id: string, isFavorite: boolean) =>
    request<LessonDetail>(`/lessons/${id}/favorite`, {
      method: 'PATCH',
      body: JSON.stringify({ is_favorite: isFavorite }),
    }),
  getProgress: (id: string) => request<Progress>(`/lessons/${id}/progress`),
  saveProgress: (id: string, progress: Omit<Progress, 'lesson_id' | 'percentage'>) =>
    request<Progress>(`/lessons/${id}/progress`, {
      method: 'PUT',
      body: JSON.stringify(progress),
    }),
  saveAnswer: (lessonId: string, questionId: string, answer: string) =>
    request(`/lessons/${lessonId}/answers/${questionId}`, {
      method: 'PUT',
      body: JSON.stringify({ answer }),
    }),
}


import { render, screen } from '@testing-library/vue'
import VisualizationRenderer from '../visualizations/VisualizationRenderer.vue'
import type { Visualization } from '../types/lesson'

const visual: Visualization = {
  id: 'flow', type: 'request-flow', title: 'Fluxo', teaching_goal: 'Entender o caminho',
  elements: [{ id: 'a', label: 'Cliente', kind: 'client', description: 'Origem' }, { id: 'b', label: 'Servidor', kind: 'server', description: 'Destino' }],
  relations: [{ source: 'a', target: 'b', label: 'HTTP' }], initial_state: {}, steps: [], captions: [], controls: [],
  accessible_description: 'Cliente envia uma requisição ao servidor.', asset_id: null, data: {},
}

test('renders a registered visual and its accessible description', async () => {
  render(VisualizationRenderer, { props: { visual } })
  expect(screen.getByText('Fluxo')).toBeTruthy()
  expect(screen.getByText('Ouvir / ler descrição do diagrama')).toBeTruthy()
})

test('falls back safely for an unknown visual', () => {
  render(VisualizationRenderer, { props: { visual: { ...visual, type: 'future-visual' } } })
  expect(screen.getByText(/ainda não tem um renderizador/)).toBeTruthy()
})

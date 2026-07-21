import { fireEvent, render, screen } from '@testing-library/vue'
import ConceptMap from '../lesson/ConceptMap.vue'
import type { ConceptMap as ConceptMapData } from '../types/lesson'

const map: ConceptMapData = {
  id: 'map-test', title: 'Mapa de teste', teaching_goal: 'Relacionar ideias com clareza.',
  accessible_description: 'Seis conceitos conectados para revisar a aula.',
  nodes: [
    { id: 'core', label: 'Núcleo', summary: 'A ideia principal.', mnemonic: 'Comece aqui.', kind: 'core' },
    { id: 'one', label: 'Um', summary: 'Vizinho direto.', mnemonic: 'Conecte um.', kind: 'concept' },
    { id: 'two', label: 'Dois', summary: 'Outro vizinho.', mnemonic: 'Conecte dois.', kind: 'decision' },
    { id: 'three', label: 'Três', summary: 'Não é vizinho.', mnemonic: 'Observe três.', kind: 'risk' },
    { id: 'four', label: 'Quatro', summary: 'Também distante.', mnemonic: 'Observe quatro.', kind: 'metric' },
    { id: 'five', label: 'Cinco', summary: 'Fecha o mapa.', mnemonic: 'Feche cinco.', kind: 'concept' },
  ],
  edges: [
    { source: 'core', target: 'one' }, { source: 'core', target: 'two' }, { source: 'one', target: 'five' },
    { source: 'two', target: 'four' }, { source: 'three', target: 'four' },
  ],
}

test('renders the concept map title and nodes', async () => {
  render(ConceptMap, { props: { map } })
  expect(screen.getByText('Mapa de teste')).toBeTruthy()
  expect(await screen.findByRole('button', { name: /Núcleo/ })).toBeTruthy()
  expect(await screen.findByRole('button', { name: /Cinco/ })).toBeTruthy()
})

test('highlights only the direct neighborhood on hover and focus', async () => {
  render(ConceptMap, { props: { map } })
  const core = await screen.findByRole('button', { name: /Núcleo/ })
  const connected = await screen.findByRole('button', { name: /Um/ })
  const distant = await screen.findByRole('button', { name: /Três/ })
  await fireEvent.pointerEnter(core)
  expect(core.classList.contains('active')).toBe(true)
  expect(connected.classList.contains('connected')).toBe(true)
  expect(distant.classList.contains('dim')).toBe(true)
  expect(screen.getByText('A ideia principal.')).toBeTruthy()
  await fireEvent.blur(core)
  expect(distant.classList.contains('dim')).toBe(false)
})

test('updates a node position while dragging', async () => {
  const { container } = render(ConceptMap, { props: { map } })
  const core = await screen.findByRole('button', { name: /Núcleo/ })
  const before = core.getAttribute('transform')
  const stage = container.querySelector('.concept-map') as HTMLElement
  Object.defineProperty(stage, 'getBoundingClientRect', { value: () => ({ left: 0, top: 0, width: 760, height: 440 }) })
  await fireEvent.pointerDown(core, { pointerId: 1, clientX: 180, clientY: 145 })
  await fireEvent.pointerMove(core, { pointerId: 1, clientX: 260, clientY: 210 })
  expect(core.getAttribute('transform')).not.toBe(before)
  await fireEvent.pointerUp(core, { pointerId: 1 })
})

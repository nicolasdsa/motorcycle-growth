import { fireEvent, render, screen } from '@testing-library/vue'
import LoadBalancerLab from '../activities/LoadBalancerLab.vue'
import type { InteractiveActivity } from '../types/lesson'

const activity: InteractiveActivity = {
  id: 'lab', type: 'simulation-playground', title: 'Laboratório',
  teaching_goal: 'Observar capacidade e falha de servidores.',
  instructions: ['Altere tráfego'], accessible_description: 'Laboratório acessível.',
  config: { traffic: 900, maxTraffic: 2400, algorithm: 'round-robin', servers: [
    { id: 'a', name: 'A', capacity: 500 }, { id: 'b', name: 'B', capacity: 500 }, { id: 'c', name: 'C', capacity: 500 },
  ] },
}

test('updates capacity when a server fails', async () => {
  render(LoadBalancerLab, { props: { activity } })
  expect(screen.getByText('1500')).toBeTruthy()
  await fireEvent.click(screen.getAllByText('Provocar falha')[0])
  expect(screen.getByText('1000')).toBeTruthy()
})


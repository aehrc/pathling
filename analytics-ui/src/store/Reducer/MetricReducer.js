import { Map, OrderedSet } from 'immutable'

const initialState = Map({
  metrics: null,
  loading: false,
  selectedMetrics: OrderedSet(),
})

export default (state = initialState, action) => {
  switch (action.type) {
    case 'REQUEST_METRICS':
      return state.set('loading', true)
    case 'RECEIVE_METRICS':
      return state.merge({ metrics: action.metrics, loading: false })
    case 'SELECT_METRIC':
      return state.set(
        'selectedMetrics',
        state.get('selectedMetrics').add(action.reference),
      )
    case 'DESELECT_METRIC':
      return state.set(
        'selectedMetrics',
        state.get('selectedMetrics').remove(action.reference),
      )
    default:
      return state
  }
}

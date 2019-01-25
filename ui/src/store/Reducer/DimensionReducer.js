import { Map, OrderedSet } from 'immutable'

const initialState = Map({
  dimensions: null,
  loading: false,
  selectedDimensionAttributes: OrderedSet(),
})

export default (state = initialState, action) => {
  switch (action.type) {
    case 'REQUEST_DIMENSIONS':
      return state.set('loading', true)
    case 'RECEIVE_DIMENSIONS':
      return state.merge({ dimensions: action.dimensions, loading: false })
    case 'SELECT_DIMENSION_ATTRIBUTE':
      return state.set(
        'selectedDimensionAttributes',
        state.get('selectedDimensionAttributes').add(action.reference),
      )
    case 'DESELECT_DIMENSION_ATTRIBUTE':
      return state.set(
        'selectedDimensionAttributes',
        state.get('selectedDimensionAttributes').remove(action.reference),
      )
    default:
      return state
  }
}

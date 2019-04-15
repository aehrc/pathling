import { Map } from 'immutable'

const initialState = Map({
  groupings: null,
  loading: false,
})

export default (state = initialState, action) => {
  switch (action.type) {
    case 'REQUEST_QUERY_RESULT':
      return state.set('loading', true)
    case 'RECEIVE_QUERY_RESULT':
      return state.merge({ queryResult: action.queryResult, loading: false })
    default:
      return state
  }
}

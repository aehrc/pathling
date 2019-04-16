import { Map, fromJS } from 'immutable'

const initialState = Map({
  groupings: null,
  loading: false,
})

export default (state = initialState, action) => {
  switch (action.type) {
    case 'REQUEST_QUERY_RESULT':
      return state.set('loading', true)
    case 'RECEIVE_QUERY_RESULT':
      return state.merge({
        queryResult: fromJS(action.queryResult),
        loading: false,
      })
    case 'RECEIVE_QUERY_RESULT_ERROR':
      return state.merge({
        error: Map({
          message: action.message,
          opOutcome: action.opOutcome,
        }),
        loading: false,
      })
    default:
      return state
  }
}

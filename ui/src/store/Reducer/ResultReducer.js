import { Map, fromJS } from 'immutable'

const initialState = Map({
  groupings: null,
  loading: false,
  error: null,
})

export default (state = initialState, action) => {
  switch (action.type) {
    case 'REQUEST_QUERY_RESULT':
      return state.set('loading', true)
    case 'QUERY_RESULT':
      return state.merge({
        groupings: groupingsFromResult(action.result),
        loading: false,
        error: null,
      })
    case 'QUERY_ERROR':
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

function groupingsFromResult(result) {
  return result
    .get('parameter')
    .filter(p => p.get('name') === 'grouping')
    .map(p =>
      Map({
        part: p.get('part'),
      }),
    )
}

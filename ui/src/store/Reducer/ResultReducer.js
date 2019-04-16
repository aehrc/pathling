import { Map, fromJS } from 'immutable'

const initialState = Map({
  query: null,
  groupings: null,
  loading: false,
  error: null,
})

export default (state = initialState, action) => {
  switch (action.type) {
    case 'QUERY_REQUEST':
      return initialState.set('loading', true)
    case 'QUERY_RESULT':
      return state.merge({
        query: action.query,
        groupings: groupingsFromResult(action.result),
        loading: false,
        error: null,
      })
    case 'QUERY_ERROR':
      return initialState.merge({
        error: Map({
          message: action.message,
          opOutcome: action.opOutcome,
        }),
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

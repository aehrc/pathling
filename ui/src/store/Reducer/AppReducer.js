import { Map } from 'immutable'

const initialState = Map({
  error: null,
})

export default (state = initialState, action) => {
  // noinspection JSRedundantSwitchStatement
  switch (action.type) {
    case 'CATCH_ERROR':
      return state.set(
        'error',
        Map({
          message: action.message,
          opOutcome: action.opOutcome,
        }),
      )
    default:
      return state
  }
}

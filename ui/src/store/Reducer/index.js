import { combineReducers } from 'redux-immutable'

import QueryReducer from './QueryReducer'
import ResultReducer from './ResultReducer'

export default combineReducers({
  query: QueryReducer,
  result: ResultReducer,
})

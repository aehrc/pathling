import { combineReducers } from 'redux-immutable'

import AppReducer from './AppReducer'
import QueryReducer from './QueryReducer'
import ResultReducer from './ResultReducer'

export default combineReducers({
  app: AppReducer,
  query: QueryReducer,
  result: ResultReducer,
})

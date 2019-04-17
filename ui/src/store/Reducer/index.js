/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { combineReducers } from 'redux-immutable'

import QueryReducer from './QueryReducer'
import ResultReducer from './ResultReducer'

export default combineReducers({
  query: QueryReducer,
  result: ResultReducer,
})

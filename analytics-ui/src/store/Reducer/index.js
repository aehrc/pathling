import { combineReducers } from 'redux-immutable'

import AppReducer from './AppReducer'
import DimensionReducer from './DimensionReducer'
import MetricReducer from './MetricReducer'
import QueryReducer from './QueryReducer'

export default combineReducers({
  app: AppReducer,
  dimensions: DimensionReducer,
  metrics: MetricReducer,
  query: QueryReducer,
})

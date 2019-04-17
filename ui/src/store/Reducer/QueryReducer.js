/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Map, List } from 'immutable'

const initialState = Map({
  aggregations: List(),
  groupings: List(),
})

export default (state = initialState, action) => {
  switch (action.type) {
    case 'ADD_AGGREGATION':
      return state.set(
        'aggregations',
        state.get('aggregations').push(Map(action.aggregation)),
      )
    case 'REMOVE_AGGREGATION':
      return state.set(
        'aggregations',
        state.get('aggregations').delete(action.index),
      )
    case 'ADD_GROUPING':
      return state.set(
        'groupings',
        state.get('groupings').push(Map(action.grouping)),
      )
    case 'REMOVE_GROUPING':
      return state.set('groupings', state.get('groupings').delete(action.index))
    case 'CLEAR_QUERY':
      return initialState
    default:
      return state
  }
}

export const addAggregation = aggregation => ({
  type: 'ADD_AGGREGATION',
  aggregation,
})
export const removeAggregation = index => ({
  type: 'REMOVE_AGGREGATION',
  index,
})

export const addGrouping = grouping => ({
  type: 'ADD_GROUPING',
  grouping,
})
export const removeGrouping = index => ({
  type: 'REMOVE_GROUPING',
  index,
})

export const clearQuery = () => ({
  type: 'CLEAR_QUERY',
})

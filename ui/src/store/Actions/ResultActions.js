import http from 'axios'
import { Map, fromJS } from 'immutable'

import { opOutcomeFromJsonResponse } from '../../fhir/OperationOutcome'

export const queryRequest = () => ({
  type: 'QUERY_REQUEST',
})

export const queryResult = (result, query) => ({
  type: 'QUERY_RESULT',
  result,
  query,
})

export const queryError = (message, opOutcome) => ({
  type: 'QUERY_ERROR',
  message,
  opOutcome,
})

export const fetchQueryResult = () => (dispatch, getState) => {
  try {
    const aggregations = getState().getIn(['query', 'aggregations']),
      groupings = getState().getIn(['query', 'groupings']),
      aggregationParams = aggregations.map(aggregation => ({
        name: 'aggregation',
        part: [
          {
            name: 'label',
            valueString: aggregation.get('label'),
          },
          {
            name: 'expression',
            valueString: aggregation.get('expression'),
          },
        ],
      })),
      groupingParams = groupings.map(grouping => ({
        name: 'grouping',
        part: [
          {
            name: 'label',
            valueString: grouping.get('label'),
          },
          {
            name: 'expression',
            valueString: grouping.get('expression'),
          },
        ],
      })),
      query = {
        resourceType: 'Parameters',
        parameter: aggregationParams.concat(groupingParams),
      }
    if (aggregations.size === 0) {
      // noinspection ExceptionCaughtLocallyJS
      throw new Error('Query must have at least one aggregation.')
    }
    dispatch(queryRequest())
    return http
      .post('http://localhost:8090/fhir/$aggregate-query', query, {
        headers: {
          'Content-Type': 'application/fhir+json',
          Accept: 'application/fhir+json',
        },
      })
      .then(response => {
        if (response.data.resourceType !== 'Parameters')
          throw 'Response is not of type Parameters.'
        const result = Map(fromJS(response.data))
        dispatch(queryResult(result, getState().get('query')))
        return result
      })
      .catch(error => {
        if (
          error.response &&
          error.response.headers['content-type'].includes(
            'application/fhir+json',
          )
        ) {
          const opOutcome = opOutcomeFromJsonResponse(error.response.data)
          dispatch(queryError(opOutcome.message, opOutcome))
        } else dispatch(queryError(error.message))
      })
  } catch (error) {
    dispatch(queryError(error.message))
  }
}

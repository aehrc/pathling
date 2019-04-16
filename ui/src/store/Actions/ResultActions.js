import http from 'axios'
import { Map, fromJS } from 'immutable'

import { catchError } from './AppActions'
import { opOutcomeFromJsonResponse } from '../../fhir/OperationOutcome'

export const requestQueryResult = () => ({
  type: 'REQUEST_QUERY_RESULT',
})

export const receiveQueryResult = queryResult => ({
  type: 'RECEIVE_QUERY_RESULT',
  queryResult,
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
    dispatch(requestQueryResult())
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
        const queryResult = Map(fromJS(response.data))
        dispatch(receiveQueryResult(queryResult))
        return queryResult
      })
      .catch(error => {
        if (
          error.response.headers['content-type'].includes(
            'application/fhir+json',
          )
        ) {
          const opOutcome = opOutcomeFromJsonResponse(error.response.data)
          dispatch(catchError(opOutcome.message, opOutcome))
        } else dispatch(catchError(error.message))
      })
  } catch (error) {
    dispatch(catchError(error.message))
  }
}

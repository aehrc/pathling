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
    const dimensionAttributes = getState().getIn([
        'dimensions',
        'selectedDimensionAttributes',
      ]),
      metrics = getState().getIn(['metrics', 'selectedMetrics']),
      query = {
        resourceType: 'Query',
        meta: {
          profile: [
            'https://clinsight.csiro.au/fhir/StructureDefinition/query-0',
          ],
        },
        dimensionAttribute: dimensionAttributes.toArray(),
        metric: metrics.toArray(),
      },
      parameters = {
        resourceType: 'Parameters',
        parameter: [
          {
            name: 'query',
            resource: query,
          },
        ],
      }
    if (metrics.size === 0)
      throw new Error('Query must have at least one metric.')
    dispatch(requestQueryResult())
    return http
      .post('http://localhost:8080/fhir/$query', parameters, {
        headers: {
          'Content-Type': 'application/json+fhir',
          Accept: 'application/json+fhir',
        },
      })
      .then(response => {
        if (response.data.resourceType !== 'QueryResult')
          throw 'Response is not of type QueryResult.'
        const queryResult = Map(fromJS(response.data))
        dispatch(receiveQueryResult(queryResult))
        return queryResult
      })
      .catch(error => {
        if (
          error.response.headers['content-type'].includes(
            'application/json+fhir',
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

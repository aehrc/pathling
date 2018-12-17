import http from 'axios'
import { OrderedSet, Map, fromJS } from 'immutable'

import { catchError } from './AppActions'
import { opOutcomeFromJsonResponse } from '../../fhir/OperationOutcome'

export const requestMetrics = () => ({
  type: 'REQUEST_METRICS',
})

export const receiveMetrics = metrics => ({
  type: 'RECEIVE_METRICS',
  metrics,
})

export const selectMetric = reference => ({
  type: 'SELECT_METRIC',
  reference,
})

export const deselectMetric = reference => ({
  type: 'DESELECT_METRIC',
  reference,
})

export const fetchMetrics = () => dispatch => {
  dispatch(requestMetrics())
  return http
    .get('http://localhost:8080/fhir/Metric', {
      headers: { Accept: 'application/json+fhir' },
    })
    .then(response => {
      if (response.data.resourceType !== 'Bundle')
        throw 'Response is not of type Bundle.'
      const metrics = OrderedSet(
        response.data.entry.map(entry => Map(fromJS(entry.resource))),
      )
      dispatch(receiveMetrics(metrics))
      return metrics
    })
    .catch(error => {
      if (
        error.response.headers['content-type'].includes('application/json+fhir')
      ) {
        const opOutcome = opOutcomeFromJsonResponse(error.response.data)
        dispatch(catchError(opOutcome.message, opOutcome))
      } else dispatch(catchError(error.message))
    })
}

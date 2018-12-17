import http from 'axios'
import { OrderedSet, Map, fromJS } from 'immutable'

import { catchError } from './AppActions'
import { opOutcomeFromJsonResponse } from '../../fhir/OperationOutcome'

export const requestDimensions = () => ({
  type: 'REQUEST_DIMENSIONS',
})

export const receiveDimensions = dimensions => ({
  type: 'RECEIVE_DIMENSIONS',
  dimensions,
})

export const selectDimensionAttribute = reference => ({
  type: 'SELECT_DIMENSION_ATTRIBUTE',
  reference,
})

export const deselectDimensionAttribute = reference => ({
  type: 'DESELECT_DIMENSION_ATTRIBUTE',
  reference,
})

export const fetchDimensions = () => dispatch => {
  dispatch(requestDimensions())
  return http
    .get('http://localhost:8080/fhir/Dimension', {
      headers: { Accept: 'application/json+fhir' },
    })
    .then(response => {
      if (response.data.resourceType !== 'Bundle')
        throw 'Response is not of type Bundle.'
      const dimensions = OrderedSet(
        response.data.entry.map(entry => Map(fromJS(entry.resource))),
      )
      dispatch(receiveDimensions(dimensions))
      return dimensions
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

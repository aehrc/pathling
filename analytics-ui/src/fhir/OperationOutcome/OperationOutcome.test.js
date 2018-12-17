import { opOutcomeFromJsonResponse } from '../../src/fhir/core.js'

import opOutcome from '../fixtures/operationOutcome.json'

describe('opOutcomeFromJsonResponse', () => {
  it('should return a correct OpOutcomeError', () =>
    expect(opOutcomeFromJsonResponse(opOutcome)).toMatchSnapshot())
})

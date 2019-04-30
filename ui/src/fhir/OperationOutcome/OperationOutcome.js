/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

// @flow

import pick from 'lodash.pick'

type Issue = {
  details?: { coding?: { display?: string }, text?: string },
  diagnostics?: string,
}

/**
 * Custom error class for representing an error returned from a FHIR API as an
 * OperationOutcome resource.
 */
export class OpOutcomeError extends Error {
  issue: Issue

  constructor(issue: Issue) {
    const message =
      issue.details && issue.details.coding && issue.details.coding.display
        ? issue.details.coding.display
        : issue.diagnostics
    super(message)
    this.name = 'OpOutcomeError'
    this.issue = issue
    if (Error.captureStackTrace !== undefined)
      Error.captureStackTrace(this, OpOutcomeError)
  }
}

type OpOutcomeFromJsonResponseArg =
  | {
      resourceType: 'OperationOutcome',
      issue: Issue[],
    }
  | {}
export const opOutcomeFromJsonResponse = (
  parsed: OpOutcomeFromJsonResponseArg,
): OpOutcomeError => {
  // $FlowFixMe: See https://github.com/facebook/flow/issues/4328
  if (parsed.resourceType !== 'OperationOutcome' || parsed.issue.length === 0)
    throw new Error('Unable to parse response as OperationOutcome.')
  return new OpOutcomeError(
    pick(
      // We only ever look at the first issue described within an
      // OperationOutcome resource.
      parsed.issue[0],
      'severity',
      'code',
      'details',
      'diagnostics',
      'location',
      'expression',
    ),
  )
}

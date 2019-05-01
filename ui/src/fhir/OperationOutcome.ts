/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { OperationOutcome, OperationOutcomeIssue } from "./Types";

/**
 * Custom error class for representing an error returned from a FHIR API as an
 * OperationOutcome resource.
 */
export class OpOutcomeError extends Error {
  issue: OperationOutcomeIssue;

  constructor(issue: OperationOutcomeIssue) {
    const message = issue.diagnostics;
    super(message);
    this.name = "OpOutcomeError";
    this.issue = issue;
    if (Error.captureStackTrace !== undefined)
      Error.captureStackTrace(this, OpOutcomeError);
  }
}

export const opOutcomeFromJsonResponse = (parsed: OperationOutcome) => {
  if (
    parsed.resourceType !== "OperationOutcome" ||
    !parsed.issue ||
    parsed.issue.length === 0
  )
    throw new Error("Unable to parse response as OperationOutcome.");
  return new OpOutcomeError(parsed.issue[0]);
};

/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { ErrorState } from "./ErrorReducer";
import { OpOutcomeError } from "../fhir/OperationOutcome";

interface CatchError extends ErrorState {
  type: "CATCH_ERROR";
}

interface ClearError {
  type: "CLEAR_ERROR";
}

export type ErrorAction = CatchError | ClearError;

export const catchError = (
  message: string,
  opOutcome?: OpOutcomeError
): CatchError => ({
  type: "CATCH_ERROR",
  message,
  opOutcome
});

export const clearError = () => ({ type: "CLEAR_ERROR" });

/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { OpOutcomeError } from "../fhir/OperationOutcome";
import { ErrorAction } from "./ErrorActions";

export interface ErrorState {
  message: string;
  opOutcome?: OpOutcomeError;
}

export default (state: ErrorState = null, action: ErrorAction): ErrorState => {
  switch (action.type) {
    case "CATCH_ERROR":
      return { message: action.message, opOutcome: action.opOutcome };
    case "CLEAR_ERROR":
      return null;
    default:
      return state;
  }
};

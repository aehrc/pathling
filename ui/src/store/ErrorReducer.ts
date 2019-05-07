/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { OpOutcomeError } from "../fhir/OperationOutcome";
import { ErrorAction } from "./ErrorActions";

export interface Error {
  message: string;
  opOutcome?: OpOutcomeError;
}

export default (state: Error = null, action: ErrorAction): Error => {
  switch (action.type) {
    case "CATCH_ERROR":
      return { message: action.message, opOutcome: action.opOutcome };
    case "CLEAR_ERROR":
      return null;
    default:
      return state;
  }
};

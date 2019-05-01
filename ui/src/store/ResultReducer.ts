/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Query } from "./QueryReducer";
import { OpOutcomeError } from "../fhir/OperationOutcome";
import { ResultAction } from "./ResultActions";
import { Parameters, Parameter } from "../fhir/Types";

export interface Result {
  query: Query;
  groupings: Parameter[];
  loading: boolean;
  error: Error | null;
}

export interface Error {
  message: string;
  opOutcome?: OpOutcomeError;
}

const initialState: Result = {
  query: null,
  groupings: null,
  loading: false,
  error: null
};

const ResultReducer = (state = initialState, action: ResultAction): Result => {
  switch (action.type) {
    case "QUERY_REQUEST":
      return {
        ...initialState,
        loading: true
      };
    case "QUERY_RESULT":
      return {
        ...state,
        query: action.query,
        groupings: groupingsFromResult(action.result),
        loading: false,
        error: null
      };
    case "QUERY_ERROR":
      return {
        ...state,
        error: {
          message: action.message,
          opOutcome: action.opOutcome
        }
      };
    default:
      return state;
  }
};

function groupingsFromResult(result: Parameters): Parameter[] {
  return result.parameter.filter((p: Parameter) => p.name === "grouping");
}

export default ResultReducer;

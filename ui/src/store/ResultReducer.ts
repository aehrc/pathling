/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Query } from "./QueryReducer";
import { ResultAction } from "./ResultActions";
import { Parameter, Parameters } from "../fhir/Types";

export interface Result {
  query: Query;
  groupings: Parameter[];
  loading: boolean;
}

const initialState: Result = {
  query: null,
  groupings: null,
  loading: false
};

const ResultReducer = (state = initialState, action: ResultAction): Result => {
  switch (action.type) {
    case "SEND_QUERY_REQUEST":
      return {
        ...initialState,
        loading: true
      };
    case "RECEIVE_QUERY_RESULT":
      return {
        ...state,
        query: action.query,
        groupings: groupingsFromResult(action.result),
        loading: false
      };
    case "CATCH_QUERY_ERROR":
      return {
        ...state,
        loading: false
      };
    default:
      return state;
  }
};

function groupingsFromResult(result: Parameters): Parameter[] {
  if (!result.parameter) return [];
  return result.parameter.filter((p: Parameter) => p.name === "grouping");
}

export default ResultReducer;

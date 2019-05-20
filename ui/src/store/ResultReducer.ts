/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { QueryState } from "./QueryReducer";
import { ResultAction } from "./ResultActions";
import { Parameter, Parameters } from "../fhir/Types";
import { CancelTokenSource } from "axios";

export interface ResultState {
  query: QueryState;
  groupings: Parameter[];
  loading: boolean;
  cancel: CancelTokenSource;
}

const initialState: ResultState = {
  query: null,
  groupings: null,
  loading: false,
  cancel: null
};

const ResultReducer = (
  state = initialState,
  action: ResultAction
): ResultState => {
  switch (action.type) {
    case "SEND_QUERY_REQUEST":
      return {
        ...initialState,
        loading: true,
        cancel: action.cancel
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
    case "CLEAR_RESULT":
      return initialState;
    default:
      return state;
  }
};

function groupingsFromResult(result: Parameters): Parameter[] {
  if (!result.parameter) return [];
  return result.parameter.filter((p: Parameter) => p.name === "grouping");
}

export default ResultReducer;

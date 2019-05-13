/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import http, { AxiosPromise } from "axios";
import { Dispatch } from "redux";

import {
  OpOutcomeError,
  opOutcomeFromJsonResponse
} from "../fhir/OperationOutcome";
import { GlobalState } from "./index";
import { Aggregation, Grouping, QueryState } from "./QueryReducer";
import {
  AggregationRequestParameter,
  GroupingRequestParameter,
  Parameters
} from "../fhir/Types";
import { catchError, clearError } from "./ErrorActions";

interface SendQueryRequest {
  type: "SEND_QUERY_REQUEST";
}

interface ReceiveQueryResult {
  type: "RECEIVE_QUERY_RESULT";
  result: Parameters;
  query: QueryState;
}

interface CatchQueryError {
  type: "CATCH_QUERY_ERROR";
  message: string;
  opOutcome?: OpOutcomeError;
}

export type ResultAction =
  | SendQueryRequest
  | ReceiveQueryResult
  | CatchQueryError;

export const sendQueryRequest = (): SendQueryRequest => ({
  type: "SEND_QUERY_REQUEST"
});

export const receiveQueryResult = (
  result: Parameters,
  query: QueryState
): ReceiveQueryResult => ({
  type: "RECEIVE_QUERY_RESULT",
  result,
  query
});

export const catchQueryError = (
  message: string,
  opOutcome?: OpOutcomeError
): CatchQueryError => ({
  type: "CATCH_QUERY_ERROR",
  message,
  opOutcome
});

const aggregationToParam = (
  aggregation: Aggregation
): AggregationRequestParameter => {
  const param = {
    name: "aggregation",
    part: [
      {
        name: "expression",
        valueString: aggregation.expression
      }
    ]
  };
  if (aggregation.label) {
    param.part.push({
      name: "label",
      valueString: aggregation.label
    });
  }
  return param;
};

const groupingToParam = (grouping: Grouping): GroupingRequestParameter => {
  const param = {
    name: "grouping",
    part: [
      {
        name: "expression",
        valueString: grouping.expression
      }
    ]
  };
  if (grouping.label) {
    param.part.push({
      name: "label",
      valueString: grouping.label
    });
  }
  return param;
};

/**
 * Fetches a result based on the current query within state, then dispatches the
 * relevant actions to signal either a successful or error response.
 */
export const fetchQueryResult = (fhirServer: string) => (
  dispatch: Dispatch,
  getState: () => GlobalState
): AxiosPromise => {
  const aggregations = getState().query.aggregations,
    groupings = getState().query.groupings,
    aggregationParams = aggregations.map(aggregationToParam),
    groupingParams = groupings.map(groupingToParam),
    query: Parameters = {
      resourceType: "Parameters",
      parameter: aggregationParams.concat(groupingParams)
    };

  if (aggregations.length === 0) {
    dispatch(catchQueryError("Query must have at least one aggregation."));
  }
  if (getState().error) dispatch(clearError());
  dispatch(sendQueryRequest());
  return http
    .post(`${fhirServer}/$aggregate-query`, query, {
      headers: {
        "Content-Type": "application/fhir+json",
        Accept: "application/fhir+json"
      }
    })
    .then(response => {
      if (response.data.resourceType !== "Parameters")
        throw "Response is not of type Parameters.";
      const result = response.data;
      dispatch(receiveQueryResult(result, getState().query));
      return result;
    })
    .catch(error => {
      if (
        error.response &&
        error.response.headers["content-type"].includes("application/fhir+json")
      ) {
        const opOutcome = opOutcomeFromJsonResponse(error.response.data);
        dispatch(catchQueryError(opOutcome.message, opOutcome));
        dispatch(catchError(opOutcome.message, opOutcome));
      } else {
        dispatch(catchQueryError(error.message));
        dispatch(catchError(error.message));
      }
    });
};

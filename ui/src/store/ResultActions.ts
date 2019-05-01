/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import http, { AxiosPromise } from "axios";

import {
  OpOutcomeError,
  opOutcomeFromJsonResponse
} from "../fhir/OperationOutcome";
import { Dispatch } from "redux";
import { GlobalState } from "./index";
import { Aggregation, Grouping, Query } from "./QueryReducer";
import {
  AggregationRequestParameter,
  GroupingRequestParameter,
  Parameters
} from "../fhir/Types";

interface QueryRequest {
  type: "QUERY_REQUEST";
}

interface QueryResult {
  type: "QUERY_RESULT";
  result: Parameters;
  query: Query;
}

interface QueryError {
  type: "QUERY_ERROR";
  message: string;
  opOutcome?: OpOutcomeError;
}

export type ResultAction = QueryRequest | QueryResult | QueryError;

export const queryRequest = (): QueryRequest => ({
  type: "QUERY_REQUEST"
});

export const queryResult = (result: Parameters, query: Query): QueryResult => ({
  type: "QUERY_RESULT",
  result,
  query
});

export const queryError = (
  message: string,
  opOutcome?: OpOutcomeError
): QueryError => ({
  type: "QUERY_ERROR",
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
export const fetchQueryResult = () => (
  dispatch: Dispatch,
  getState: () => GlobalState
): AxiosPromise => {
  try {
    const aggregations = getState().query.aggregations,
      groupings = getState().query.groupings,
      aggregationParams = aggregations.map(aggregationToParam),
      groupingParams = groupings.map(groupingToParam),
      query: Parameters = {
        resourceType: "Parameters",
        parameter: aggregationParams.concat(groupingParams)
      };

    if (aggregations.length === 0) {
      throw new Error("Query must have at least one aggregation.");
    }
    dispatch(queryRequest());
    return http
      .post(
        // TODO: Extract this URL out into configuration.
        "http://localhost:8090/fhir/$aggregate-query",
        query,
        {
          headers: {
            "Content-Type": "application/fhir+json",
            Accept: "application/fhir+json"
          }
        }
      )
      .then(response => {
        if (response.data.resourceType !== "Parameters")
          throw "Response is not of type Parameters.";
        const result = response.data;
        dispatch(queryResult(result, getState().query));
        return result;
      })
      .catch(error => {
        if (
          error.response &&
          error.response.headers["content-type"].includes(
            "application/fhir+json"
          )
        ) {
          const opOutcome = opOutcomeFromJsonResponse(error.response.data);
          dispatch(queryError(opOutcome.message, opOutcome));
        } else dispatch(queryError(error.message));
      });
  } catch (error) {
    dispatch(queryError(error.message));
  }
};

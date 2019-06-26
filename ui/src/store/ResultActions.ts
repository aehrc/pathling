/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import http, { AxiosPromise, CancelTokenSource } from "axios";
import { Dispatch } from "redux";
import Alerter from "../components/Alerter";

import {
  OpOutcomeError,
  opOutcomeFromJsonResponse
} from "../fhir/OperationOutcome";
import {
  AggregationRequestParameter,
  FilterRequestParameter,
  GroupingRequestParameter,
  Parameter,
  Parameters
} from "../fhir/Types";
import { GlobalState } from "./index";
import { Aggregation, Filter, Grouping, Query } from "./QueryReducer";

interface SendQueryRequest {
  type: "SEND_QUERY_REQUEST";
  startTime: number;
  cancel: CancelTokenSource;
}

interface ReceiveQueryResult {
  type: "RECEIVE_QUERY_RESULT";
  result: Parameters;
  query: Query;
  executionTime: number;
}

interface CatchQueryError {
  type: "CATCH_QUERY_ERROR";
  message: string;
  opOutcome?: OpOutcomeError;
}

interface ClearResult {
  type: "CLEAR_RESULT";
}

export type ResultAction =
  | SendQueryRequest
  | ReceiveQueryResult
  | CatchQueryError
  | ClearResult;

export const sendQueryRequest = (
  startTime: number,
  cancel: CancelTokenSource
): SendQueryRequest => ({
  type: "SEND_QUERY_REQUEST",
  startTime,
  cancel
});

export const receiveQueryResult = (
  result: Parameters,
  query: Query,
  executionTime: number
): ReceiveQueryResult => ({
  type: "RECEIVE_QUERY_RESULT",
  result,
  query,
  executionTime
});

export const catchQueryError = (
  message: string,
  opOutcome?: OpOutcomeError
): CatchQueryError => ({
  type: "CATCH_QUERY_ERROR",
  message,
  opOutcome
});

export const clearResult = () => ({ type: "CLEAR_RESULT" });

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

const filterToParam = (filter: Filter): FilterRequestParameter => ({
  name: "filter",
  valueString: filter.expression
});

/**
 * Fetches a result based on the current query within state, then dispatches the
 * relevant actions to signal either a successful or error response.
 */
export const fetchQueryResult = (fhirServer: string) => (
  dispatch: Dispatch,
  getState: () => GlobalState
): AxiosPromise => {
  const {
      query: { query }
    } = getState(),
    { aggregations, groupings, filters } = query,
    aggregationParams: Parameter[] = aggregations.map(aggregationToParam),
    groupingParams: Parameter[] = groupings.map(groupingToParam),
    filterParams: Parameter[] = filters.map(filterToParam),
    parameters: Parameters = {
      resourceType: "Parameters",
      parameter: aggregationParams.concat(groupingParams).concat(filterParams)
    };

  if (aggregations.length === 0) {
    dispatch(catchQueryError("Query must have at least one aggregation."));
  }
  let cancel = http.CancelToken.source();
  const result = http
    .post(`${fhirServer}/$aggregate-query`, parameters, {
      headers: {
        "Content-Type": "application/fhir+json",
        Accept: "application/fhir+json"
      },
      cancelToken: cancel.token
    })
    .then(response => {
      if (response.data.resourceType !== "Parameters")
        throw "Response is not of type Parameters.";
      const result = response.data,
        startTime = getState().result.startTime,
        executionTime = startTime ? performance.now() - startTime : null;
      dispatch(receiveQueryResult(result, query, executionTime));
      return result;
    })
    .catch(error => {
      // Don't report an error if this is a request cancellation.
      if (http.isCancel(error)) return;
      if (
        error.response &&
        error.response.headers["content-type"].includes("application/fhir+json")
      ) {
        const opOutcome = opOutcomeFromJsonResponse(error.response.data);
        dispatch(catchQueryError(opOutcome.message, opOutcome));
        Alerter.show({ message: opOutcome.message, intent: "danger" });
      } else {
        dispatch(catchQueryError(error.message));
        Alerter.show({ message: error.message, intent: "danger" });
      }
    });
  dispatch(sendQueryRequest(performance.now(), cancel));
  return result;
};

/**
 * Cancels any outstanding request and clears the result state.
 */
export const cancelAndClearResult = () => (
  dispatch: Dispatch,
  getState: () => GlobalState
): void => {
  const cancel = getState().result.cancel;
  if (cancel) cancel.cancel();
  dispatch(clearResult());
};

/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { QueryState } from "./QueryReducer";
import { SavedQueriesAction } from "./SavedQueriesActions";

export interface SavedQuery extends QueryState {}

export interface SavedQueryWithStatus {
  query: SavedQuery;
  status: "saving" | "loaded" | "deleting" | "error";
}

export type SavedQueries = { [name: string]: SavedQuery };
export type SavedQueriesWithStatuses = { [name: string]: SavedQueryWithStatus };

export type SavedQueriesState = {
  queries: SavedQueriesWithStatuses;
  status: "not-initialised" | "loading" | "loaded" | "error";
};

const initialState: SavedQueriesState = {
  queries: {},
  status: "not-initialised"
};

export default (
  state: SavedQueriesState = initialState,
  action: SavedQueriesAction
): SavedQueriesState => {
  switch (action.type) {
    case "SEND_LOAD_QUERIES_REQUEST":
      return {
        ...state,
        status: "loading"
      };
    case "RECEIVE_LOAD_QUERIES_RESPONSE":
      return {
        ...state,
        queries: Object.keys(action.queries).reduce(
          (result: SavedQueriesWithStatuses, key) => {
            result[key] = {
              query: { ...action.queries[key] },
              status: "loaded"
            };
            return result;
          },
          {}
        ),
        status: "loaded"
      };
    case "CATCH_LOAD_QUERIES_ERROR":
      return {
        ...state,
        status: "error"
      };
    case "SEND_SAVE_QUERY_REQUEST":
      return {
        ...state,
        queries: {
          ...state.queries,
          [action.name]: { query: { ...action.query }, status: "saving" }
        }
      };
    case "RECEIVE_SAVE_QUERY_RESPONSE":
      return {
        ...state,
        queries: {
          ...state.queries,
          [action.name]: { ...state.queries[action.name], status: "loaded" }
        }
      };
    case "SEND_DELETE_QUERY_REQUEST":
      return {
        ...state,
        queries: {
          ...state.queries,
          [action.name]: { ...state.queries[action.name], status: "deleting" }
        }
      };
    case "RECEIVE_DELETE_QUERY_RESPONSE":
      return {
        ...state,
        queries: Object.keys(state.queries).reduce(
          (result: SavedQueriesWithStatuses, key) => {
            if (key !== action.name) result[key] = state.queries[key];
            return result;
          },
          {}
        )
      };
    default:
      return state;
  }
};

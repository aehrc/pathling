/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Query } from "./QueryReducer";
import { SavedQueriesAction } from "./SavedQueriesActions";

export interface SavedQuery {
  id: string;
  name: string;
  query: Query;
}

export interface SavedQueryWithStatus extends SavedQuery {
  status: "saving" | "editing" | "saved" | "deleting" | "error";
}

export type SavedQueries = SavedQuery[];
export type SavedQueriesWithStatuses = SavedQueryWithStatus[];

export type SavedQueriesState = {
  queries: SavedQueriesWithStatuses;
  status: "not-initialised" | "loading" | "loaded" | "error";
  focusedQuery?: string;
};

const initialState: SavedQueriesState = {
  queries: [],
  status: "not-initialised",
  focusedQuery: null
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
        queries: action.queries.map(query => ({
          ...query,
          status: "saved"
        })),
        status: "loaded"
      };
    case "CATCH_LOAD_QUERIES_ERROR":
      return {
        ...state,
        status: "error"
      };
    case "EDIT_SAVED_QUERY":
      return {
        ...state,
        queries: state.queries.map(query =>
          query.id === action.id ? { ...query, status: "editing" } : query
        ),
        focusedQuery: action.id
      };
    case "CANCEL_EDITING_SAVED_QUERY":
      return {
        ...state,
        queries: state.queries.map(query =>
          query.id === action.id ? { ...query, status: "saved" } : query
        )
      };
    case "SEND_SAVE_QUERY_REQUEST":
      return {
        ...state,
        queries: state.queries.concat({ ...action.query, status: "saving" })
      };
    case "RECEIVE_SAVE_QUERY_RESPONSE":
      return {
        ...state,
        queries: state.queries.map(query =>
          query.id === action.id ? { ...query, status: "editing" } : query
        ),
        focusedQuery: action.id
      };
    case "CATCH_SAVE_QUERY_ERROR":
      return {
        ...state,
        queries: state.queries.map(query =>
          query.id === action.id ? { ...query, status: "error" } : query
        )
      };
    case "SEND_UPDATE_QUERY_REQUEST":
      return {
        ...state,
        queries: state.queries.map(query =>
          query.id === action.query.id
            ? { ...action.query, status: "saving" }
            : query
        )
      };
    case "RECEIVE_UPDATE_QUERY_RESPONSE":
      return {
        ...state,
        queries: state.queries.map(query =>
          query.id === action.id ? { ...query, status: "saved" } : query
        )
      };
    case "CATCH_UPDATE_QUERY_ERROR":
      return {
        ...state,
        queries: state.queries.map(query =>
          query.id === action.id ? { ...query, status: "error" } : query
        )
      };
    case "SEND_DELETE_QUERY_REQUEST":
      return {
        ...state,
        queries: state.queries.map(query =>
          query.id === action.id ? { ...query, status: "deleting" } : query
        )
      };
    case "RECEIVE_DELETE_QUERY_RESPONSE":
      return {
        ...state,
        queries: state.queries.reduce(
          (
            result: SavedQueriesWithStatuses,
            existingQuery: SavedQueryWithStatus
          ) => {
            if (existingQuery.id !== action.id)
              result = result.concat(existingQuery);
            return result;
          },
          []
        )
      };
    case "RECEIVE_SAVED_QUERY_FOCUS":
      return {
        ...state,
        focusedQuery: null
      };
    default:
      return state;
  }
};

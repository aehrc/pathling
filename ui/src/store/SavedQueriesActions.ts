/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { SavedQueries, SavedQuery } from "./SavedQueriesReducer";
import { Dispatch } from "redux";

export interface SendLoadQueriesRequest {
  type: "SEND_LOAD_QUERIES_REQUEST";
}

export interface ReceiveLoadQueriesResponse {
  type: "RECEIVE_LOAD_QUERIES_RESPONSE";
  queries: SavedQueries;
}

export interface CatchLoadQueriesError {
  type: "CATCH_LOAD_QUERIES_ERROR";
  message: string;
}

export interface SendSaveQueryRequest {
  type: "SEND_SAVE_QUERY_REQUEST";
  name: string;
  query: SavedQuery;
}

export interface ReceiveSaveQueryResponse {
  type: "RECEIVE_SAVE_QUERY_RESPONSE";
  name: string;
}

export interface CatchSaveQueryError {
  type: "CATCH_SAVE_QUERY_ERROR";
  message: string;
}

export interface SendDeleteQueryRequest {
  type: "SEND_DELETE_QUERY_REQUEST";
  name: string;
}

export interface ReceiveDeleteQueryResponse {
  type: "RECEIVE_DELETE_QUERY_RESPONSE";
  name: string;
}

export interface CatchDeleteQueryError {
  type: "CATCH_DELETE_QUERY_ERROR";
  message: string;
}

export type SavedQueriesAction =
  | SendLoadQueriesRequest
  | ReceiveLoadQueriesResponse
  | CatchLoadQueriesError
  | SendSaveQueryRequest
  | ReceiveSaveQueryResponse
  | CatchLoadQueriesError
  | SendDeleteQueryRequest
  | ReceiveDeleteQueryResponse
  | CatchDeleteQueryError;

export const sendLoadQueriesRequest = (): SendLoadQueriesRequest => ({
  type: "SEND_LOAD_QUERIES_REQUEST"
});

export const receiveLoadQueriesResponse = (
  queries: SavedQueries
): ReceiveLoadQueriesResponse => ({
  type: "RECEIVE_LOAD_QUERIES_RESPONSE",
  queries
});

export const catchLoadQueriesError = (
  message: string
): CatchLoadQueriesError => ({ type: "CATCH_LOAD_QUERIES_ERROR", message });

export const sendSaveQueryRequest = (
  name: string,
  query: SavedQuery
): SendSaveQueryRequest => ({ type: "SEND_SAVE_QUERY_REQUEST", name, query });

export const receiveSaveQueryResponse = (
  name: string
): ReceiveSaveQueryResponse => ({ type: "RECEIVE_SAVE_QUERY_RESPONSE", name });

export const catchSaveQueryError = (message: string): CatchSaveQueryError => ({
  type: "CATCH_SAVE_QUERY_ERROR",
  message
});

export const sendDeleteQueryRequest = (
  name: string
): SendDeleteQueryRequest => ({ type: "SEND_DELETE_QUERY_REQUEST", name });

export const receiveDeleteQueryResponse = (
  name: string
): ReceiveDeleteQueryResponse => ({
  type: "RECEIVE_DELETE_QUERY_RESPONSE",
  name
});

export const catchDeleteQueryError = (
  message: string
): CatchDeleteQueryError => ({
  type: "CATCH_DELETE_QUERY_ERROR",
  message
});

const localStorageKey = "savedQueries";

/**
 * Load all queries. The current implementation uses local storage.
 */
export const loadQueries = () => (dispatch: Dispatch): any => {
  const storage = window.localStorage;
  if (!storage) dispatch(catchLoadQueriesError("Local storage not available"));
  dispatch(sendLoadQueriesRequest());
  try {
    const existingQueriesJSON = storage.getItem(localStorageKey),
      existingQueries =
        existingQueriesJSON === null ? {} : JSON.parse(existingQueriesJSON);
    dispatch(receiveLoadQueriesResponse(existingQueries));
  } catch (e) {
    dispatch(
      catchLoadQueriesError(
        "Error loading queries from local storage: " + e.message
      )
    );
  }
};

/**
 * Saves a query. The current implementation uses local storage.
 */
export const saveQuery = (name: string, query: SavedQuery) => (
  dispatch: Dispatch
): any => {
  const storage = window.localStorage;
  if (!storage) dispatch(catchSaveQueryError("Local storage not available"));
  dispatch(sendSaveQueryRequest(name, query));
  try {
    const existingQueriesJSON = storage.getItem(localStorageKey),
      existingQueries =
        existingQueriesJSON === null ? {} : JSON.parse(existingQueriesJSON),
      newQueries = { ...existingQueries, [name]: query };
    storage.setItem(localStorageKey, JSON.stringify(newQueries));
  } catch (e) {
    dispatch(
      catchSaveQueryError("Error saving query to local storage: " + e.message)
    );
  }
  dispatch(receiveSaveQueryResponse(name));
};

/**
 * Deletes a query. The current implementation uses local storage.
 */
export const deleteQuery = (name: string) => (dispatch: Dispatch): any => {
  const storage = window.localStorage;
  if (!storage) dispatch(catchDeleteQueryError("Local storage not available"));
  dispatch(sendDeleteQueryRequest(name));
  try {
    const existingQueriesJSON = storage.getItem(localStorageKey),
      existingQueries =
        existingQueriesJSON === null ? {} : JSON.parse(existingQueriesJSON),
      newQueries = Object.keys(existingQueries).reduce(
        (result: SavedQueries, key) => {
          if (key !== name) result[key] = existingQueries[key];
          return result;
        },
        {}
      );
    storage.setItem(localStorageKey, JSON.stringify(newQueries));
  } catch (e) {
    dispatch(
      catchSaveQueryError("Error saving query to local storage: " + e.message)
    );
  }
  dispatch(receiveDeleteQueryResponse(name));
};

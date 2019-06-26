/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Dispatch } from "redux";
import Alerter from "../components/Alerter";
import { clearElementTreeFocus } from "./ElementTreeActions";
import { GlobalState } from "./index";
import { clearQuery, loadQuery } from "./QueryActions";
import { Query } from "./QueryReducer";
import { SavedQueries, SavedQuery } from "./SavedQueriesReducer";
import uuidv4 from "uuid/v4";

export interface SendLoadQueriesRequest {
  type: "SEND_LOAD_QUERIES_REQUEST";
}

export interface ReceiveLoadQueriesResponse {
  type: "RECEIVE_LOAD_QUERIES_RESPONSE";
  queries: SavedQueries;
}

export interface CatchLoadQueriesError {
  type: "CATCH_LOAD_QUERIES_ERROR";
}

export interface EditSavedQuery {
  type: "EDIT_SAVED_QUERY";
  id: string;
}

export interface CancelEditingSavedQuery {
  type: "CANCEL_EDITING_SAVED_QUERY";
  id: string;
}

export interface SendSaveQueryRequest {
  type: "SEND_SAVE_QUERY_REQUEST";
  query: SavedQuery;
}

export interface ReceiveSaveQueryResponse {
  type: "RECEIVE_SAVE_QUERY_RESPONSE";
  id: string;
}

export interface CatchSaveQueryError {
  type: "CATCH_SAVE_QUERY_ERROR";
  id: string;
}

export interface SendUpdateQueryRequest {
  type: "SEND_UPDATE_QUERY_REQUEST";
  query: SavedQuery;
}

export interface ReceiveUpdateQueryResponse {
  type: "RECEIVE_UPDATE_QUERY_RESPONSE";
  id: string;
}

export interface CatchUpdateQueryError {
  type: "CATCH_UPDATE_QUERY_ERROR";
  id: string;
}

export interface SendDeleteQueryRequest {
  type: "SEND_DELETE_QUERY_REQUEST";
  id: string;
}

export interface ReceiveDeleteQueryResponse {
  type: "RECEIVE_DELETE_QUERY_RESPONSE";
  id: string;
}

export interface CatchDeleteQueryError {
  type: "CATCH_DELETE_QUERY_ERROR";
  id: string;
}

export interface ReceiveSavedQueryFocus {
  type: "RECEIVE_SAVED_QUERY_FOCUS";
}

export type SavedQueriesAction =
  | SendLoadQueriesRequest
  | ReceiveLoadQueriesResponse
  | CatchLoadQueriesError
  | EditSavedQuery
  | CancelEditingSavedQuery
  | SendSaveQueryRequest
  | ReceiveSaveQueryResponse
  | CatchSaveQueryError
  | SendUpdateQueryRequest
  | ReceiveUpdateQueryResponse
  | CatchUpdateQueryError
  | SendDeleteQueryRequest
  | ReceiveDeleteQueryResponse
  | CatchDeleteQueryError
  | ReceiveSavedQueryFocus;

export const sendLoadQueriesRequest = (): SendLoadQueriesRequest => ({
  type: "SEND_LOAD_QUERIES_REQUEST"
});

export const receiveLoadQueriesResponse = (
  queries: SavedQueries
): ReceiveLoadQueriesResponse => ({
  type: "RECEIVE_LOAD_QUERIES_RESPONSE",
  queries
});

export const catchLoadQueriesError = (): CatchLoadQueriesError => ({
  type: "CATCH_LOAD_QUERIES_ERROR"
});

export const editSavedQuery = (id: string): EditSavedQuery => ({
  type: "EDIT_SAVED_QUERY",
  id
});

export const cancelEditingSavedQuery = (
  id: string
): CancelEditingSavedQuery => ({ type: "CANCEL_EDITING_SAVED_QUERY", id });

export const sendSaveQueryRequest = (
  query: SavedQuery
): SendSaveQueryRequest => ({
  type: "SEND_SAVE_QUERY_REQUEST",
  query
});

export const receiveSaveQueryResponse = (
  id: string
): ReceiveSaveQueryResponse => ({
  type: "RECEIVE_SAVE_QUERY_RESPONSE",
  id
});

export const catchSaveQueryError = (id: string): CatchSaveQueryError => ({
  type: "CATCH_SAVE_QUERY_ERROR",
  id
});

export const sendUpdateQueryRequest = (
  query: SavedQuery
): SendUpdateQueryRequest => ({
  type: "SEND_UPDATE_QUERY_REQUEST",
  query
});

export const receiveUpdateQueryResponse = (
  id: string
): ReceiveUpdateQueryResponse => ({
  type: "RECEIVE_UPDATE_QUERY_RESPONSE",
  id
});

export const catchUpdateQueryError = (id: string): CatchUpdateQueryError => ({
  type: "CATCH_UPDATE_QUERY_ERROR",
  id
});

export const sendDeleteQueryRequest = (id: string): SendDeleteQueryRequest => ({
  type: "SEND_DELETE_QUERY_REQUEST",
  id
});

export const receiveDeleteQueryResponse = (
  id: string
): ReceiveDeleteQueryResponse => ({
  type: "RECEIVE_DELETE_QUERY_RESPONSE",
  id
});

export const catchDeleteQueryError = (id: string): CatchDeleteQueryError => ({
  type: "CATCH_DELETE_QUERY_ERROR",
  id
});

export const receiveSavedQueryFocus = (): ReceiveSavedQueryFocus => ({
  type: "RECEIVE_SAVED_QUERY_FOCUS"
});

const localStorageKey = "savedQueries";

const getLocalStorage = (
  dispatch: Dispatch,
  action: SavedQueriesAction
): Storage => {
  const storage = window.localStorage;
  if (!storage) {
    dispatch(action);
    Alerter.show({ message: "Local storage not available", intent: "danger" });
    return null;
  } else {
    return storage;
  }
};

/**
 * Load all queries. The current implementation uses local storage.
 */
export const loadQueries = () => (dispatch: Dispatch): any => {
  const storage = getLocalStorage(dispatch, catchLoadQueriesError());
  if (!storage) return;
  dispatch(sendLoadQueriesRequest());
  try {
    const existingQueriesJSON = storage.getItem(localStorageKey),
      existingQueries =
        existingQueriesJSON === null ? [] : JSON.parse(existingQueriesJSON);
    dispatch(receiveLoadQueriesResponse(existingQueries));
  } catch (e) {
    dispatch(catchLoadQueriesError());
    Alerter.show({
      message: "Error loading queries from local storage: " + e.message,
      intent: "danger"
    });
  }
};

/**
 * Saves a query. The current implementation uses local storage.
 */
export const saveQuery = (query: Query) => (dispatch: Dispatch): any => {
  const identifiedQuery: SavedQuery = {
      id: uuidv4(),
      name: "Untitled query",
      query
    },
    storage = getLocalStorage(
      dispatch,
      catchSaveQueryError(identifiedQuery.id)
    );
  if (!storage) return;
  dispatch(sendSaveQueryRequest(identifiedQuery));
  try {
    const existingQueriesJSON = storage.getItem(localStorageKey),
      existingQueries =
        existingQueriesJSON === null ? [] : JSON.parse(existingQueriesJSON);
    if (
      existingQueries.find(
        (existingQuery: SavedQuery) => existingQuery.id === identifiedQuery.id
      )
    ) {
      dispatch(catchSaveQueryError(identifiedQuery.id));
      Alerter.show({
        message: "An unexpected error occurred",
        intent: "danger"
      });
      console.debug(
        `Attempted to save with existing query ID: ${identifiedQuery.id}`
      );
    }
    const newQueries = existingQueries.concat(identifiedQuery);
    storage.setItem(localStorageKey, JSON.stringify(newQueries));
  } catch (e) {
    dispatch(catchSaveQueryError(identifiedQuery.id));
    Alerter.show({
      message: "Error saving query to local storage: " + e.message,
      intent: "danger"
    });
  }
  dispatch(receiveSaveQueryResponse(identifiedQuery.id));
  dispatch(loadQuery(identifiedQuery));
};

/**
 * Updates an existing query. The current implementation uses local storage.
 */
export const updateQuery = (query: SavedQuery) => (dispatch: Dispatch): any => {
  const storage = getLocalStorage(dispatch, catchUpdateQueryError(query.id));
  if (!storage) return;
  dispatch(sendUpdateQueryRequest(query));
  try {
    const existingQueriesJSON = storage.getItem(localStorageKey),
      existingQueries =
        existingQueriesJSON === null ? [] : JSON.parse(existingQueriesJSON);
    if (
      !existingQueries.find(
        (existingQuery: SavedQuery) => existingQuery.id === query.id
      )
    ) {
      dispatch(catchUpdateQueryError(query.id));
      Alerter.show({
        message: "An unexpected error occurred",
        intent: "danger"
      });
      console.debug(`Attempted to update non-existent query ID: ${query.id}`);
      return;
    }
    const newQueries = existingQueries.map((existingQuery: SavedQuery) =>
      existingQuery.id === query.id ? query : existingQuery
    );
    storage.setItem(localStorageKey, JSON.stringify(newQueries));
  } catch (e) {
    dispatch(catchUpdateQueryError(query.id));
    Alerter.show({
      message: "Error saving query to local storage: " + e.message,
      intent: "danger"
    });
  }
  dispatch(receiveUpdateQueryResponse(query.id));
  dispatch(loadQuery(query));
};

/**
 * Deletes a query. The current implementation uses local storage.
 */
export const deleteQuery = (id: string) => (
  dispatch: Dispatch,
  getState: () => GlobalState
): any => {
  const storage = getLocalStorage(dispatch, catchDeleteQueryError(id));
  if (!storage) return;
  dispatch(sendDeleteQueryRequest(id));
  try {
    const existingQueriesJSON = storage.getItem(localStorageKey),
      existingQueries =
        existingQueriesJSON === null ? [] : JSON.parse(existingQueriesJSON);
    if (
      !existingQueries.find(
        (existingQuery: SavedQuery) => existingQuery.id === id
      )
    ) {
      dispatch(catchDeleteQueryError(id));
      Alerter.show({
        message: "An unexpected error occurred",
        intent: "danger"
      });
      console.debug(`Attempted to delete non-existent query ID: ${id}`);
      return;
    }
    const newQueries = existingQueries.reduce(
      (result: SavedQueries, existingQuery: SavedQuery) => {
        if (existingQuery.id !== id) result = result.concat(existingQuery);
        return result;
      },
      []
    );
    storage.setItem(localStorageKey, JSON.stringify(newQueries));
  } catch (e) {
    dispatch(catchDeleteQueryError(id));
    Alerter.show({
      message: "Error saving query to local storage: " + e.message,
      intent: "danger"
    });
  }
  dispatch(receiveDeleteQueryResponse(id));
  if (getState().query.id === id) {
    dispatch(clearQuery());
    dispatch(clearElementTreeFocus());
  }
};

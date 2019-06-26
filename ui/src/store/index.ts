/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import {
  AnyAction,
  applyMiddleware,
  combineReducers,
  createStore
} from "redux";
import { createLogger } from "redux-logger";
import thunk, { ThunkDispatch } from "redux-thunk";
import ConfigReducer, { ConfigState } from "./ConfigReducer";
import ElementTreeReducer, { ElementTreeState } from "./ElementTreeReducer";
import QueryReducer, { QueryState } from "./QueryReducer";

import ResultReducer, { ResultState } from "./ResultReducer";
import { loadQueries } from "./SavedQueriesActions";
import SavedQueriesReducer, { SavedQueriesState } from "./SavedQueriesReducer";

export interface GlobalState {
  query: QueryState;
  result: ResultState;
  elementTree: ElementTreeState;
  savedQueries: SavedQueriesState;
  config: ConfigState;
}

const Reducer = combineReducers({
  query: QueryReducer,
  result: ResultReducer,
  elementTree: ElementTreeReducer,
  savedQueries: SavedQueriesReducer,
  config: ConfigReducer
});

// Enable console logging of Redux actions for all environments other than
// production.
const middleware = [thunk];
if (process.env.NODE_ENV !== "production") {
  // @ts-ignore
  middleware.push(createLogger());
}

const store = createStore(Reducer, applyMiddleware(...middleware));

// Kick off loading of queries.
(store.dispatch as ThunkDispatch<GlobalState, void, AnyAction>)(loadQueries());

export default store;

/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { createStore, applyMiddleware, combineReducers } from "redux";
import thunk from "redux-thunk";
import { createLogger } from "redux-logger";

import ResultReducer, { Result } from "./ResultReducer";
import QueryReducer, { Query } from "./QueryReducer";

export interface GlobalState {
  query: Query;
  result: Result;
}

const Reducer = combineReducers({
  query: QueryReducer,
  result: ResultReducer
});

// Enable console logging of Redux actions for all environments other than
// production.
const middleware = [thunk];
if (process.env.NODE_ENV !== "production") {
  // @ts-ignore
  middleware.push(createLogger());
}

export default createStore(Reducer, applyMiddleware(...middleware));

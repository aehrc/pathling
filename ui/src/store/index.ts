/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { createStore, applyMiddleware, combineReducers } from "redux";
import thunk from "redux-thunk";
import { createLogger } from "redux-logger";

import ResultReducer, { ResultState } from "./ResultReducer";
import QueryReducer, { QueryState } from "./QueryReducer";
import ElementTreeReducer, { ElementTreeState } from "./ElementTreeReducer";
import ConfigReducer, { ConfigState } from "./ConfigReducer";
import ErrorReducer, { ErrorState } from "./ErrorReducer";

export interface GlobalState {
  query: QueryState;
  result: ResultState;
  elementTree: ElementTreeState;
  config: ConfigState;
  error: ErrorState;
}

const Reducer = combineReducers({
  query: QueryReducer,
  result: ResultReducer,
  elementTree: ElementTreeReducer,
  config: ConfigReducer,
  error: ErrorReducer
});

// Enable console logging of Redux actions for all environments other than
// production.
const middleware = [thunk];
if (process.env.NODE_ENV !== "production") {
  // @ts-ignore
  middleware.push(createLogger());
}

export default createStore(Reducer, applyMiddleware(...middleware));

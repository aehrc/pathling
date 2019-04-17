/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { createStore, applyMiddleware } from 'redux'
import thunk from 'redux-thunk'
import { createLogger } from 'redux-logger'

import Reducer from './Reducer'

// Enable console logging of Redux actions for all environments other than
// production.
const middleware = [thunk]
if (process.env.NODE_ENV !== 'production') {
  middleware.push(createLogger())
}

export default createStore(Reducer, applyMiddleware(...middleware))

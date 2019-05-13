/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { ConfigAction } from "./ConfigActions";

export interface ConfigState {
  fhirServer?: string;
  version?: string;
}

const initialState: ConfigState = {};

export default (state = initialState, action: ConfigAction): ConfigState => {
  if (action.type === "RECEIVE_CONFIG") {
    return action.config;
  } else {
    return state;
  }
};

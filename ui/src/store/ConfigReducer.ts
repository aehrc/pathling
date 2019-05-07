/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { ConfigAction } from "./ConfigActions";

export interface Config {
  fhirServer?: string;
  version?: string;
}

const initialState: Config = {};

export default (state = initialState, action: ConfigAction): Config => {
  if (action.type === "RECEIVE_CONFIG") {
    return action.config;
  } else {
    return state;
  }
};

/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Dispatch } from "redux";
import http, { AxiosPromise } from "axios";

import { Config } from "./ConfigReducer";
import { catchError } from "./ErrorActions";

export interface ReceiveConfig {
  type: "RECEIVE_CONFIG";
  config: Config;
}

export type ConfigAction = ReceiveConfig;

export const receiveConfig = (config: Config): ReceiveConfig => ({
  type: "RECEIVE_CONFIG",
  config: config
});

export const fetchConfig = () => (dispatch: Dispatch): AxiosPromise => {
  return http
    .get("/config.json")
    .then(response => {
      dispatch(receiveConfig(response.data));
      return response.data;
    })
    .catch(error => dispatch(catchError(error.message)));
};

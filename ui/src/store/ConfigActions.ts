/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import http, { AxiosPromise } from "axios";
import { Dispatch } from "redux";

import { ConfigState } from "./ConfigReducer";
import Alerter from "../components/Alerter";

export interface ReceiveConfig {
  type: "RECEIVE_CONFIG";
  config: ConfigState;
}

export type ConfigAction = ReceiveConfig;

export const receiveConfig = (config: ConfigState): ReceiveConfig => ({
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
    .catch(error => Alerter.show({ message: error.message, intent: "danger" }));
};

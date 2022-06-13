/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

import axios, { AxiosRequestConfig } from "axios";
import { performance } from "just-performance";
import { getStatusUrl, waitForAsyncResult } from "./async";
import { PathlingClientOptionsResolved, QueryOptions } from "./index";
import { buildResponseError } from "./OperationOutcome";

/**
 * The FHIR JSON content type.
 */
export const FHIR_CONTENT_TYPE = "application/fhir+json";

/**
 * Create request config common to any request.
 */
export function requestConfig(
  url: string,
  options?: QueryOptions
): AxiosRequestConfig {
  const auth = { Authorization: `Bearer ${options?.token}` },
    preferAsync = { Prefer: "respond-async" };
  return {
    url,
    headers: {
      Accept: FHIR_CONTENT_TYPE,
      ...(options?.token ? auth : {}),
      ...(options?.preferAsync ? preferAsync : {})
    }
  };
}

/**
 * Create request config for a GET request.
 */
export function getConfig(
  url: string,
  params: URLSearchParams,
  options?: QueryOptions
): AxiosRequestConfig {
  return {
    ...requestConfig(url, options),
    method: "GET",
    params
  };
}

/**
 * Create request config for a POST request with a FHIR JSON body.
 */
export function postFhirConfig<T>(
  url: string,
  data: T,
  options?: QueryOptions
): AxiosRequestConfig<T> {
  const baseConfig = requestConfig(url, options);
  return {
    ...baseConfig,
    method: "POST",
    data,
    headers: { ...baseConfig.headers, "Content-Type": FHIR_CONTENT_TYPE }
  };
}

/**
 * Create request config for a POST request with an
 * `application/x-www-form-urlencoded` body.
 */
export function postFormConfig(
  url: string,
  params: URLSearchParams,
  options?: QueryOptions
): AxiosRequestConfig {
  const baseConfig = requestConfig(url, options);
  return {
    ...baseConfig,
    method: "POST",
    params
  };
}

/**
 * Execute a request configuration, taking into account a possible asynchronous
 * response.
 *
 * @param config The Axios request configuration.
 * @param message A message to use when logging the progress of the requesst.
 * @param clientOptions The relevant Pathling client options.
 * @param requestOptions The options specific to this query.
 */
export function makeRequest<I, O>(
  config: AxiosRequestConfig<I>,
  message: string,
  clientOptions: PathlingClientOptionsResolved,
  requestOptions?: QueryOptions
) {
  return addExecutionTime<O>(async () => {
    const response = await axios.request<O>(config);
    return response.status === 202
      ? waitForAsyncResult(
        getStatusUrl(response),
        message,
        clientOptions,
        requestOptions
      )
      : response.data;
  }).catch((e) => {
    throw buildResponseError(e);
  });
}

/**
 * Wraps a promise that returns an object, measuring the time to resolve and
 * adding this as an additional key in the final result.
 *
 * @param executor A function returning a Promise which resolves to an object
 */
export async function addExecutionTime<T = object>(
  executor: () => Promise<T>
): Promise<{ response: T; executionTime: number }> {
  const startTime = performance.now(),
    response: T = await executor();
  return {
    response,
    executionTime: performance.now() - startTime
  };
}

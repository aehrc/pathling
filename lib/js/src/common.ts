/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { getStatusUrl, waitForAsyncResult } from "./async.js";
import { PathlingClientOptionsResolved, QueryOptions } from "./index.js";

/**
 * The FHIR JSON content type.
 */
export const FHIR_CONTENT_TYPE = "application/fhir+json";

export interface RequestConfig {
  input: RequestInfo | URL;
  init?: RequestInit;
}

export interface ResponseWithExecutionTime<ResponseType> {
  response: ResponseType;
  executionTime: number;
}

/**
 * Create request config common to any request.
 */
export function requestConfig(
  url: string,
  options?: QueryOptions
): RequestConfig {
  const auth = { Authorization: `Bearer ${options?.token}` },
    preferAsync = { Prefer: "respond-async" };
  return {
    input: new URL(url),
    init: {
      headers: {
        Accept: FHIR_CONTENT_TYPE,
        ...(options?.token ? auth : {}),
        ...(options?.preferAsync ? preferAsync : {}),
      },
    },
  };
}

/**
 * Create request config for a GET request.
 */
export function getConfig(
  url: string,
  params?: URLSearchParams,
  options?: QueryOptions
): RequestConfig {
  const baseConfig = requestConfig(url, options);
  return {
    ...baseConfig,
    input: new URL(params ? `${url}?${params.toString()}` : url),
    init: {
      ...baseConfig.init,
      method: "GET",
    },
  };
}

/**
 * Create request config for a POST request with a FHIR JSON body.
 */
export function postFhirConfig(
  url: string,
  data: any,
  options?: QueryOptions
): RequestConfig {
  const baseConfig = requestConfig(url, options);
  return {
    ...baseConfig,
    init: {
      ...baseConfig.init,
      method: "POST",
      headers: {
        ...baseConfig.init?.headers,
        "Content-Type": FHIR_CONTENT_TYPE,
      },
      body: JSON.stringify(data),
    },
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
): RequestConfig {
  const baseConfig = requestConfig(url, options);
  return {
    ...baseConfig,
    init: {
      ...baseConfig.init,
      method: "POST",
      body: params,
    },
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
export function makeRequest<ResponseType>(
  config: RequestConfig,
  message: string,
  clientOptions: PathlingClientOptionsResolved,
  requestOptions?: QueryOptions
): Promise<ResponseWithExecutionTime<ResponseType>> {
  return addExecutionTime<ResponseType>(async () => {
    const response = await fetch(config.input, config.init);
    return response.status === 202
      ? waitForAsyncResult(
          getStatusUrl(response),
          message,
          clientOptions,
          requestOptions
        )
      : ((await response.json()) as ResponseType);
  });
}

/**
 * Wraps a promise that returns an object, measuring the time to resolve and
 * adding this as an additional key in the final result.
 *
 * @param executor A function returning a Promise which resolves to an object
 */
export async function addExecutionTime<ResponseType = object>(
  executor: () => Promise<ResponseType>
): Promise<ResponseWithExecutionTime<ResponseType>> {
  const startTime = performance.now(),
    response: ResponseType = await executor();
  return {
    response,
    executionTime: performance.now() - startTime,
  };
}

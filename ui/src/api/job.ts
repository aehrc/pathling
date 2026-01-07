/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
 *
 * Author: John Grimes
 */

import type {
  JobCancelOptions,
  JobStatusOptions,
  JobStatusResult,
} from "../types/api";
import { buildHeaders, checkResponse, resolveUrl } from "./utils";

/**
 * Checks the status of an async job.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Job status options including polling URL.
 * @returns The job status result with status, optional progress, and optional result.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const status = await jobStatus("https://example.com/fhir", {
 *   pollingUrl: "https://example.com/fhir/$job?id=abc-123",
 *   accessToken: "token123"
 * });
 * if (status.status === "complete") {
 *   console.log(status.result);
 * }
 */
export async function jobStatus(
  baseUrl: string,
  options: JobStatusOptions,
): Promise<JobStatusResult> {
  const url = resolveUrl(baseUrl, options.pollingUrl);
  const headers = buildHeaders({ accessToken: options.accessToken });

  const response = await fetch(url, {
    method: "GET",
    headers,
  });

  // 202 indicates job still in progress.
  if (response.status === 202) {
    const progressHeader = response.headers.get("X-Progress");
    return {
      status: "in-progress",
      progress: progressHeader ?? undefined,
    };
  }

  // 200 indicates job complete.
  if (response.status === 200) {
    const result: unknown = await response.json();

    return {
      status: "complete",
      result,
    };
  }

  await checkResponse(response, "Job status");

  // This should never be reached, but TypeScript needs it.
  throw new Error("Unexpected response");
}

/**
 * Cancels an async job.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Job cancel options including polling URL.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * await jobCancel("https://example.com/fhir", {
 *   pollingUrl: "https://example.com/fhir/$job?id=abc-123",
 *   accessToken: "token123"
 * });
 */
export async function jobCancel(
  baseUrl: string,
  options: JobCancelOptions,
): Promise<void> {
  const url = resolveUrl(baseUrl, options.pollingUrl);
  const headers = buildHeaders({ accessToken: options.accessToken });

  const response = await fetch(url, {
    method: "DELETE",
    headers,
  });

  await checkResponse(response, "Job cancel");
}

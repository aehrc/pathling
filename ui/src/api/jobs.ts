/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

import { buildHeaders, buildUrl, checkResponse } from "./utils";

import type { AuthOptions } from "./rest";
import type { Parameters, ParametersParameter } from "fhir/r4";

/** The externally visible status of a job, as returned by the {@code $jobs} operation. */
export type JobSummaryStatus =
  | "in-progress"
  | "completed"
  | "failed"
  | "cancelled";

/** The valid status values, used to validate parsed responses. */
const JOB_STATUSES: readonly JobSummaryStatus[] = [
  "in-progress",
  "completed",
  "failed",
  "cancelled",
];

/**
 * A single job as projected by the server's {@code $jobs} list operation.
 */
export interface JobSummary {
  id: string;
  operation: string;
  status: JobSummaryStatus;
  progress?: number;
  startTime: string;
  url: string;
}

/** Options for {@link listJobs}. */
export type ListJobsOptions = AuthOptions;

/**
 * Reads the first primitive value from a named part of a job parameter.
 *
 * @param part - The job parameter whose parts to search.
 * @param name - The part name to read.
 * @returns The part's string value, or undefined if absent.
 */
function partString(
  part: ParametersParameter,
  name: string,
): string | undefined {
  const found = part.part?.find((p) => p.name === name);
  if (!found) {
    return undefined;
  }
  return (
    found.valueString ?? found.valueCode ?? found.valueUri ?? found.valueInstant
  );
}

/**
 * Reads the integer value from a named part of a job parameter.
 *
 * @param part - The job parameter whose parts to search.
 * @param name - The part name to read.
 * @returns The part's integer value, or undefined if absent.
 */
function partInteger(
  part: ParametersParameter,
  name: string,
): number | undefined {
  const found = part.part?.find((p) => p.name === name);
  return found?.valueInteger;
}

/**
 * Parses a single {@code job} parameter into a {@link JobSummary}, returning null when a required
 * part is missing or the status is not recognised.
 *
 * @param jobParam - The {@code job} parameter to parse.
 * @returns The parsed job summary, or null if the entry is malformed.
 */
function parseJob(jobParam: ParametersParameter): JobSummary | null {
  const id = partString(jobParam, "id");
  const operation = partString(jobParam, "operation");
  const status = partString(jobParam, "status");
  const startTime = partString(jobParam, "startTime");
  const url = partString(jobParam, "url");

  if (!id || !operation || !status || !startTime || !url) {
    return null;
  }
  if (!JOB_STATUSES.includes(status as JobSummaryStatus)) {
    return null;
  }

  const progress = partInteger(jobParam, "progress");
  return {
    id,
    operation,
    status: status as JobSummaryStatus,
    ...(progress !== undefined ? { progress } : {}),
    startTime,
    url,
  };
}

/**
 * Parses a {@code $jobs} Parameters response into a list of job summaries, skipping malformed
 * entries.
 *
 * @param parameters - The FHIR Parameters resource returned by the operation.
 * @returns The parsed job summaries.
 */
export function parseJobsResponse(parameters: Parameters): JobSummary[] {
  return (parameters.parameter ?? [])
    .filter((param) => param.name === "job")
    .map(parseJob)
    .filter((job): job is JobSummary => job !== null);
}

/**
 * Lists the caller's asynchronous jobs via the server's {@code $jobs} operation.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Optional authentication options.
 * @returns The caller's jobs, newest first as ordered by the server.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {NotFoundError} When the request receives a 404 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const jobs = await listJobs("https://example.com/fhir", { accessToken: "token123" });
 */
export async function listJobs(
  baseUrl: string,
  options: ListJobsOptions = {},
): Promise<JobSummary[]> {
  const url = buildUrl(baseUrl, "/$jobs");
  const response = await fetch(url, {
    method: "GET",
    headers: buildHeaders({ accessToken: options.accessToken }),
  });

  await checkResponse(response, "Job list");

  const parameters = (await response.json()) as Parameters;
  return parseJobsResponse(parameters);
}

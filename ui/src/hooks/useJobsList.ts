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

import { useQuery } from "@tanstack/react-query";

import { listJobs } from "../api";
import { selectRefetchInterval } from "../components/jobs/jobsPresentation";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";

import type { JobSummary } from "../api";
import type { UseQueryResult } from "@tanstack/react-query";

/** The base query key for the job list; the FHIR base URL is appended per instance. */
export const JOBS_QUERY_KEY = ["jobs"] as const;

/**
 * Thin TanStack Query wrapper that polls the server's {@code $jobs} list. It refetches quickly while
 * any job is in progress and slowly otherwise, and refetches when the window regains focus so the
 * list stays current across tabs.
 *
 * @returns The query result holding the caller's jobs.
 */
export function useJobsList(): UseQueryResult<JobSummary[], Error> {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  return useQuery<JobSummary[], Error>({
    queryKey: [...JOBS_QUERY_KEY, fhirBaseUrl],
    queryFn: () => listJobs(fhirBaseUrl!, { accessToken }),
    enabled: !!fhirBaseUrl,
    refetchInterval: (query) => selectRefetchInterval(query.state.data ?? []),
    refetchOnWindowFocus: true,
  });
}

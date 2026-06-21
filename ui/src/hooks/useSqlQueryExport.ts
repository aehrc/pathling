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

import { useCallback, useRef } from "react";

import {
  jobCancel,
  jobStatus,
  sqlQueryExportDownload,
  sqlQueryExportKickOff,
} from "../api";
import { config } from "../config";
import { useAsyncJob } from "./useAsyncJob";
import { useAsyncJobCallbacks } from "./useAsyncJobCallbacks";
import { useAuth } from "../contexts/AuthContext";

import type { AsyncJobOptions, UseAsyncJobResult } from "./useAsyncJob";
import type {
  SqlQueryExportManifest,
  SqlQueryExportRequest,
} from "../types/sqlQuery";

/**
 * Options for the {@link useSqlQueryExport} hook (callbacks only).
 */
export type UseSqlQueryExportOptions = AsyncJobOptions;

/**
 * Result of the {@link useSqlQueryExport} hook.
 */
export interface UseSqlQueryExportResult extends UseAsyncJobResult<
  SqlQueryExportRequest,
  SqlQueryExportManifest
> {
  /** Downloads an output file from the manifest by its location URL. */
  download: (location: string) => Promise<ReadableStream>;
}

/**
 * Executes a `$sqlquery-export` operation with polling, reusing the synchronous run's query source.
 * A thin wrapper composing {@link useAsyncJob} with the export kick-off, status polling, and cancel
 * APIs.
 *
 * @param options - Optional callbacks for progress, completion, and error events.
 * @returns Hook result with status, result, control functions, and a download function.
 */
export function useSqlQueryExport(
  options?: UseSqlQueryExportOptions,
): UseSqlQueryExportResult {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const pollingUrlRef = useRef<string | undefined>(undefined);

  const callbacks = useAsyncJobCallbacks(options);

  const buildOptions = useCallback(
    (request: SqlQueryExportRequest) => ({
      kickOff: () =>
        sqlQueryExportKickOff(fhirBaseUrl!, { request, accessToken }),
      getJobId: (result: { pollingUrl: string }) => {
        pollingUrlRef.current = result.pollingUrl;
        return result.pollingUrl;
      },
      checkStatus: (pollingUrl: string) =>
        jobStatus(fhirBaseUrl!, { pollingUrl, accessToken }),
      isComplete: (status: { status: string }) => status.status === "complete",
      getResult: (status: { result?: unknown }) =>
        status.result as UseSqlQueryExportResult["result"],
      cancel: (pollingUrl: string) =>
        jobCancel(fhirBaseUrl!, { pollingUrl, accessToken }),
      pollingInterval: 3000,
    }),
    [fhirBaseUrl, accessToken],
  );

  const job = useAsyncJob(buildOptions, callbacks);

  const download = useCallback(
    (location: string) => sqlQueryExportDownload({ location, accessToken }),
    [accessToken],
  );

  return { ...job, download };
}

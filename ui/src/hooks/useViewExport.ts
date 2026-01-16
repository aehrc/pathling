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
  viewExportKickOff,
  viewExportDownload,
  jobStatus,
  jobCancel,
  extractJobIdFromUrl,
} from "../api";
import { config } from "../config";
import { useAsyncJob } from "./useAsyncJob";
import { useAsyncJobCallbacks } from "./useAsyncJobCallbacks";
import { useAuth } from "../contexts/AuthContext";

import type { AsyncJobOptions, UseAsyncJobResult } from "./useAsyncJob";
import type { ViewDefinition } from "../api";
import type { ViewExportManifest } from "../types/viewExport";

/**
 * Output format for asynchronous view export operations.
 */
export type ViewExportOutputFormat = "ndjson" | "csv" | "parquet";

/**
 * Request parameters for view export operations.
 */
export interface ViewExportRequest {
  /** Views to export. */
  views: Array<{
    viewDefinition: ViewDefinition;
    name?: string;
  }>;
  /** Output format. */
  format?: ViewExportOutputFormat;
  /** Whether to include header row (CSV only). */
  header?: boolean;
}

/**
 * Options for useViewExport hook (callbacks only).
 */
export type UseViewExportOptions = AsyncJobOptions;

/**
 * Result of useViewExport hook.
 */
export interface UseViewExportResult
  extends UseAsyncJobResult<ViewExportRequest, ViewExportManifest> {
  /** Function to download a file from the manifest. */
  download: (fileName: string) => Promise<ReadableStream>;
}

/**
 * Execute a view export operation with polling.
 */
export type UseViewExportFn = (
  options?: UseViewExportOptions,
) => UseViewExportResult;

/**
 * Execute a view export operation with polling.
 *
 * @param options - Optional callbacks for progress, completion, and error events.
 * @returns Hook result with status, result, and control functions including startWith.
 */
export const useViewExport: UseViewExportFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const pollingUrlRef = useRef<string | undefined>(undefined);

  const callbacks = useAsyncJobCallbacks(options);

  const buildOptions = useCallback(
    (request: ViewExportRequest) => ({
      kickOff: () =>
        viewExportKickOff(fhirBaseUrl!, {
          views: request.views,
          format: request.format,
          header: request.header,
          accessToken,
        }),
      getJobId: (result: { pollingUrl: string }) => {
        pollingUrlRef.current = result.pollingUrl;
        return result.pollingUrl;
      },
      checkStatus: (pollingUrl: string) =>
        jobStatus(fhirBaseUrl!, { pollingUrl, accessToken }),
      isComplete: (status: { status: string }) => status.status === "complete",
      getResult: (status: { result?: unknown }) =>
        status.result as UseViewExportResult["result"],
      cancel: (pollingUrl: string) =>
        jobCancel(fhirBaseUrl!, { pollingUrl, accessToken }),
      pollingInterval: 3000,
    }),
    [fhirBaseUrl, accessToken],
  );

  const job = useAsyncJob(buildOptions, callbacks);

  const download = useCallback(
    async (fileName: string) => {
      if (!pollingUrlRef.current) throw new Error("No polling URL available");
      const jobId = extractJobIdFromUrl(pollingUrlRef.current);
      return viewExportDownload(fhirBaseUrl!, {
        jobId,
        fileName,
        accessToken,
      });
    },
    [fhirBaseUrl, accessToken],
  );

  return { ...job, download };
};

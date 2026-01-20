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

import { useCallback } from "react";

import { importKickOff, jobStatus, jobCancel } from "../api";
import { config } from "../config";
import { useAsyncJob } from "./useAsyncJob";
import { useAsyncJobCallbacks } from "./useAsyncJobCallbacks";
import { useAuth } from "../contexts/AuthContext";

import type { AsyncJobOptions, UseAsyncJobResult } from "./useAsyncJob";
import type { ResourceType } from "../api";
import type { ImportFormat, SaveMode } from "../types/import";

/**
 * Request parameters for standard import operations.
 */
export interface ImportJobRequest {
  /** Source URLs to import from. */
  sources: string[];
  /** Resource types to import (optional filter). */
  resourceTypes?: string[];
  /** Save mode for the import operation. */
  saveMode: SaveMode;
  /** Input format for the import data. */
  inputFormat: ImportFormat;
}

/**
 * Options for useImport hook (callbacks only).
 */
export type UseImportOptions = AsyncJobOptions;

/**
 * Result of useImport hook.
 */
export type UseImportResult = UseAsyncJobResult<ImportJobRequest, void>;

/**
 * Execute a standard import operation with polling.
 */
export type UseImportFn = (options?: UseImportOptions) => UseImportResult;

/**
 * Execute a standard import operation with polling.
 *
 * @param options - Optional callbacks for progress, completion, and error events.
 * @returns Hook result with status, result, and control functions including startWith.
 */
export const useImport: UseImportFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  const callbacks = useAsyncJobCallbacks(options);

  const buildOptions = useCallback(
    (request: ImportJobRequest) => ({
      kickOff: () =>
        importKickOff(fhirBaseUrl!, {
          input: request.sources.map((url, i) => ({
            type: (request.resourceTypes?.[i] ?? "Bundle") as ResourceType,
            url,
          })),
          inputFormat: request.inputFormat,
          mode: request.saveMode,
          accessToken,
        }),
      getJobId: (result: { pollingUrl: string }) => result.pollingUrl,
      checkStatus: (pollingUrl: string) =>
        jobStatus(fhirBaseUrl!, { pollingUrl, accessToken }),
      isComplete: (status: { status: string }) => status.status === "complete",
      getResult: () => undefined,
      cancel: (pollingUrl: string) =>
        jobCancel(fhirBaseUrl!, { pollingUrl, accessToken }),
      pollingInterval: 3000,
    }),
    [fhirBaseUrl, accessToken],
  );

  return useAsyncJob(buildOptions, callbacks);
};

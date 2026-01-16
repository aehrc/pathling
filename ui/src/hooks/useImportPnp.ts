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

import { importPnpKickOff, jobStatus, jobCancel } from "../api";
import { config } from "../config";
import { useAsyncJob } from "./useAsyncJob";
import { useAsyncJobCallbacks } from "./useAsyncJobCallbacks";
import { useAuth } from "../contexts/AuthContext";

import type { AsyncJobOptions, UseAsyncJobResult } from "./useAsyncJob";
import type { ImportFormat, SaveMode } from "../types/import";

/**
 * Request parameters for passthrough (PnP) import operations.
 */
export interface ImportPnpJobRequest {
  /** Export URL to import from. */
  exportUrl: string;
  /** Save mode for the import operation. */
  saveMode: SaveMode;
  /** Input format for the import data. */
  inputFormat: ImportFormat;
  // Bulk export passthrough parameters.
  /** Resource types to include in the export. */
  types?: string[];
  /** Export resources modified after this timestamp. */
  since?: string;
  /** Export resources modified before this timestamp. */
  until?: string;
  /** Elements to include in the export output. */
  elements?: string;
  /** FHIR search queries to filter resources during export. */
  typeFilters?: string[];
  /** Pre-defined sets of associated data to include. */
  includeAssociatedData?: string[];
}

/**
 * Options for useImportPnp hook (callbacks only).
 */
export type UseImportPnpOptions = AsyncJobOptions;

/**
 * Result of useImportPnp hook.
 */
export type UseImportPnpResult = UseAsyncJobResult<ImportPnpJobRequest, void>;

/**
 * Execute a passthrough (PnP) import operation with polling.
 */
export type UseImportPnpFn = (
  options?: UseImportPnpOptions,
) => UseImportPnpResult;

/**
 * Execute a passthrough (PnP) import operation with polling.
 *
 * @param options - Optional callbacks for progress, completion, and error events.
 * @returns Hook result with status, result, and control functions including startWith.
 */
export const useImportPnp: UseImportPnpFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  const callbacks = useAsyncJobCallbacks(options);

  const buildOptions = useCallback(
    (request: ImportPnpJobRequest) => ({
      kickOff: () =>
        importPnpKickOff(fhirBaseUrl!, {
          exportUrl: request.exportUrl,
          exportType: "dynamic",
          saveMode: request.saveMode,
          inputFormat: request.inputFormat,
          // Bulk export passthrough parameters.
          types: request.types,
          since: request.since,
          until: request.until,
          elements: request.elements,
          typeFilters: request.typeFilters,
          includeAssociatedData: request.includeAssociatedData,
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

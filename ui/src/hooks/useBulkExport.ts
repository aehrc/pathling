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

import { useCallback, useMemo } from "react";

import {
  systemExportKickOff,
  allPatientsExportKickOff,
  patientExportKickOff,
  groupExportKickOff,
  bulkExportStatus,
  bulkExportDownload,
  jobCancel,
} from "../api";
import { config } from "../config";
import { useAsyncJob } from "./useAsyncJob";
import { useAuth } from "../contexts/AuthContext";
import { getExportOutputFiles } from "../types/export";

import type { ResourceType } from "../types/api";
import type {
  UseBulkExportFn,
  BulkExportRequest,
  ExportManifest,
} from "../types/hooks";


interface KickOffResult {
  pollingUrl: string;
}

interface StatusResult {
  status: string;
  manifest?: ExportManifest;
}

/**
 * Execute a bulk export operation with polling.
 *
 * @param options - Optional callbacks for progress, completion, and error events.
 * @returns Hook result with status, result, and control functions including startWith.
 */
export const useBulkExport: UseBulkExportFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  const callbacks = useMemo(
    () => ({
      onProgress: options?.onProgress,
      onComplete: options?.onComplete,
      onError: options?.onError,
    }),
    [options?.onProgress, options?.onComplete, options?.onError],
  );

  const buildOptions = useCallback(
    (request: BulkExportRequest) => ({
      kickOff: async () => {
        const baseOptions = {
          types: request.resourceTypes as ResourceType[] | undefined,
          since: request.since,
          until: request.until,
          elements: request.elements,
          accessToken,
        };

        switch (request.type) {
          case "system":
            return systemExportKickOff(fhirBaseUrl!, baseOptions);
          case "all-patients":
            return allPatientsExportKickOff(fhirBaseUrl!, baseOptions);
          case "patient":
            if (!request.patientId) throw new Error("Patient ID required");
            return patientExportKickOff(fhirBaseUrl!, {
              ...baseOptions,
              patientId: request.patientId,
            });
          case "group":
            if (!request.groupId) throw new Error("Group ID required");
            return groupExportKickOff(fhirBaseUrl!, {
              ...baseOptions,
              groupId: request.groupId,
            });
          default:
            throw new Error(`Unknown export type: ${request.type}`);
        }
      },
      getJobId: (result: KickOffResult) => result.pollingUrl,
      checkStatus: (pollingUrl: string) =>
        bulkExportStatus(fhirBaseUrl!, { pollingUrl, accessToken }),
      isComplete: (status: StatusResult) => status.status === "complete",
      getResult: (status: StatusResult) => status.manifest!,
      cancel: (pollingUrl: string) =>
        jobCancel(fhirBaseUrl!, { pollingUrl, accessToken }),
      pollingInterval: 3000,
    }),
    [fhirBaseUrl, accessToken],
  );

  const job = useAsyncJob<
    BulkExportRequest,
    KickOffResult,
    StatusResult,
    ExportManifest
  >(buildOptions, callbacks);

  const download = useCallback(
    async (fileName: string) => {
      if (!job.result) throw new Error("No export result available");
      const outputFiles = getExportOutputFiles(job.result);
      const file = outputFiles.find((f) => f.url.endsWith(fileName));
      if (!file) throw new Error(`File not found: ${fileName}`);
      return bulkExportDownload(fhirBaseUrl!, {
        fileUrl: file.url,
        accessToken,
      });
    },
    [fhirBaseUrl, accessToken, job.result],
  );

  return { ...job, download };
};

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

import { useCallback } from "react";
import {
  systemExportKickOff,
  allPatientsExportKickOff,
  patientExportKickOff,
  groupExportKickOff,
  bulkExportStatus,
  bulkExportDownload,
} from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useAsyncJob } from "./useAsyncJob";
import type { UseBulkExportFn } from "../types/hooks";
import type { ResourceType } from "../types/api";

/**
 * Execute a bulk export operation with polling.
 */
export const useBulkExport: UseBulkExportFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  const job = useAsyncJob(
    () => ({
      kickOff: async () => {
        const baseOptions = {
          types: options.resourceTypes as ResourceType[] | undefined,
          since: options.since,
          accessToken,
        };

        switch (options.type) {
          case "system":
            return systemExportKickOff(fhirBaseUrl!, baseOptions);
          case "all-patients":
            return allPatientsExportKickOff(fhirBaseUrl!, baseOptions);
          case "patient":
            if (!options.patientId) throw new Error("Patient ID required");
            return patientExportKickOff(fhirBaseUrl!, {
              ...baseOptions,
              patientId: options.patientId,
            });
          case "group":
            if (!options.groupId) throw new Error("Group ID required");
            return groupExportKickOff(fhirBaseUrl!, {
              ...baseOptions,
              groupId: options.groupId,
            });
          default:
            throw new Error(`Unknown export type: ${options.type}`);
        }
      },
      getJobId: (result) => result.pollingUrl,
      checkStatus: (pollingUrl) =>
        bulkExportStatus(fhirBaseUrl!, { pollingUrl, accessToken }),
      isComplete: (status) => status.status === "complete",
      getResult: (status) => status.manifest!,
      cancel: async () => {},
      pollingInterval: 3000,
    }),
    {
      onProgress: options.onProgress,
      onComplete: options.onComplete,
      onError: options.onError,
    },
  );

  const download = useCallback(
    async (fileName: string) => {
      const file = job.result?.output.find((f) => f.url.endsWith(fileName));
      if (!file) throw new Error(`File not found: ${fileName}`);
      return bulkExportDownload(fhirBaseUrl!, { fileUrl: file.url, accessToken });
    },
    [fhirBaseUrl, accessToken, job.result],
  );

  return { ...job, download };
}

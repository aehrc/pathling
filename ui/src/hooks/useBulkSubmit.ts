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
import { bulkSubmit, bulkSubmitStatus, bulkSubmitDownload } from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useAsyncJob } from "./useAsyncJob";
import type { UseBulkSubmitFn } from "../types/hooks";

/**
 * Execute a bulk submit operation with polling.
 */
export const useBulkSubmit: UseBulkSubmitFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  const job = useAsyncJob(
    () => ({
      kickOff: () =>
        bulkSubmit(fhirBaseUrl!, {
          submitter: options.submitter,
          submissionId: options.submissionId,
          submissionStatus: "in-progress",
          manifestUrl: options.manifestUrl,
          fhirBaseUrl: options.fhirBaseUrl,
          replacesManifestUrl: options.replacesManifestUrl,
          oauthMetadataUrl: options.oauthMetadataUrl,
          metadata: options.metadata,
          fileRequestHeaders: options.fileRequestHeaders,
          accessToken,
        }),
      getJobId: (result) => result.submissionId,
      checkStatus: () =>
        bulkSubmitStatus(fhirBaseUrl!, {
          submitter: options.submitter,
          submissionId: options.submissionId,
          accessToken,
        }),
      isComplete: (status) =>
        status.status === "completed" ||
        status.status === "completed-with-errors" ||
        status.status === "aborted",
      getResult: (status) => status.manifest!,
      cancel: async () => {
        // Cancel by submitting with aborted status.
        await bulkSubmit(fhirBaseUrl!, {
          submitter: options.submitter,
          submissionId: options.submissionId,
          submissionStatus: "aborted",
          manifestUrl: options.manifestUrl,
          accessToken,
        });
      },
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
      return bulkSubmitDownload(fhirBaseUrl!, {
        submissionId: options.submissionId,
        fileName,
        accessToken,
      });
    },
    [fhirBaseUrl, accessToken, options.submissionId],
  );

  return { ...job, download };
}

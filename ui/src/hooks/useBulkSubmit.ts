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

import { useCallback, useMemo, useRef } from "react";
import { bulkSubmit, bulkSubmitStatus, bulkSubmitDownload } from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useAsyncJob } from "./useAsyncJob";
import type {
  UseBulkSubmitFn,
  BulkSubmitRequest,
  BulkSubmitManifest,
} from "../types/hooks";

interface KickOffResult {
  submissionId: string;
}

interface StatusResult {
  status: string;
  manifest?: BulkSubmitManifest;
}

/**
 * Execute a bulk submit operation with polling.
 *
 * @param options - Optional callbacks for progress, completion, and error events.
 * @returns Hook result with status, result, and control functions including startWith.
 */
export const useBulkSubmit: UseBulkSubmitFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const submissionIdRef = useRef<string | undefined>(undefined);

  const callbacks = useMemo(
    () => ({
      onProgress: options?.onProgress,
      onComplete: options?.onComplete,
      onError: options?.onError,
    }),
    [options?.onProgress, options?.onComplete, options?.onError],
  );

  const buildOptions = useCallback(
    (request: BulkSubmitRequest) => {
      submissionIdRef.current = request.submissionId;
      return {
        kickOff: () =>
          bulkSubmit(fhirBaseUrl!, {
            submitter: request.submitter,
            submissionId: request.submissionId,
            submissionStatus: "in-progress",
            manifestUrl: request.manifestUrl,
            fhirBaseUrl: request.fhirBaseUrl,
            replacesManifestUrl: request.replacesManifestUrl,
            oauthMetadataUrl: request.oauthMetadataUrl,
            metadata: request.metadata,
            fileRequestHeaders: request.fileRequestHeaders,
            accessToken,
          }),
        getJobId: (result: KickOffResult) => result.submissionId,
        checkStatus: () =>
          bulkSubmitStatus(fhirBaseUrl!, {
            submitter: request.submitter,
            submissionId: request.submissionId,
            accessToken,
          }),
        isComplete: (status: StatusResult) =>
          status.status === "completed" ||
          status.status === "completed-with-errors" ||
          status.status === "aborted",
        getResult: (status: StatusResult) => status.manifest!,
        cancel: async () => {
          // Cancel by submitting with aborted status.
          await bulkSubmit(fhirBaseUrl!, {
            submitter: request.submitter,
            submissionId: request.submissionId,
            submissionStatus: "aborted",
            manifestUrl: request.manifestUrl,
            accessToken,
          });
        },
        pollingInterval: 3000,
      };
    },
    [fhirBaseUrl, accessToken],
  );

  const job = useAsyncJob<
    BulkSubmitRequest,
    KickOffResult,
    StatusResult,
    BulkSubmitManifest
  >(buildOptions, callbacks);

  const download = useCallback(
    async (fileName: string) => {
      if (!submissionIdRef.current)
        throw new Error("No submission ID available");
      return bulkSubmitDownload(fhirBaseUrl!, {
        submissionId: submissionIdRef.current,
        fileName,
        accessToken,
      });
    },
    [fhirBaseUrl, accessToken],
  );

  return { ...job, download };
};

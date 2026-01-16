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

import { useCallback, useMemo, useRef } from "react";

import { bulkSubmit, bulkSubmitStatus, bulkSubmitDownload } from "../api";
import { config } from "../config";
import { useAsyncJob } from "./useAsyncJob";
import { useAuth } from "../contexts/AuthContext";

import type { AsyncJobOptions, UseAsyncJobResult } from "./useAsyncJob";

// ============================================================================
// Bulk Submit Types
// ============================================================================

/**
 * Submitter identifier for bulk submit.
 */
export interface SubmitterIdentifier {
  system: string;
  value: string;
}

/**
 * Base request parameters for bulk submit operations (submit mode).
 */
export interface BulkSubmitRequestBase {
  /** Unique submission ID. */
  submissionId: string;
  /** Submitter identifier. */
  submitter: SubmitterIdentifier;
  /** URL of the manifest file. */
  manifestUrl: string;
  /** Optional FHIR base URL for the source. */
  fhirBaseUrl?: string;
  /** URL of manifest being replaced (for updates). */
  replacesManifestUrl?: string;
  /** OAuth metadata URL for source authentication. */
  oauthMetadataUrl?: string;
  /** Additional metadata key-value pairs. */
  metadata?: Record<string, string>;
  /** Headers to include when fetching files. */
  fileRequestHeaders?: Record<string, string>;
}

/**
 * Request parameters for bulk submit operations (submit mode).
 */
export interface BulkSubmitRequest extends BulkSubmitRequestBase {
  /** Operation mode: submit a new submission. */
  mode: "submit";
}

/**
 * Request parameters for monitoring an existing bulk submit operation.
 */
export interface BulkSubmitMonitorRequest {
  /** Operation mode: monitor an existing submission. */
  mode: "monitor";
  /** Unique submission ID to monitor. */
  submissionId: string;
  /** Submitter identifier. */
  submitter: SubmitterIdentifier;
}

/**
 * Unified request type for bulk submit operations.
 * Use mode: 'submit' to create a new submission.
 * Use mode: 'monitor' to monitor an existing submission.
 */
export type BulkSubmitRequestUnion =
  | BulkSubmitRequest
  | BulkSubmitMonitorRequest;

/**
 * Options for useBulkSubmit hook (callbacks only).
 */
export type UseBulkSubmitOptions = AsyncJobOptions;

/**
 * Bulk submit manifest entry.
 */
export interface BulkSubmitManifestEntry {
  type: string;
  url: string;
  count?: number;
}

/**
 * Complete bulk submit manifest.
 */
export interface BulkSubmitManifest {
  transactionTime: string;
  request: string;
  output: BulkSubmitManifestEntry[];
  error?: BulkSubmitManifestEntry[];
}

/**
 * Result of useBulkSubmit hook.
 */
export interface UseBulkSubmitResult
  extends UseAsyncJobResult<BulkSubmitRequestUnion, BulkSubmitManifest> {
  /** Function to download a file from the manifest. */
  download: (fileName: string) => Promise<ReadableStream>;
}

/**
 * Options for useBulkSubmitMonitor hook (callbacks only).
 */
export type UseBulkSubmitMonitorOptions = AsyncJobOptions;

/**
 * Result of useBulkSubmitMonitor hook.
 */
export interface UseBulkSubmitMonitorResult
  extends UseAsyncJobResult<BulkSubmitMonitorRequest, BulkSubmitManifest> {
  /** Function to download a file from the manifest. */
  download: (fileName: string) => Promise<ReadableStream>;
}

/**
 * Execute a bulk submit operation with polling.
 */
export type UseBulkSubmitFn = (
  options?: UseBulkSubmitOptions,
) => UseBulkSubmitResult;

/**
 * Monitor an existing bulk submit operation with polling.
 * @deprecated Use useBulkSubmit with mode: 'monitor' instead.
 */
export type UseBulkSubmitMonitorFn = UseBulkSubmitFn;

// ============================================================================
// Internal Types
// ============================================================================

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
    (request: BulkSubmitRequestUnion) => {
      submissionIdRef.current = request.submissionId;
      return {
        kickOff:
          request.mode === "monitor"
            ? // Monitor mode: no API call, just return the submission ID to start polling.
              async () => ({ submissionId: request.submissionId })
            : // Submit mode: call the bulkSubmit API.
              () =>
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
          const manifestUrl =
            request.mode === "submit" ? request.manifestUrl : undefined;
          await bulkSubmit(fhirBaseUrl!, {
            submitter: request.submitter,
            submissionId: request.submissionId,
            submissionStatus: "aborted",
            manifestUrl,
            accessToken,
          });
        },
        pollingInterval: 3000,
      };
    },
    [fhirBaseUrl, accessToken],
  );

  const job = useAsyncJob<
    BulkSubmitRequestUnion,
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

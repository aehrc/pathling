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

/**
 * Hook for the asynchronous `$sql-export` operation: kick-off, polling,
 * cancellation and download. One job carries any mixture of subjects, so this
 * hook serves both the single-subject export from a result card and the mixed
 * export set.
 *
 * @author John Grimes
 */

import { useCallback, useRef } from "react";

import {
  jobCancel,
  jobStatus,
  sqlExportDownload,
  sqlExportKickOff,
} from "../api";
import { config } from "../config";
import { useAsyncJob } from "./useAsyncJob";
import { useAsyncJobCallbacks } from "./useAsyncJobCallbacks";
import { useAuth } from "../contexts/AuthContext";

import type { AsyncJobOptions, UseAsyncJobResult } from "./useAsyncJob";
import type { SqlExportEntry, SqlExportFormat } from "../api";
import type { Parameters } from "fhir/r4";

/**
 * An `$sql-export` kick-off request: the subjects to export plus the job-wide
 * output settings and filters.
 */
export interface SqlExportRequest {
  /** The subjects to export, one output each. */
  subjects: SqlExportEntry[];
  /** Output format. */
  format: SqlExportFormat;
  /** Whether CSV output carries a header row. */
  header?: boolean;
  /** Patient ids restricting the data every subject reads. */
  patientIds?: string[];
  /** Group ids restricting the data every subject reads. */
  groupIds?: string[];
  /** Restricts to resources updated at or after this instant. */
  since?: string;
}

/** The completion manifest, a FHIR Parameters resource. */
export type SqlExportManifest = Parameters;

/** Options for {@link useSqlExport} (callbacks only). */
export type UseSqlExportOptions = AsyncJobOptions;

/** Result of {@link useSqlExport}. */
export interface UseSqlExportResult extends UseAsyncJobResult<
  SqlExportRequest,
  SqlExportManifest
> {
  /** Downloads an output file by its manifest location URL. */
  download: (location: string) => Promise<ReadableStream>;
}

/**
 * Runs an `$sql-export` job with polling.
 *
 * @param options - Optional callbacks for progress, completion and error.
 * @returns Hook result with status, manifest, control functions and download.
 */
export function useSqlExport(
  options?: UseSqlExportOptions,
): UseSqlExportResult {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const pollingUrlRef = useRef<string | undefined>(undefined);

  const callbacks = useAsyncJobCallbacks(options);

  const buildOptions = useCallback(
    (request: SqlExportRequest) => ({
      kickOff: () =>
        sqlExportKickOff(fhirBaseUrl!, {
          subjects: request.subjects,
          format: request.format,
          header: request.header,
          patientIds: request.patientIds,
          groupIds: request.groupIds,
          since: request.since,
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
        status.result as SqlExportManifest,
      cancel: (pollingUrl: string) =>
        jobCancel(fhirBaseUrl!, { pollingUrl, accessToken }),
      pollingInterval: 3000,
    }),
    [fhirBaseUrl, accessToken],
  );

  const job = useAsyncJob(buildOptions, callbacks);

  const download = useCallback(
    (location: string) => sqlExportDownload({ location, accessToken }),
    [accessToken],
  );

  return { ...job, download };
}

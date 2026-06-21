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
 * Wrapper component that manages a single SQL query export job lifecycle. Starts the export on
 * mount, reusing the synchronous run's query source, and renders the reusable {@link ExportJobCard}
 * with the current state.
 *
 * @author John Grimes
 */

import { useEffect, useRef } from "react";

import { ExportJobCard } from "./ExportJobCard";
import { parseSqlQueryExportManifest, useDownloadFile, useSqlQueryExport } from "../../hooks";

import type { JobStatus } from "../../types/job";
import type {
  SqlQueryExportFormat,
  SqlQueryExportManifest,
  SqlQueryExportRequest,
  SqlQueryRequest,
} from "../../types/sqlQuery";
import type { Parameters } from "fhir/r4";

interface SqlQueryExportCardWrapperProps {
  /** The synchronous run's query source, reused for the export. */
  source: SqlQueryRequest;
  format: SqlQueryExportFormat;
  createdAt: Date;
  onClose: () => void;
  onError: (message: string) => void;
}

/**
 * Derives the export request from the synchronous run's query source and the chosen format.
 *
 * @param source - The synchronous run's request (stored or inline).
 * @param format - The chosen export format.
 * @returns The export request.
 */
function toExportRequest(
  source: SqlQueryRequest,
  format: SqlQueryExportFormat,
): SqlQueryExportRequest {
  return source.mode === "stored"
    ? { mode: "stored", libraryId: source.libraryId, format, header: true }
    : { mode: "inline", library: source.library, format, header: true };
}

/**
 * Maps the async-job status to the export job card status.
 *
 * @param status - The async-job status.
 * @returns The corresponding job card status.
 */
function toJobStatus(status: string): JobStatus {
  switch (status) {
    case "pending":
    case "in-progress":
      return "in_progress";
    case "complete":
      return "completed";
    case "error":
      return "failed";
    case "cancelled":
      return "cancelled";
    default:
      return "pending";
  }
}

/**
 * Manages a SQL query export job lifecycle, starting it on mount.
 *
 * @param props - Component props.
 * @param props.source - The synchronous run's query source, reused for the export.
 * @param props.format - The output format for the export.
 * @param props.createdAt - The timestamp when the export was created.
 * @param props.onClose - Callback to remove this export card.
 * @param props.onError - Callback for error handling.
 * @returns The rendered export card.
 */
export function SqlQueryExportCardWrapper({
  source,
  format,
  createdAt,
  onClose,
  onError,
}: Readonly<SqlQueryExportCardWrapperProps>) {
  const hasStartedRef = useRef(false);
  const handleDownload = useDownloadFile((err) => onError(err.message));

  const { startWith, cancel, status, result, error, progress } = useSqlQueryExport({
    onError: (err) => onError(err.message),
  });

  // Start the export on mount.
  useEffect(() => {
    if (!hasStartedRef.current) {
      hasStartedRef.current = true;
      startWith(toExportRequest(source, format));
    }
  }, [source, format, startWith]);

  return (
    <ExportJobCard
      job={{
        status: toJobStatus(status),
        progress: progress ?? null,
        error: error ?? null,
        format,
        manifest: (result as SqlQueryExportManifest | null) ?? null,
        createdAt,
      }}
      getOutputs={(manifest: Parameters) => parseSqlQueryExportManifest(manifest)}
      onCancel={cancel}
      onDownload={handleDownload}
      onClose={onClose}
    />
  );
}

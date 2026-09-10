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
 * Wrapper that manages one `$sql-export` job lifecycle, starting it on mount
 * and rendering the shared {@link ExportJobCard} with the current state.
 *
 * One wrapper serves every export flow, because one job carries any mixture
 * of subjects: a single view exported from a result card and a whole export
 * set differ only in how many subjects they name.
 *
 * @author John Grimes
 */

import { useEffect, useRef } from "react";

import { ExportJobCard } from "./ExportJobCard";
import { parseSqlExportManifest } from "../../api";
import { useToast } from "../../contexts/ToastContext";
import { useDownloadFile, useSqlExport } from "../../hooks";

import type { SqlExportEntry } from "../../api";
import type { JobStatus } from "../../types/job";
import type { SqlExportFormat, SqlExportManifest } from "../../types/sqlExport";
import type { Parameters } from "fhir/r4";

interface SqlExportCardWrapperProps {
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
  /** Wall-clock submission time, shown on the card. */
  createdAt: Date;
  /** Removes this card. */
  onClose: () => void;
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
 * Manages a `$sql-export` job lifecycle, starting it on mount.
 *
 * @param props - Component props.
 * @param props.subjects - The subjects to export.
 * @param props.format - The output format.
 * @param props.header - Whether CSV output carries a header row.
 * @param props.patientIds - Patient ids restricting the data read.
 * @param props.groupIds - Group ids restricting the data read.
 * @param props.since - Restricts to resources updated at or after this instant.
 * @param props.createdAt - The timestamp when the export was requested.
 * @param props.onClose - Callback to remove this card.
 * @returns The rendered export card.
 */
export function SqlExportCardWrapper({
  subjects,
  format,
  header = true,
  patientIds,
  groupIds,
  since,
  createdAt,
  onClose,
}: Readonly<SqlExportCardWrapperProps>) {
  const { showToast } = useToast();
  const hasStartedRef = useRef(false);
  // A failed download is the one failure this card cannot display, because the
  // card is describing a job that succeeded, so it is notified instead.
  const handleDownload = useDownloadFile((err) => showToast("Download failed", err.message));

  const { startWith, cancel, status, result, error, progress } = useSqlExport();

  useEffect(() => {
    if (!hasStartedRef.current) {
      hasStartedRef.current = true;
      startWith({ subjects, format, header, patientIds, groupIds, since });
    }
  }, [subjects, format, header, patientIds, groupIds, since, startWith]);

  return (
    <ExportJobCard
      job={{
        status: toJobStatus(status),
        progress: progress ?? null,
        error: error ?? null,
        format,
        manifest: (result as SqlExportManifest | null) ?? null,
        createdAt,
      }}
      getOutputs={(manifest: Parameters) => parseSqlExportManifest(manifest)}
      onCancel={cancel}
      onDownload={handleDownload}
      onClose={onClose}
    />
  );
}

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
 * Wrapper component that manages a single view export job lifecycle.
 * Calls useViewExport internally and starts the export on mount.
 *
 * @author John Grimes
 */

import { useEffect, useRef } from "react";

import { ViewExportCard } from "./ViewExportCard";
import { useDownloadFile, useViewExport } from "../../hooks";

import type { ViewDefinition } from "../../api";
import type { ViewExportOutputFormat } from "../../hooks";
import type { ViewExportJob } from "../../types/job";
import type { ViewExportManifest } from "../../types/viewExport";

interface ViewExportCardWrapperProps {
  id: string;
  viewDefinition: ViewDefinition;
  format: ViewExportOutputFormat;
  createdAt: Date;
  onClose: () => void;
  onError: (message: string) => void;
}

/**
 * Wrapper component that manages a view export job lifecycle.
 * Starts the export on mount and renders ViewExportCard with the current state.
 *
 * @param props - Component props.
 * @param props.id - Unique identifier for this export instance.
 * @param props.viewDefinition - The view definition to export.
 * @param props.format - The output format for the export.
 * @param props.createdAt - The timestamp when the export was created.
 * @param props.onClose - Callback to remove this export card.
 * @param props.onError - Callback for error handling.
 * @returns The rendered export card wrapper component.
 */
export function ViewExportCardWrapper({
  id,
  viewDefinition,
  format,
  createdAt,
  onClose,
  onError,
}: Readonly<ViewExportCardWrapperProps>) {
  const hasStartedRef = useRef(false);
  const handleDownloadError = useDownloadFile((err) => onError(err.message));

  const viewExport = useViewExport({
    onError: (err) => onError(err.message),
  });

  const { startWith, cancel, status, result, error, progress, request } = viewExport;

  // Start the export on mount.
  useEffect(() => {
    if (!hasStartedRef.current) {
      hasStartedRef.current = true;
      startWith({
        views: [{ viewDefinition }],
        format,
        header: true,
      });
    }
  }, [viewDefinition, format, startWith]);

  // Build the export job structure for ViewExportCard.
  const isRunning = status === "pending" || status === "in-progress";
  const exportJob: ViewExportJob = {
    id,
    type: "view-export",
    pollUrl: null,
    status: isRunning
      ? "in_progress"
      : status === "complete"
        ? "completed"
        : status === "error"
          ? "failed"
          : status === "cancelled"
            ? "cancelled"
            : "pending",
    progress: progress ?? null,
    error: error ?? null,
    request: { format: request?.format ?? format },
    manifest: result as ViewExportManifest | null,
    createdAt,
  };

  return (
    <ViewExportCard
      job={exportJob}
      onCancel={cancel}
      onDownload={handleDownloadError}
      onClose={onClose}
    />
  );
}

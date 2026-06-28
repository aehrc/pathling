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
 * Card component displaying a view export job. A thin adapter over the reusable {@link
 * ExportJobCard}, supplying the view export manifest parser.
 *
 * @author John Grimes
 */

import { ExportJobCard } from "./ExportJobCard";
import { getViewExportOutputFiles } from "../../types/viewExport";

import type { ViewExportJob } from "../../types/job";

interface ViewExportCardProps {
  job: ViewExportJob;
  onCancel: () => void;
  onDownload: (url: string, filename: string) => void;
  onClose?: () => void;
  /** Optional callback to delete the export files from the server. */
  onDelete?: () => Promise<void>;
}

/**
 * Displays a view export job's status with progress and download links.
 *
 * @param root0 - The component props.
 * @param root0.job - The view export job to display.
 * @param root0.onCancel - Callback to cancel the export.
 * @param root0.onDownload - Callback to download an output file.
 * @param root0.onClose - Optional callback to close/remove the card.
 * @param root0.onDelete - Optional callback to delete export files from the server.
 * @returns The view export card component.
 */
export function ViewExportCard({
  job,
  onCancel,
  onDownload,
  onClose,
  onDelete,
}: Readonly<ViewExportCardProps>) {
  return (
    <ExportJobCard
      job={{
        status: job.status,
        progress: job.progress,
        error: job.error,
        format: job.request.format,
        manifest: job.manifest,
        createdAt: job.createdAt,
      }}
      getOutputs={getViewExportOutputFiles}
      onCancel={onCancel}
      onDownload={onDownload}
      onClose={onClose}
      onDelete={onDelete}
    />
  );
}

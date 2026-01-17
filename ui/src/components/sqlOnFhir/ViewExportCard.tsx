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
 * Card component displaying a view export job with progress and download links.
 * This is a pure presentational component - all state management happens in the parent.
 *
 * @author John Grimes
 */

import { Cross2Icon, DownloadIcon, ReloadIcon, TrashIcon } from "@radix-ui/react-icons";
import { Badge, Box, Button, Card, Flex, Progress, Text } from "@radix-ui/themes";
import { useState } from "react";

import { getViewExportOutputFiles } from "../../types/viewExport";
import { formatDateTime } from "../../utils";

import type { ViewExportJob } from "../../types/job";
import type { ViewExportFormat } from "../../types/viewExport";

interface ViewExportCardProps {
  job: ViewExportJob;
  onCancel: () => void;
  onDownload: (url: string, filename: string) => void;
  onClose?: () => void;
  /** Optional callback to delete the export files from the server. */
  onDelete?: () => Promise<void>;
}

const STATUS_COLORS: Record<ViewExportJob["status"], "blue" | "green" | "red" | "gray"> = {
  pending: "blue",
  in_progress: "blue",
  completed: "green",
  failed: "red",
  cancelled: "gray",
};

const STATUS_LABELS: Record<ViewExportJob["status"], string> = {
  pending: "Pending",
  in_progress: "Exporting",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
};

const FORMAT_LABELS: Record<ViewExportFormat, string> = {
  ndjson: "NDJSON",
  csv: "CSV",
  parquet: "Parquet",
};

/**
 * Extracts the filename from a result URL's query parameters.
 *
 * @param url - The URL to extract the filename from.
 * @returns The extracted filename or "unknown" if not found.
 */
function getFilenameFromUrl(url: string): string {
  const params = new URLSearchParams(new URL(url).search);
  return params.get("file") ?? "unknown";
}

/**
 * Displays export job status with progress and download links.
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
  const [isDeleting, setIsDeleting] = useState(false);

  const isActive = job.status === "pending" || job.status === "in_progress";
  const showProgress = isActive && job.progress !== null;
  const canDelete = job.status === "completed" && onDelete !== undefined;
  const canClose =
    job.status === "completed" || job.status === "cancelled" || job.status === "failed";

  /**
   * Handles the delete action by calling onDelete to clean up server files,
   * then closing the card.
   */
  async function handleDelete() {
    if (!onDelete) return;
    setIsDeleting(true);
    try {
      await onDelete();
    } finally {
      setIsDeleting(false);
      onClose?.();
    }
  }

  return (
    <Card size="1" mt="3">
      <Flex direction="column" gap="2">
        <Flex justify="between" align="start">
          <Box>
            <Flex align="center" gap="2" mb="1">
              <Text size="2" weight="medium">
                Export to {FORMAT_LABELS[job.request.format]}
              </Text>
              <Badge size="1" color={STATUS_COLORS[job.status]}>
                {STATUS_LABELS[job.status]}
              </Badge>
            </Flex>
            <Text size="1" color="gray">
              {formatDateTime(job.createdAt)}
            </Text>
          </Box>
          {isActive && (
            <Button size="1" variant="ghost" color="red" onClick={onCancel}>
              <Cross2Icon />
              Cancel
            </Button>
          )}
          {canDelete && onClose && (
            <Flex gap="2">
              <Button
                size="1"
                variant="soft"
                color="red"
                onClick={handleDelete}
                disabled={isDeleting}
              >
                <TrashIcon />
                Delete
              </Button>
              <Button size="1" variant="soft" color="gray" onClick={onClose}>
                <Cross2Icon />
                Close
              </Button>
            </Flex>
          )}
          {canClose && !canDelete && onClose && (
            <Button size="1" variant="soft" color="gray" onClick={onClose}>
              <Cross2Icon />
              Close
            </Button>
          )}
        </Flex>

        {showProgress && (
          <Box>
            <Flex justify="between" mb="1">
              <Text size="1" color="gray">
                Progress
              </Text>
              <Text size="1" color="gray">
                {job.progress}%
              </Text>
            </Flex>
            <Progress size="1" value={job.progress ?? 0} />
          </Box>
        )}

        {isActive && !showProgress && (
          <Flex align="center" gap="2">
            <ReloadIcon style={{ animation: "spin 1s linear infinite" }} />
            <Text size="2" color="gray">
              Exporting...
            </Text>
          </Flex>
        )}

        {job.status === "failed" && job.error && (
          <Text size="2" color="red">
            Error: {job.error.message}
          </Text>
        )}

        {job.status === "completed" &&
          job.manifest &&
          (() => {
            const outputs = getViewExportOutputFiles(job.manifest);
            return outputs.length > 0 ? (
              <Box>
                <Text size="2" weight="medium" mb="1">
                  Output files ({outputs.length})
                </Text>
                <Flex direction="column" gap="1">
                  {outputs
                    .slice()
                    .sort((a, b) =>
                      getFilenameFromUrl(a.url).localeCompare(getFilenameFromUrl(b.url)),
                    )
                    .map((output) => (
                      <Flex key={output.url} justify="between" align="center">
                        <Text size="2">{getFilenameFromUrl(output.url)}</Text>
                        <Button
                          size="1"
                          variant="soft"
                          onClick={() => onDownload(output.url, getFilenameFromUrl(output.url))}
                        >
                          <DownloadIcon />
                          Download
                        </Button>
                      </Flex>
                    ))}
                </Flex>
              </Box>
            ) : null;
          })()}
      </Flex>
    </Card>
  );
}

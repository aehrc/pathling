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
 * Reusable, presentational card displaying an asynchronous export job (view export or SQL query
 * export) with progress, cancellation, and download links. All state management happens in the
 * parent; this component is parameterised by a manifest-output parser so it can serve either export
 * operation, both of which share the SQL on FHIR manifest shape and the NDJSON/CSV/Parquet format
 * set.
 *
 * @author John Grimes
 */

import { Cross2Icon, DownloadIcon, ReloadIcon, TrashIcon } from "@radix-ui/react-icons";
import { Badge, Box, Button, Card, Flex, Progress, Text } from "@radix-ui/themes";
import { useState } from "react";

import { OperationOutcomeDisplay } from "../../components/error/OperationOutcomeDisplay";
import { formatDateTime } from "../../utils";

import type { JobStatus } from "../../types/job";
import type { Parameters } from "fhir/r4";

/** The NDJSON/CSV/Parquet format set shared by both export operations. */
export type ExportJobFormat = "ndjson" | "csv" | "parquet";

/** One downloadable output extracted from a completion manifest. */
export interface ExportJobOutput {
  name: string;
  url: string;
}

/** The presentational state of an export job, common to view and SQL query export. */
export interface ExportJobCardData {
  status: JobStatus;
  progress: number | null;
  error: Error | null;
  format: ExportJobFormat;
  manifest: Parameters | null;
  createdAt: Date;
}

interface ExportJobCardProps {
  job: ExportJobCardData;
  /** Parses the completion manifest into the downloadable outputs. */
  getOutputs: (manifest: Parameters) => ExportJobOutput[];
  onCancel: () => void;
  onDownload: (url: string, filename: string) => void;
  onClose?: () => void;
  /** Optional callback to delete the export files from the server. */
  onDelete?: () => Promise<void>;
}

const STATUS_COLORS: Record<JobStatus, "blue" | "green" | "red" | "gray"> = {
  pending: "blue",
  in_progress: "blue",
  completed: "green",
  failed: "red",
  cancelled: "gray",
};

const STATUS_LABELS: Record<JobStatus, string> = {
  pending: "Pending",
  in_progress: "Exporting",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
};

const FORMAT_LABELS: Record<ExportJobFormat, string> = {
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
 * Displays an export job's status with progress and download links.
 *
 * @param root0 - The component props.
 * @param root0.job - The export job state to display.
 * @param root0.getOutputs - Parses the manifest into downloadable outputs.
 * @param root0.onCancel - Callback to cancel the export.
 * @param root0.onDownload - Callback to download an output file.
 * @param root0.onClose - Optional callback to close/remove the card.
 * @param root0.onDelete - Optional callback to delete export files from the server.
 * @returns The export job card component.
 */
export function ExportJobCard({
  job,
  getOutputs,
  onCancel,
  onDownload,
  onClose,
  onDelete,
}: Readonly<ExportJobCardProps>) {
  const [isDeleting, setIsDeleting] = useState(false);

  const isActive = job.status === "pending" || job.status === "in_progress";
  const showProgress = isActive && job.progress !== null;
  const canDelete = job.status === "completed" && onDelete !== undefined;
  const canClose =
    job.status === "completed" || job.status === "cancelled" || job.status === "failed";

  /**
   * Handles the delete action by cleaning up server files, then closing the card.
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
        <Flex justify="between" align="start" gap="1" overflow="hidden" wrap="wrap" gapY="1">
          <Box>
            <Flex align="center" gap="2" mb="1">
              <Text size="2" weight="medium" wrap="nowrap" style={{ textOverflow: "ellipsis" }}>
                Export to {FORMAT_LABELS[job.format]}
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

        {job.status === "failed" && job.error && <OperationOutcomeDisplay error={job.error} />}

        {job.status === "completed" &&
          job.manifest &&
          (() => {
            const outputs = getOutputs(job.manifest);
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
                        <Text
                          size="2"
                          style={{
                            flexShrink: 1,
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            textWrap: "nowrap",
                          }}
                        >
                          {getFilenameFromUrl(output.url)}
                        </Text>
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

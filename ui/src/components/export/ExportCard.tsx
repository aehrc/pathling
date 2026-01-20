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
 * Card component that displays and manages a single bulk export job.
 * Each card manages its own export lifecycle via the useBulkExport hook.
 *
 * @author John Grimes
 */

import { Cross2Icon, DownloadIcon, ReloadIcon, TrashIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, Progress, Text } from "@radix-ui/themes";
import { useEffect, useRef, useState } from "react";

import { OperationOutcomeDisplay } from "../../components/error/OperationOutcomeDisplay";
import { useBulkExport, useDownloadFile } from "../../hooks";
import { getExportOutputFiles } from "../../types/export";
import { formatDateTime } from "../../utils";

import type { BulkExportType } from "../../hooks/useBulkExport";
import type { ExportRequest } from "../../types/export";

interface ExportCardProps {
  request: ExportRequest;
  createdAt: Date;
  onError: (message: string) => void;
  onClose?: () => void;
}

/**
 * Maps export level to hook export type.
 *
 * @param type - The bulk export type.
 * @returns Human-readable label for the export type.
 */
function getExportTypeLabel(type: BulkExportType): string {
  switch (type) {
    case "system":
      return "System export";
    case "all-patients":
      return "All patients export";
    case "patient":
      return "Patient export";
    case "group":
      return "Group export";
  }
}

/**
 * Extracts the filename from a result URL's query parameters.
 *
 * @param url - The URL containing the file query parameter.
 * @returns The filename extracted from the URL.
 */
function getFilenameFromUrl(url: string): string {
  const params = new URLSearchParams(new URL(url).search);
  return params.get("file") ?? "unknown";
}

/**
 * Displays and manages a single bulk export job.
 *
 * @param props - Component props.
 * @param props.request - The export request configuration.
 * @param props.createdAt - The timestamp when the export was created.
 * @param props.onError - Callback for error handling (e.g., auth errors).
 * @param props.onClose - Optional callback to close/remove the card.
 * @returns The rendered export card component.
 */
export function ExportCard({ request, createdAt, onError, onClose }: Readonly<ExportCardProps>) {
  const handleDownloadFile = useDownloadFile((err) => onError(err.message));
  const hasStartedRef = useRef(false);
  const [isDeleting, setIsDeleting] = useState(false);

  const { startWith, cancel, deleteJob, status, result, error, progress } = useBulkExport();

  // Extract output files from the Parameters result.
  const outputFiles = result ? getExportOutputFiles(result) : [];

  // Derive isRunning from status.
  const isRunning = status === "pending" || status === "in-progress";

  // Determine button visibility.
  const canDelete = status === "complete";
  const canClose = status === "complete" || status === "cancelled" || status === "error";

  /**
   * Handles the delete action by calling deleteJob to clean up server files,
   * then closing the card.
   */
  async function handleDelete() {
    setIsDeleting(true);
    try {
      await deleteJob();
    } finally {
      setIsDeleting(false);
      onClose?.();
    }
  }

  // Start the export immediately on mount.
  useEffect(() => {
    if (!hasStartedRef.current) {
      hasStartedRef.current = true;
      startWith({
        type: request.level,
        resourceTypes: request.resourceTypes,
        since: request.since,
        until: request.until,
        elements: request.elements,
        patientId: request.patientId,
        groupId: request.groupId,
        outputFormat: request.outputFormat,
      });
    }
  }, [request, startWith]);

  return (
    <Card>
      <Flex direction="column" gap="3">
        <Flex justify="between" align="start">
          <Box>
            <Text weight="medium" as="div">
              {getExportTypeLabel(request.level)}
            </Text>
            {request.resourceTypes && request.resourceTypes.length > 0 && (
              <Text size="1" color="gray" as="div" mb="1">
                Types: {request.resourceTypes.join(", ")}
              </Text>
            )}
            <Text size="1" color="gray" as="div" mb="1">
              {formatDateTime(createdAt)}
            </Text>
          </Box>
          {isRunning && (
            <Button size="1" variant="soft" color="red" onClick={cancel}>
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

        {isRunning && progress !== undefined && (
          <Box>
            <Flex justify="between" mb="1">
              <Text size="1" color="gray">
                Progress
              </Text>
              <Text size="1" color="gray">
                {progress}%
              </Text>
            </Flex>
            <Progress value={progress} />
          </Box>
        )}

        {isRunning && progress === undefined && (
          <Flex align="center" gap="2">
            <ReloadIcon style={{ animation: "spin 1s linear infinite" }} />
            <Text size="2" color="gray">
              Processing...
            </Text>
          </Flex>
        )}

        {error && <OperationOutcomeDisplay error={error} />}

        {status === "cancelled" && (
          <Text size="2" color="gray">
            Cancelled
          </Text>
        )}

        {outputFiles.length > 0 && (
          <Box>
            <Text size="2" weight="medium" mb="2" as="div">
              Output files ({outputFiles.length})
            </Text>
            <Flex direction="column" gap="1">
              {outputFiles
                .slice()
                .sort((a, b) => getFilenameFromUrl(a.url).localeCompare(getFilenameFromUrl(b.url)))
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
                      {output.count !== undefined && (
                        <Text color="gray"> ({output.count} resources)</Text>
                      )}
                    </Text>
                    <Button
                      size="1"
                      variant="soft"
                      onClick={() => handleDownloadFile(output.url, getFilenameFromUrl(output.url))}
                    >
                      <DownloadIcon />
                      Download
                    </Button>
                  </Flex>
                ))}
            </Flex>
          </Box>
        )}
      </Flex>
    </Card>
  );
}

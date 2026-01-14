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
 * Card component that displays and manages a single import job.
 * Each card manages its own import lifecycle via the appropriate hook.
 *
 * @author John Grimes
 */

import { Cross2Icon, ReloadIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, Progress, Text } from "@radix-ui/themes";
import { useEffect, useRef } from "react";

import { useImport, useImportPnp } from "../../hooks";
import { formatDateTime } from "../../utils";

import type { ImportJob, ImportRequest } from "../../types/import";
import type { ImportPnpRequest } from "../../types/importPnp";

interface ImportCardProps {
  job: ImportJob;
  onError: (message: string) => void;
  onClose?: () => void;
}

/**
 * Gets the label for an import job based on its type.
 *
 * @param type - The import job type.
 * @returns Human-readable label for the import type.
 */
function getImportTypeLabel(type: ImportJob["type"]): string {
  switch (type) {
    case "standard":
      return "Import from URLs";
    case "pnp":
      return "Import from FHIR server";
  }
}

/**
 * Gets the subtitle describing what is being imported.
 *
 * @param job - The import job.
 * @returns Subtitle text describing the import sources.
 */
function getImportSubtitle(job: ImportJob): string {
  if (job.type === "standard") {
    const request = job.request as ImportRequest;
    return `Importing ${request.input.length} source(s)`;
  } else {
    const request = job.request as ImportPnpRequest;
    return `Importing from ${request.exportUrl}`;
  }
}

/**
 * Displays and manages a single import job.
 *
 * @param props - Component props.
 * @param props.job - The import job configuration.
 * @param props.onError - Callback for error handling (e.g., auth errors).
 * @param props.onClose - Optional callback to close/remove the card.
 * @returns The rendered import card component.
 */
export function ImportCard({ job, onError, onClose }: Readonly<ImportCardProps>) {
  const hasStartedRef = useRef(false);

  // Use the appropriate hook based on job type.
  const standardImport = useImport();
  const pnpImport = useImportPnp();

  // Select the correct hook result based on job type.
  const importJob = job.type === "standard" ? standardImport : pnpImport;
  const { cancel, status, error, progress } = importJob;

  // Derive isRunning from status.
  const isRunning = status === "pending" || status === "in-progress";

  // Determine if the close button should be shown.
  const canClose = status === "complete" || status === "cancelled" || status === "error";

  // Start the import immediately on mount.
  useEffect(() => {
    if (!hasStartedRef.current) {
      hasStartedRef.current = true;
      if (job.type === "standard") {
        const request = job.request as ImportRequest;
        standardImport.startWith({
          sources: request.input.map((i) => i.url),
          resourceTypes: request.input.map((i) => i.type),
          saveMode: request.saveMode,
          inputFormat: request.inputFormat,
        });
      } else {
        const request = job.request as ImportPnpRequest;
        pnpImport.startWith({
          exportUrl: request.exportUrl,
          saveMode: request.saveMode,
          inputFormat: request.inputFormat,
        });
      }
    }
  }, [job, standardImport, pnpImport]);

  // Report errors to parent.
  useEffect(() => {
    if (error) {
      onError(error.message);
    }
  }, [error, onError]);

  return (
    <Card>
      <Flex direction="column" gap="3">
        <Flex justify="between" align="start">
          <Box>
            <Text weight="medium" as="div" mb="1">
              {getImportTypeLabel(job.type)}
            </Text>
            <Text size="1" color="gray" as="div" mb="1">
              {getImportSubtitle(job)}
            </Text>
            <Text size="1" color="gray" as="div" mb="1">
              {formatDateTime(job.createdAt)}
            </Text>
          </Box>
          {isRunning && (
            <Button size="1" variant="soft" color="red" onClick={cancel}>
              <Cross2Icon />
              Cancel
            </Button>
          )}
          {canClose && onClose && (
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

        {error && (
          <Text size="2" color="red">
            Error: {error.message}
          </Text>
        )}

        {status === "cancelled" && (
          <Text size="2" color="gray">
            Cancelled
          </Text>
        )}

        {status === "complete" && !error && (
          <Text size="2" color="green">
            Import completed successfully
          </Text>
        )}
      </Flex>
    </Card>
  );
}

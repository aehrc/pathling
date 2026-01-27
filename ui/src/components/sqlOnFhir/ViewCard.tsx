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
 * Card component that displays and manages a single view query job.
 * Each card manages its own query lifecycle via the useViewRun hook.
 * Supports multiple concurrent exports, each displayed in its own card.
 *
 * @author John Grimes
 */

import { Cross2Icon } from "@radix-ui/react-icons";
import { Badge, Box, Button, Card, Code, Flex, Spinner, Table, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useState } from "react";

import { ExportControls } from "./ExportControls";
import { ViewExportCardWrapper } from "./ViewExportCardWrapper";
import { read } from "../../api";
import { config } from "../../config";
import { useAuth } from "../../contexts/AuthContext";
import { useViewRun } from "../../hooks";
import { formatDateTime } from "../../utils";

import type { ViewDefinition } from "../../api";
import type { ViewExportOutputFormat } from "../../hooks";
import type { ViewJob } from "../../types/viewJob";

interface ViewCardProps {
  job: ViewJob;
  onError: (message: string) => void;
  onClose?: () => void;
}

/**
 * Represents a single export instance within a ViewCard.
 */
interface ViewExportInstance {
  id: string;
  format: ViewExportOutputFormat;
  createdAt: Date;
}

/**
 * Gets the label for a view job based on its mode.
 *
 * @param mode - The view job mode.
 * @returns Human-readable label for the view mode.
 */
function getModeLabel(mode: ViewJob["mode"]): string {
  switch (mode) {
    case "stored":
      return "Run stored view definition";
    case "inline":
      return "Run provided view definition";
  }
}

/**
 * Formats a cell value for display.
 *
 * @param value - The value to format.
 * @returns The formatted string representation.
 */
function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

/**
 * Displays and manages a single view query job.
 *
 * @param props - Component props.
 * @param props.job - The view job configuration.
 * @param props.onError - Callback for error handling (e.g., auth errors).
 * @param props.onClose - Optional callback to close/remove the card.
 * @returns The rendered view card component.
 */
export function ViewCard({ job, onError, onClose }: Readonly<ViewCardProps>) {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  // Use the view run hook for query execution.
  const viewRun = useViewRun();
  const { execute, status, result, error } = viewRun;

  // Track multiple exports within this card.
  const [exports, setExports] = useState<ViewExportInstance[]>([]);

  // Cache the resolved view definition to avoid refetching for each export.
  const [cachedViewDefinition, setCachedViewDefinition] = useState<ViewDefinition | null>(null);

  // Derive states.
  const isRunning = status === "pending";
  const isComplete = status === "success";
  const isError = status === "error";

  // Determine if the close button should be shown.
  const canClose = isComplete || isError;

  // Start the query when component mounts with idle status.
  // Using status === "idle" instead of a ref ensures the mutation starts on the
  // actual mount rather than the first of React 18 Strict Mode's double renders.
  useEffect(() => {
    if (status === "idle") {
      execute({
        mode: job.mode,
        viewDefinitionId: job.viewDefinitionId,
        viewDefinitionJson: job.viewDefinitionJson,
        limit: job.limit ?? 10,
      });
    }
  }, [status, job, execute]);

  // Report errors to parent.
  useEffect(() => {
    if (error) {
      onError(error.message);
    }
  }, [error, onError]);

  // Handle export by creating a new export instance.
  const handleExport = useCallback(
    async (format: ViewExportOutputFormat) => {
      if (!fhirBaseUrl) return;

      // Get or build the view definition (cache for subsequent exports).
      let viewDefinition = cachedViewDefinition;
      if (!viewDefinition) {
        if (job.mode === "stored" && job.viewDefinitionId) {
          // Fetch the stored view definition.
          const resource = await read(fhirBaseUrl, {
            resourceType: "ViewDefinition",
            id: job.viewDefinitionId,
            accessToken,
          });
          viewDefinition = resource as ViewDefinition;
        } else if (job.mode === "inline" && job.viewDefinitionJson) {
          viewDefinition = JSON.parse(job.viewDefinitionJson);
        } else {
          throw new Error("No view definition available");
        }
        setCachedViewDefinition(viewDefinition);
      }

      // Create a new export instance and prepend it (most recent first).
      const newExport: ViewExportInstance = {
        id: crypto.randomUUID(),
        format,
        createdAt: new Date(),
      };
      setExports((prev) => [newExport, ...prev]);
    },
    [job, fhirBaseUrl, accessToken, cachedViewDefinition],
  );

  // Handle closing an individual export.
  const handleCloseExport = useCallback((exportId: string) => {
    setExports((prev) => prev.filter((e) => e.id !== exportId));
  }, []);

  return (
    <Card>
      <Flex direction="column" gap="3">
        <Flex justify="between" align="start">
          <Box>
            <Text weight="medium" as="div" mb="1">
              {getModeLabel(job.mode)}
            </Text>
            <Text size="1" color="gray" as="div" mb="1">
              Job ID: {job.id}
            </Text>
            {job.viewDefinitionId ? (
              <Text size="1" color="gray" as="div" mb="1">
                View definition ID: {job.viewDefinitionId}
              </Text>
            ) : null}
            <Text size="1" color="gray" as="div" mb="1">
              {formatDateTime(job.createdAt)}
            </Text>
          </Box>
          {canClose && onClose && (
            <Button size="1" variant="soft" color="gray" onClick={onClose}>
              <Cross2Icon />
              Close
            </Button>
          )}
        </Flex>

        {isRunning && (
          <Flex align="center" gap="2">
            <Spinner size="1" />
            <Text size="2" color="gray">
              Executing view definition...
            </Text>
          </Flex>
        )}

        {error && (
          <Text size="2" color="red">
            View run failed: {error.message}
          </Text>
        )}

        {isComplete && result && result.rows.length === 0 && (
          <Text size="2" color="gray">
            No rows returned.
          </Text>
        )}

        {isComplete && result && result.rows.length > 0 && (
          <>
            <Flex align="center" justify="between">
              <Badge color="gray">{result.rows.length} rows (first 10)</Badge>
              <ExportControls onExport={handleExport} disabled={false} />
            </Flex>
            <Box style={{ width: "100%", overflowX: "auto" }}>
              <Table.Root size="1">
                <Table.Header>
                  <Table.Row>
                    {result.columns.map((column) => (
                      <Table.ColumnHeaderCell key={column} style={{ whiteSpace: "nowrap" }}>
                        <Text weight="medium" size="1">
                          {column}
                        </Text>
                      </Table.ColumnHeaderCell>
                    ))}
                  </Table.Row>
                </Table.Header>
                <Table.Body>
                  {result.rows.map((row, rowIndex) => (
                    // eslint-disable-next-line @eslint-react/no-array-index-key -- Query result rows have no stable identifier.
                    <Table.Row key={rowIndex}>
                      {result.columns.map((column) => (
                        <Table.Cell key={column} style={{ whiteSpace: "nowrap" }}>
                          <Code size="1" title={formatCellValue(row[column])}>
                            {formatCellValue(row[column])}
                          </Code>
                        </Table.Cell>
                      ))}
                    </Table.Row>
                  ))}
                </Table.Body>
              </Table.Root>
            </Box>

            {cachedViewDefinition &&
              exports.map((exportInstance) => (
                <ViewExportCardWrapper
                  key={exportInstance.id}
                  id={exportInstance.id}
                  viewDefinition={cachedViewDefinition}
                  format={exportInstance.format}
                  createdAt={exportInstance.createdAt}
                  onClose={() => handleCloseExport(exportInstance.id)}
                  onError={onError}
                />
              ))}
          </>
        )}
      </Flex>
    </Card>
  );
}

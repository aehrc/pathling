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
 * Each card manages its own query lifecycle via the useSqlRun hook.
 * Supports multiple concurrent exports, each displayed in its own card.
 *
 * @author John Grimes
 */

import { Cross2Icon } from "@radix-ui/react-icons";
import { Badge, Box, Button, Card, Code, Flex, Spinner, Table, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useState } from "react";

import { ExportControls } from "./ExportControls";
import { SqlExportCardWrapper } from "./SqlExportCardWrapper";
import { useSqlRun } from "../../hooks";
import { formatDateTime } from "../../utils";
import { ErrorCallout } from "../error/ErrorCallout";
import { toDisplayIssues } from "../error/errorPresentation";

import type { SubjectSource } from "../../api";
import type { SqlExportFormat } from "../../types/sqlExport";
import type { ViewJob } from "../../types/viewJob";

interface ViewCardProps {
  job: ViewJob;
  onClose?: () => void;
}

/**
 * Represents a single export instance within a ViewCard.
 */
interface ViewExportInstance {
  id: string;
  format: SqlExportFormat;
  createdAt: Date;
}

/**
 * Derives the wire subject form for a view job: a stored view is named by a
 * typed reference, an inline one is sent whole.
 *
 * @param job - The view job.
 * @returns The subject source to send.
 * @throws {Error} When the job names no view definition at all.
 */
function toSubjectSource(job: ViewJob): SubjectSource {
  if (job.mode === "stored" && job.viewDefinitionId) {
    return {
      kind: "reference",
      reference: `ViewDefinition/${job.viewDefinitionId}`,
    };
  }
  if (job.mode === "inline" && job.viewDefinitionJson) {
    return { kind: "resource", resource: JSON.parse(job.viewDefinitionJson) };
  }
  throw new Error("Invalid request: missing view definition ID or JSON");
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
 * @param props.onClose - Optional callback to close/remove the card.
 * @returns The rendered view card component.
 */
export function ViewCard({ job, onClose }: Readonly<ViewCardProps>) {
  const { execute, status, result, error } = useSqlRun();

  // Track multiple exports within this card.
  const [exports, setExports] = useState<ViewExportInstance[]>([]);

  // A view is always run in a tabular format, so a binary result cannot arise.
  const tabular = result?.kind === "tabular" ? result : undefined;

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
      execute({ subject: toSubjectSource(job), limit: job.limit ?? 10 });
    }
  }, [status, job, execute]);

  // Handle export by creating a new export instance.
  const handleExport = useCallback((format: SqlExportFormat) => {
    setExports((prev) => [{ id: crypto.randomUUID(), format, createdAt: new Date() }, ...prev]);
  }, []);

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

        {error && <ErrorCallout issues={toDisplayIssues(error)} size="1" />}

        {isComplete && tabular && tabular.rows.length === 0 && (
          <Text size="2" color="gray">
            No rows returned.
          </Text>
        )}

        {isComplete && tabular && tabular.rows.length > 0 && (
          <>
            <Flex align="center" justify="between">
              <Badge color="gray">{tabular.rows.length} rows (first 10)</Badge>
              <ExportControls onExport={handleExport} disabled={false} />
            </Flex>
            <Box style={{ width: "100%", overflowX: "auto" }}>
              <Table.Root size="1">
                <Table.Header>
                  <Table.Row>
                    {tabular.columns.map((column) => (
                      <Table.ColumnHeaderCell key={column} style={{ whiteSpace: "nowrap" }}>
                        <Text weight="medium" size="1">
                          {column}
                        </Text>
                      </Table.ColumnHeaderCell>
                    ))}
                  </Table.Row>
                </Table.Header>
                <Table.Body>
                  {tabular.rows.map((row, rowIndex) => (
                    // eslint-disable-next-line @eslint-react/no-array-index-key -- Query result rows have no stable identifier.
                    <Table.Row key={rowIndex}>
                      {tabular.columns.map((column) => (
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

            {exports.map((exportInstance) => (
              <SqlExportCardWrapper
                key={exportInstance.id}
                subjects={[{ subject: toSubjectSource(job) }]}
                format={exportInstance.format}
                createdAt={exportInstance.createdAt}
                onClose={() => handleCloseExport(exportInstance.id)}
              />
            ))}
          </>
        )}
      </Flex>
    </Card>
  );
}

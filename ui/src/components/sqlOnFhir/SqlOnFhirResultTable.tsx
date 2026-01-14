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
 * Component for displaying ViewDefinition execution results in a table.
 *
 * @author John Grimes
 */

import { ExclamationTriangleIcon, TableIcon } from "@radix-ui/react-icons";
import { Badge, Box, Callout, Code, Flex, Heading, Spinner, Table, Text } from "@radix-ui/themes";

import { ExportControls } from "./ExportControls";
import { ViewExportCard } from "./ViewExportCard";

import type { ViewExportJob } from "../../types/job";
import type { ViewExportFormat } from "../../types/viewExport";

interface SqlOnFhirResultTableProps {
  rows: Record<string, unknown>[] | undefined;
  columns: string[] | undefined;
  isLoading: boolean;
  error: Error | null;
  hasExecuted: boolean;
  onExport?: (format: ViewExportFormat) => void;
  exportJob?: ViewExportJob | null;
  onDownload?: (url: string, filename: string) => void;
  onCancelExport?: () => void;
  isExporting?: boolean;
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
 * Displays ViewDefinition execution results in a table format.
 *
 * @param root0 - The component props.
 * @param root0.rows - Array of result rows.
 * @param root0.columns - Column names for the table header.
 * @param root0.isLoading - Whether execution is in progress.
 * @param root0.error - Error from failed execution, if any.
 * @param root0.hasExecuted - Whether execution has been attempted.
 * @param root0.onExport - Callback when export is triggered.
 * @param root0.exportJob - Current export job, if any.
 * @param root0.onDownload - Callback to download export file.
 * @param root0.onCancelExport - Callback to cancel export.
 * @param root0.isExporting - Whether export is in progress.
 * @returns The SQL on FHIR result table component.
 */
export function SqlOnFhirResultTable({
  rows,
  columns,
  isLoading,
  error,
  hasExecuted,
  onExport,
  exportJob,
  onDownload,
  onCancelExport,
  isExporting,
}: SqlOnFhirResultTableProps) {
  // Initial state before any execution.
  if (!hasExecuted) {
    return (
      <Box>
        <Heading size="4" mb="4">
          Results
        </Heading>
        <Flex align="center" justify="center" py="8" direction="column" gap="2">
          <TableIcon width={32} height={32} color="var(--gray-8)" />
          <Text color="gray">Execute a view definition to view results.</Text>
        </Flex>
      </Box>
    );
  }

  // Loading state.
  if (isLoading) {
    return (
      <Box>
        <Heading size="4" mb="4">
          Results
        </Heading>
        <Flex align="center" gap="2" py="4">
          <Spinner />
          <Text>Executing view definition...</Text>
        </Flex>
      </Box>
    );
  }

  // Error state.
  if (error) {
    return (
      <Box>
        <Heading size="4" mb="4">
          Results
        </Heading>
        <Callout.Root color="red">
          <Callout.Icon>
            <ExclamationTriangleIcon />
          </Callout.Icon>
          <Callout.Text>{error.message}</Callout.Text>
        </Callout.Root>
      </Box>
    );
  }

  // Empty results.
  if (!rows || rows.length === 0) {
    return (
      <Box>
        <Heading size="4" mb="4">
          Results
        </Heading>
        <Flex align="center" justify="center" py="8" direction="column" gap="2">
          <Text color="gray">No rows returned.</Text>
        </Flex>
      </Box>
    );
  }

  // Determine if export is available (only when there's no active job).
  const canExport =
    onExport &&
    !isExporting &&
    (!exportJob ||
      exportJob.status === "completed" ||
      exportJob.status === "failed" ||
      exportJob.status === "cancelled");

  // Results table.
  return (
    <Box>
      <Flex align="center" justify="between" mb="4">
        <Flex align="center" gap="2">
          <Heading size="4">Results</Heading>
          <Badge color="gray">{rows.length} rows (first 10)</Badge>
        </Flex>
        {onExport && <ExportControls onExport={onExport} disabled={!canExport} />}
      </Flex>
      <Box style={{ width: "100%" }}>
        <Table.Root size="1">
          <Table.Header>
            <Table.Row>
              {columns?.map((column) => (
                <Table.ColumnHeaderCell key={column} style={{ whiteSpace: "nowrap" }}>
                  <Text weight="medium" size="1">
                    {column}
                  </Text>
                </Table.ColumnHeaderCell>
              ))}
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {rows.map((row, rowIndex) => (
              // eslint-disable-next-line @eslint-react/no-array-index-key -- Query result rows have no stable identifier.
              <Table.Row key={rowIndex}>
                {columns?.map((column) => (
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
      {exportJob && onDownload && onCancelExport && (
        <ViewExportCard job={exportJob} onCancel={onCancelExport} onDownload={onDownload} />
      )}
    </Box>
  );
}

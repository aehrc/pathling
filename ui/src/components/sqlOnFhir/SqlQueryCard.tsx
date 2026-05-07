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
 * Result card for a single SQL query execution.
 *
 * Mirrors the lifecycle pattern of `ViewCard`: each card mounts, kicks off
 * its own `$sqlquery-run` request via `useSqlQueryRun`, and renders the
 * format-appropriate result body when complete.
 *
 * @author John Grimes
 */

import { Cross2Icon, ExclamationTriangleIcon } from "@radix-ui/react-icons";
import {
  Badge,
  Box,
  Button,
  Callout,
  Card,
  Code,
  Flex,
  Spinner,
  Table,
  Text,
} from "@radix-ui/themes";
import { useEffect } from "react";

import { useSqlQueryRun } from "../../hooks";
import { OperationOutcomeError } from "../../types/errors";
import { formatDateTime } from "../../utils";

import type { SqlQueryJob, SqlQueryResult } from "../../types/sqlQuery";

interface SqlQueryCardProps {
  /** The SQL query job describing the request. */
  job: SqlQueryJob;
  /** Callback for surfacing errors to the parent (e.g. for global auth handling). */
  onError: (message: string) => void;
  /** Optional callback to remove the card once it has terminated. */
  onClose?: () => void;
}

/**
 * Renders a SQL query result card.
 *
 * @param props - The component props.
 * @param props.job - The SQL query job describing the request.
 * @param props.onError - Callback for surfacing errors to the parent.
 * @param props.onClose - Optional callback to remove the card once it has terminated.
 * @returns The card.
 */
export function SqlQueryCard({ job, onError, onClose }: Readonly<SqlQueryCardProps>) {
  const { execute, status, result, error } = useSqlQueryRun();

  const isRunning = status === "pending";
  const isComplete = status === "success";
  const isError = status === "error";
  const canClose = isComplete || isError;

  // Mount-time execution: kick off the request once when the card lands
  // in idle state. Using the status as the trigger plays nicely with React
  // Strict Mode's double-render of the mount.
  useEffect(() => {
    if (status === "idle") {
      execute(job.request);
    }
  }, [status, execute, job]);

  // Surface errors to the parent for global handling.
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
              SQL query
            </Text>
            <Text size="1" color="gray" as="div" mb="1">
              Job ID: {job.id}
            </Text>
            <Text size="1" color="gray" as="div" mb="1">
              Mode: {job.mode === "stored" ? "stored library" : "inline library"}
            </Text>
            <Text size="1" color="gray" as="div" mb="1">
              {formatDateTime(job.createdAt)}
            </Text>
          </Box>
          <Flex align="center" gap="2">
            {canClose && onClose && (
              <Button
                size="1"
                variant="soft"
                color="gray"
                onClick={onClose}
                aria-label="Close result"
              >
                <Cross2Icon />
                Close
              </Button>
            )}
          </Flex>
        </Flex>

        {isRunning && (
          <Flex align="center" gap="2">
            <Spinner size="1" />
            <Text size="2" color="gray">
              Executing SQL query...
            </Text>
          </Flex>
        )}

        {isError && error && <SqlQueryErrorBody sql={job.sql} error={error} />}

        {isComplete && result && <SqlQueryResultBody result={result} sql={job.sql} />}
      </Flex>
    </Card>
  );
}

interface SqlQueryResultBodyProps {
  result: SqlQueryResult;
  sql: string;
}

/**
 * Renders the body for a successful SQL query response, branching on the
 * result kind.
 *
 * @param props - The component props.
 * @param props.result - The successful execution result.
 * @param props.sql - The submitted SQL text, displayed beneath the table.
 * @returns The result body.
 */
function SqlQueryResultBody({ result, sql }: Readonly<SqlQueryResultBodyProps>) {
  if (result.kind === "binary") {
    return (
      <Text size="2" color="gray">
        Binary results cannot be previewed. Download will be supported by a future SQL query export
        operation.
      </Text>
    );
  }

  if (result.rows.length === 0) {
    return (
      <Text size="2" color="gray">
        No rows returned.
      </Text>
    );
  }

  const previewRows = result.rows.slice(0, 10);

  return (
    <Flex direction="column" gap="3">
      <Flex align="center" justify="between">
        <Badge color="gray">{previewRows.length} rows</Badge>
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
            {previewRows.map((row, rowIndex) => (
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
      <Text size="1" color="gray">
        Submitted SQL:
      </Text>
      <Box
        style={{
          fontFamily: "monospace",
          fontSize: "0.85em",
          background: "var(--gray-2)",
          padding: "0.5rem",
          borderRadius: "4px",
          whiteSpace: "pre-wrap",
        }}
      >
        {sql}
      </Box>
    </Flex>
  );
}

interface SqlQueryErrorBodyProps {
  sql: string;
  error: Error;
}

/**
 * Renders the error body, including the submitted SQL above the callout.
 *
 * @param props - The component props.
 * @param props.sql - The submitted SQL text to render above the callout.
 * @param props.error - The error returned by the request.
 * @returns The error body.
 */
function SqlQueryErrorBody({ sql, error }: Readonly<SqlQueryErrorBodyProps>) {
  const message =
    error instanceof OperationOutcomeError ? extractOutcomeText(error) : error.message;
  return (
    <Flex direction="column" gap="2">
      <Text size="1" color="gray">
        Submitted SQL:
      </Text>
      <Box
        style={{
          fontFamily: "monospace",
          fontSize: "0.85em",
          background: "var(--gray-2)",
          padding: "0.5rem",
          borderRadius: "4px",
          whiteSpace: "pre-wrap",
        }}
      >
        {sql || "(empty)"}
      </Box>
      <Callout.Root color="red" size="1">
        <Callout.Icon>
          <ExclamationTriangleIcon />
        </Callout.Icon>
        <Callout.Text>{message}</Callout.Text>
      </Callout.Root>
    </Flex>
  );
}

/**
 * Extracts the most useful display text from an OperationOutcome.
 *
 * @param error - The OperationOutcome error to render.
 * @returns The first available diagnostic or details text.
 */
function extractOutcomeText(error: OperationOutcomeError): string {
  const issue = error.operationOutcome.issue?.[0];
  return issue?.diagnostics ?? issue?.details?.text ?? "Server returned an error.";
}

/**
 * Formats a cell value for display.
 *
 * @param value - The cell value to render.
 * @returns A string suitable for display in a table cell.
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

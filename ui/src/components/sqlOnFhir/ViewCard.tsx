/**
 * Card component that displays and manages a single view query job.
 * Each card manages its own query lifecycle via the useViewRun hook.
 *
 * @author John Grimes
 */

import { Cross2Icon } from "@radix-ui/react-icons";
import { Badge, Box, Button, Card, Code, Flex, Spinner, Table, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useState } from "react";

import { ExportControls } from "./ExportControls";
import { ViewExportCard } from "./ViewExportCard";
import { read } from "../../api";
import { config } from "../../config";
import { useAuth } from "../../contexts/AuthContext";
import { useDownloadFile, useViewExport, useViewRun } from "../../hooks";
import { formatDateTime } from "../../utils";

import type { ViewDefinition, ViewExportOutputFormat } from "../../types/hooks";
import type { ViewExportJob } from "../../types/job";
import type { ViewExportManifest } from "../../types/viewExport";
import type { ViewJob } from "../../types/viewJob";

interface ViewCardProps {
  job: ViewJob;
  onError: (message: string) => void;
  onClose?: () => void;
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

  // Use the view export hook for exporting results.
  const viewExport = useViewExport({
    onError: (err) => onError(err.message),
  });

  // Track download errors.
  const [downloadError, setDownloadError] = useState<Error | null>(null);
  const handleDownload = useDownloadFile(setDownloadError);

  // Derive states.
  const isRunning = status === "pending";
  const isComplete = status === "success";
  const isError = status === "error";
  const isExportRunning = viewExport.status === "pending" || viewExport.status === "in-progress";

  // Determine if the close button should be shown.
  const canClose = (isComplete || isError) && !isExportRunning;

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

  // Handle export.
  const handleExport = useCallback(
    async (format: ViewExportOutputFormat) => {
      if (!fhirBaseUrl) return;

      // Get or build the view definition.
      let viewDefinition: ViewDefinition;
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

      viewExport.startWith({
        views: [{ viewDefinition }],
        format,
        header: true,
      });
    },
    [job, fhirBaseUrl, accessToken, viewExport],
  );

  const handleCancelExport = () => void viewExport.cancel();

  // Build export job structure for the ViewExportCard.
  const exportJob: ViewExportJob | null =
    isExportRunning || viewExport.result
      ? {
          id: `export-${job.id}`,
          type: "view-export" as const,
          pollUrl: null,
          status: isExportRunning ? ("in_progress" as const) : ("completed" as const),
          progress: viewExport.progress ?? null,
          error: viewExport.error ?? null,
          request: { format: viewExport.request?.format ?? "ndjson" },
          manifest: viewExport.result as ViewExportManifest | null,
          createdAt: job.createdAt,
        }
      : null;

  // Determine if export is available.
  const canExport =
    isComplete &&
    !isExportRunning &&
    (!exportJob ||
      exportJob.status === "completed" ||
      exportJob.status === "failed" ||
      exportJob.status === "cancelled");

  // Combine errors for display.
  const displayError = error ?? downloadError;

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

        {displayError && (
          <Text size="2" color="red">
            View run failed: {displayError.message}
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
              <ExportControls onExport={handleExport} disabled={!canExport} />
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

            {exportJob && (
              <ViewExportCard
                job={exportJob}
                onCancel={handleCancelExport}
                onDownload={handleDownload}
              />
            )}
          </>
        )}
      </Flex>
    </Card>
  );
}

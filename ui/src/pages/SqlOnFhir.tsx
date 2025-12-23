/**
 * Page for executing SQL on FHIR ViewDefinitions.
 *
 * @author John Grimes
 */

import { Box, Flex, Spinner, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useState } from "react";
import { viewRun, viewRunStored, read } from "../api";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { SqlOnFhirForm } from "../components/sqlOnFhir/SqlOnFhirForm";
import { SqlOnFhirResultTable } from "../components/sqlOnFhir/SqlOnFhirResultTable";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import {
  useSaveViewDefinition,
  useServerCapabilities,
  useUnauthorizedHandler,
  useViewExport,
} from "../hooks";
import { UnauthorizedError } from "../types/errors";
import type {
  ViewDefinitionExecuteRequest,
  ViewDefinitionResult,
} from "../types/sqlOnFhir";
import type { ViewDefinition, ViewExportOutputFormat } from "../types/hooks";
import type { ViewExportManifest } from "../types/viewExport";

/**
 * Parses an NDJSON response into an array of objects.
 */
function parseNdjsonResponse(ndjson: string): Record<string, unknown>[] {
  return ndjson
    .split("\n")
    .filter((line) => line.trim() !== "")
    .map((line) => JSON.parse(line));
}

/**
 * Extracts column names from the first row of results.
 */
function extractColumns(rows: Record<string, unknown>[]): string[] {
  if (rows.length === 0) return [];
  return Object.keys(rows[0]);
}

/**
 * Reads a stream to completion and returns the text content.
 */
async function streamToText(stream: ReadableStream): Promise<string> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }

  const decoder = new TextDecoder();
  return chunks.map((chunk) => decoder.decode(chunk, { stream: true })).join("") +
    decoder.decode();
}

export function SqlOnFhir() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const handleUnauthorizedError = useUnauthorizedHandler();

  const [executionResult, setExecutionResult] = useState<ViewDefinitionResult | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [executionError, setExecutionError] = useState<Error | null>(null);
  const [hasExecuted, setHasExecuted] = useState(false);
  const [lastExecutedRequest, setLastExecutedRequest] =
    useState<ViewDefinitionExecuteRequest | null>(null);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // View export hook.
  const viewExport = useViewExport({
    onError: (errorMessage) => {
      // Check if it's an unauthorized error by message.
      if (
        errorMessage.includes("401") ||
        errorMessage.toLowerCase().includes("unauthorized")
      ) {
        handleUnauthorizedError();
      } else {
        setExecutionError(new Error(errorMessage));
      }
    },
  });

  // Derive isRunning from status.
  const isExportRunning =
    viewExport.status === "pending" || viewExport.status === "in-progress";

  const handleExecute = useCallback(
    async (request: ViewDefinitionExecuteRequest) => {
      if (!fhirBaseUrl) return;

      setIsExecuting(true);
      setExecutionError(null);
      setHasExecuted(true);
      viewExport.reset();

      try {
        let stream: ReadableStream;

        if (request.mode === "stored" && request.viewDefinitionId) {
          stream = await viewRunStored(fhirBaseUrl, {
            viewDefinitionId: request.viewDefinitionId,
            limit: 10,
            accessToken,
          });
        } else if (request.mode === "inline" && request.viewDefinitionJson) {
          const parsed = JSON.parse(request.viewDefinitionJson);
          stream = await viewRun(fhirBaseUrl, {
            viewDefinition: parsed,
            limit: 10,
            accessToken,
          });
        } else {
          throw new Error("Invalid request: missing view definition ID or JSON");
        }

        const ndjsonText = await streamToText(stream);
        const rows = parseNdjsonResponse(ndjsonText);
        const columns = extractColumns(rows);

        setExecutionResult({ columns, rows });
        setLastExecutedRequest(request);
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        } else {
          setExecutionError(err instanceof Error ? err : new Error("Execution failed"));
        }
      } finally {
        setIsExecuting(false);
      }
    },
    [fhirBaseUrl, accessToken, handleUnauthorizedError, viewExport],
  );

  // Mutation for saving a ViewDefinition to the server.
  const {
    mutateAsync: saveViewDefinition,
    isPending: isSaving,
    error: saveError,
  } = useSaveViewDefinition();

  // Handle unauthorized errors from save mutation.
  useEffect(() => {
    if (saveError instanceof UnauthorizedError) {
      handleUnauthorizedError();
    }
  }, [saveError, handleUnauthorizedError]);

  const handleExport = useCallback(
    async (format: ViewExportOutputFormat) => {
      if (!lastExecutedRequest || !fhirBaseUrl) return;

      // Get or build the view definition.
      let viewDefinition: ViewDefinition;
      if (
        lastExecutedRequest.mode === "stored" &&
        lastExecutedRequest.viewDefinitionId
      ) {
        // Fetch the stored view definition.
        const resource = await read(fhirBaseUrl, {
          resourceType: "ViewDefinition",
          id: lastExecutedRequest.viewDefinitionId,
          accessToken,
        });
        viewDefinition = resource as ViewDefinition;
      } else if (
        lastExecutedRequest.mode === "inline" &&
        lastExecutedRequest.viewDefinitionJson
      ) {
        viewDefinition = JSON.parse(lastExecutedRequest.viewDefinitionJson);
      } else {
        throw new Error("No view definition available");
      }

      viewExport.startWith({
        views: [{ viewDefinition }],
        format,
        header: true,
      });
    },
    [lastExecutedRequest, fhirBaseUrl, accessToken, viewExport],
  );

  const handleDownload = useCallback(
    async (url: string, filename: string) => {
      try {
        const headers: HeadersInit = {};
        if (accessToken) {
          headers.Authorization = `Bearer ${accessToken}`;
        }

        const response = await fetch(url, { headers });

        if (response.status === 401) {
          handleUnauthorizedError();
          return;
        }

        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = downloadUrl;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(downloadUrl);
      } catch (err) {
        setExecutionError(err instanceof Error ? err : new Error("Download failed"));
      }
    },
    [accessToken, handleUnauthorizedError],
  );

  const handleCancelExport = () => {
    viewExport.cancel();
  };

  // Build export job structure for the result table.
  const exportJob =
    isExportRunning || viewExport.result
      ? {
          id: "current-export",
          type: "view-export" as const,
          pollUrl: null,
          status: isExportRunning
            ? ("in_progress" as const)
            : ("completed" as const),
          progress: viewExport.progress ?? null,
          error: viewExport.error ?? null,
          request: { format: viewExport.request?.format ?? "ndjson" },
          manifest: viewExport.result as ViewExportManifest | null,
          createdAt: new Date(),
        }
      : null;

  // Show loading state while checking server capabilities.
  if (isLoadingCapabilities) {
    return (
      <>
        <Flex align="center" gap="2">
          <Spinner />
          <Text>Checking server capabilities...</Text>
        </Flex>
        <SessionExpiredDialog />
      </>
    );
  }

  // Show login prompt if authentication is required but not authenticated.
  if (capabilities?.authRequired && !isAuthenticated) {
    return <LoginRequired />;
  }

  // Determine actual error to display (ignore unauthorized since it's handled separately).
  const displayError =
    executionError && !(executionError instanceof UnauthorizedError) ? executionError : null;

  return (
    <>
      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Box style={{ flex: 1 }}>
          <SqlOnFhirForm
            onExecute={handleExecute}
            onSaveToServer={saveViewDefinition}
            isExecuting={isExecuting}
            isSaving={isSaving}
            disabled={false}
          />
        </Box>

        <Box style={{ flex: 1, overflowX: "auto" }}>
          <SqlOnFhirResultTable
            rows={executionResult?.rows}
            columns={executionResult?.columns}
            isLoading={isExecuting}
            error={displayError}
            hasExecuted={hasExecuted}
            onExport={handleExport}
            exportJob={exportJob}
            onDownload={handleDownload}
            onCancelExport={handleCancelExport}
            isExporting={isExportRunning}
          />
        </Box>
      </Flex>
      <SessionExpiredDialog />
    </>
  );
}

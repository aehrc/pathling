/**
 * Page for executing SQL on FHIR ViewDefinitions.
 *
 * @author John Grimes
 */

import { Box, Flex, Spinner, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useState } from "react";
import { read } from "../api";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { SqlOnFhirForm } from "../components/sqlOnFhir/SqlOnFhirForm";
import { SqlOnFhirResultTable } from "../components/sqlOnFhir/SqlOnFhirResultTable";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import {
  useDownloadFile,
  useSaveViewDefinition,
  useServerCapabilities,
  useUnauthorizedHandler,
  useViewExport,
  useViewRun,
} from "../hooks";
import { UnauthorizedError } from "../types/errors";
import type { ViewDefinition, ViewExportOutputFormat, ViewRunRequest } from "../types/hooks";
import type { ViewExportManifest } from "../types/viewExport";

export function SqlOnFhir() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const handleUnauthorizedError = useUnauthorizedHandler();

  const [hasExecuted, setHasExecuted] = useState(false);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // View run hook for executing ViewDefinitions.
  const viewRun = useViewRun({
    onError: (error) => {
      if (error instanceof UnauthorizedError) {
        handleUnauthorizedError();
      }
    },
  });

  // Track download errors separately since they're not from viewRun.
  const [downloadError, setDownloadError] = useState<Error | null>(null);
  const handleDownload = useDownloadFile((message) =>
    setDownloadError(new Error(message)),
  );

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
        setDownloadError(new Error(errorMessage));
      }
    },
  });

  // Derive isRunning from status.
  const isExportRunning =
    viewExport.status === "pending" || viewExport.status === "in-progress";

  const handleExecute = useCallback(
    (request: ViewRunRequest) => {
      setHasExecuted(true);
      setDownloadError(null);
      viewExport.reset();
      viewRun.execute(request);
    },
    [viewExport, viewRun],
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
      if (!viewRun.lastRequest || !fhirBaseUrl) return;

      // Get or build the view definition.
      let viewDefinition: ViewDefinition;
      if (
        viewRun.lastRequest.mode === "stored" &&
        viewRun.lastRequest.viewDefinitionId
      ) {
        // Fetch the stored view definition.
        const resource = await read(fhirBaseUrl, {
          resourceType: "ViewDefinition",
          id: viewRun.lastRequest.viewDefinitionId,
          accessToken,
        });
        viewDefinition = resource as ViewDefinition;
      } else if (
        viewRun.lastRequest.mode === "inline" &&
        viewRun.lastRequest.viewDefinitionJson
      ) {
        viewDefinition = JSON.parse(viewRun.lastRequest.viewDefinitionJson);
      } else {
        throw new Error("No view definition available");
      }

      viewExport.startWith({
        views: [{ viewDefinition }],
        format,
        header: true,
      });
    },
    [viewRun.lastRequest, fhirBaseUrl, accessToken, viewExport],
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

  // Determine actual error to display (combine viewRun and download errors, ignore unauthorized).
  const executionError = viewRun.error instanceof UnauthorizedError ? null : viewRun.error;
  const displayError = executionError ?? downloadError;

  return (
    <>
      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Box style={{ flex: 1 }}>
          <SqlOnFhirForm
            onExecute={handleExecute}
            onSaveToServer={saveViewDefinition}
            isExecuting={viewRun.isPending}
            isSaving={isSaving}
            disabled={false}
          />
        </Box>

        <Box style={{ flex: 1, overflowX: "auto" }}>
          <SqlOnFhirResultTable
            rows={viewRun.result?.rows}
            columns={viewRun.result?.columns}
            isLoading={viewRun.isPending}
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

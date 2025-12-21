/**
 * Page for executing SQL on FHIR ViewDefinitions.
 *
 * @author John Grimes
 */

import { Box, Flex, Spinner, Text } from "@radix-ui/themes";
import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { SqlOnFhirForm } from "../components/sqlOnFhir/SqlOnFhirForm";
import { SqlOnFhirResultTable } from "../components/sqlOnFhir/SqlOnFhirResultTable";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useJobs } from "../contexts/JobContext";
import { useServerCapabilities } from "../hooks/useServerCapabilities";
import {
  cancelViewExportJob,
  createViewDefinition,
  executeInlineViewDefinition,
  executeStoredViewDefinition,
  kickOffViewExport,
} from "../services/sqlOnFhir";
import { UnauthorizedError } from "../types/errors";
import type { ViewExportJob } from "../types/job";
import type {
  CreateViewDefinitionResult,
  ViewDefinitionExecuteRequest,
  ViewDefinitionResult,
} from "../types/sqlOnFhir";
import type { ViewExportFormat } from "../types/viewExport";

export function SqlOnFhir() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, client, clearSessionAndPromptLogin } = useAuth();
  const queryClient = useQueryClient();
  const { addJob, updateJobStatus, getJob } = useJobs();

  const [executionResult, setExecutionResult] = useState<ViewDefinitionResult | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [executionError, setExecutionError] = useState<Error | null>(null);
  const [hasExecuted, setHasExecuted] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isExporting, setIsExporting] = useState(false);
  const [exportJobId, setExportJobId] = useState<string | null>(null);
  const [lastExecutedRequest, setLastExecutedRequest] = useState<ViewDefinitionExecuteRequest | null>(null);
  const unauthorizedHandledRef = useRef(false);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // Handle 401 errors by clearing session and prompting for re-authentication.
  const handleUnauthorizedError = useCallback(() => {
    if (unauthorizedHandledRef.current) return;
    unauthorizedHandledRef.current = true;
    clearSessionAndPromptLogin();
  }, [clearSessionAndPromptLogin]);

  // Reset the unauthorized flag when user becomes authenticated.
  useEffect(() => {
    if (isAuthenticated) {
      unauthorizedHandledRef.current = false;
    }
  }, [isAuthenticated]);

  const handleExecute = useCallback(
    async (request: ViewDefinitionExecuteRequest) => {
      setIsExecuting(true);
      setExecutionError(null);
      setHasExecuted(true);
      // Clear any previous export job when executing a new view.
      setExportJobId(null);

      try {
        const accessToken = client?.state.tokenResponse?.access_token;
        let result: ViewDefinitionResult;

        if (request.mode === "stored" && request.viewDefinitionId) {
          result = await executeStoredViewDefinition(
            fhirBaseUrl,
            accessToken,
            request.viewDefinitionId,
          );
        } else if (request.mode === "inline" && request.viewDefinitionJson) {
          result = await executeInlineViewDefinition(
            fhirBaseUrl,
            accessToken,
            request.viewDefinitionJson,
          );
        } else {
          throw new Error("Invalid request: missing view definition ID or JSON");
        }

        setExecutionResult(result);
        // Store the request so we can export it later.
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
    [client, fhirBaseUrl, handleUnauthorizedError],
  );

  const handleSaveToServer = useCallback(
    async (json: string): Promise<CreateViewDefinitionResult> => {
      setIsSaving(true);
      try {
        const accessToken = client?.state.tokenResponse?.access_token;
        const result = await createViewDefinition(fhirBaseUrl, accessToken, json);
        // Invalidate the ViewDefinitions cache to refresh the list.
        await queryClient.invalidateQueries({ queryKey: ["viewDefinitions"] });
        return result;
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        }
        throw err;
      } finally {
        setIsSaving(false);
      }
    },
    [client, fhirBaseUrl, handleUnauthorizedError, queryClient],
  );

  const handleExport = useCallback(
    async (format: ViewExportFormat) => {
      if (!lastExecutedRequest) return;

      setIsExporting(true);
      try {
        const accessToken = client?.state.tokenResponse?.access_token;
        const request = {
          viewDefinitionId: lastExecutedRequest.mode === "stored" ? lastExecutedRequest.viewDefinitionId : undefined,
          viewDefinitionJson: lastExecutedRequest.mode === "inline" ? lastExecutedRequest.viewDefinitionJson : undefined,
          format,
        };

        const { jobId, pollUrl } = await kickOffViewExport(fhirBaseUrl, accessToken, request);

        // Add the job to the context.
        addJob({
          id: jobId,
          type: "view-export",
          pollUrl,
          status: "pending",
          progress: null,
          error: null,
          request,
          manifest: null,
        });

        setExportJobId(jobId);
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        } else {
          // Display error in the result table area by setting execution error.
          setExecutionError(err instanceof Error ? err : new Error("Export failed"));
        }
      } finally {
        setIsExporting(false);
      }
    },
    [client, fhirBaseUrl, lastExecutedRequest, addJob, handleUnauthorizedError],
  );

  const handleDownload = useCallback(
    async (url: string, filename: string) => {
      try {
        const accessToken = client?.state.tokenResponse?.access_token;
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
    [client, handleUnauthorizedError],
  );

  const handleCancelExport = useCallback(async () => {
    if (!exportJobId) return;

    const job = getJob(exportJobId) as ViewExportJob | undefined;
    if (!job?.pollUrl) return;

    try {
      const accessToken = client?.state.tokenResponse?.access_token;
      await cancelViewExportJob(fhirBaseUrl, accessToken, job.pollUrl);
      updateJobStatus(exportJobId, "cancelled");
    } catch (err) {
      if (err instanceof UnauthorizedError) {
        handleUnauthorizedError();
      } else {
        setExecutionError(err instanceof Error ? err : new Error("Cancel failed"));
      }
    }
  }, [exportJobId, getJob, client, fhirBaseUrl, updateJobStatus, handleUnauthorizedError]);

  // Get the current export job from context.
  const currentExportJob = exportJobId ? (getJob(exportJobId) as ViewExportJob | undefined) : null;

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
            onSaveToServer={handleSaveToServer}
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
            exportJob={currentExportJob}
            onDownload={handleDownload}
            onCancelExport={handleCancelExport}
            isExporting={isExporting}
          />
        </Box>
      </Flex>
      <SessionExpiredDialog />
    </>
  );
}

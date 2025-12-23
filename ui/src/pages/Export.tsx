/**
 * Page for managing bulk export operations.
 *
 * @author John Grimes
 */

import { Box, Button, Card, Flex, Progress, Spinner, Text } from "@radix-ui/themes";
import { Cross2Icon, DownloadIcon, ReloadIcon } from "@radix-ui/react-icons";
import { useCallback, useEffect, useState } from "react";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { ExportForm } from "../components/export/ExportForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useBulkExport, useServerCapabilities, useUnauthorizedHandler } from "../hooks";
import type { ExportRequest } from "../types/export";

type ExportType = "system" | "all-patients" | "patient" | "group";

/**
 * Maps form export level to hook export type.
 */
function mapExportLevel(level: ExportRequest["level"]): ExportType {
  switch (level) {
    case "system":
      return "system";
    case "patient-type":
      return "all-patients";
    case "patient-instance":
      return "patient";
    case "group":
      return "group";
  }
}

/**
 * Extracts the filename from a result URL's query parameters.
 */
function getFilenameFromUrl(url: string): string {
  const params = new URLSearchParams(new URL(url).search);
  return params.get("file") ?? "unknown";
}

export function Export() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, client, setError } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const handleUnauthorizedError = useUnauthorizedHandler();

  // Track the current export request and state.
  const [exportRequest, setExportRequest] = useState<ExportRequest | null>(null);
  const [progress, setProgress] = useState<string | undefined>(undefined);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  const handleComplete = useCallback(() => {
    setProgress(undefined);
  }, []);

  const handleError = useCallback(
    (errorMessage: string) => {
      setProgress(undefined);
      // Hook checks if message looks like 401 and handles it.
      handleUnauthorizedError(errorMessage);
      // Set error for non-401 errors.
      if (!errorMessage.includes("401") && !errorMessage.toLowerCase().includes("unauthorized")) {
        setError(errorMessage);
      }
    },
    [handleUnauthorizedError, setError],
  );

  const handleProgress = useCallback((progressValue: number) => {
    setProgress(`${progressValue}%`);
  }, []);

  // Use the bulk export hook.
  const { start, cancel, status, result, error } = useBulkExport({
    type: exportRequest ? mapExportLevel(exportRequest.level) : "system",
    resourceTypes: exportRequest?.resourceTypes,
    since: exportRequest?.since,
    patientId: exportRequest?.patientId,
    groupId: exportRequest?.groupId,
    onProgress: handleProgress,
    onComplete: handleComplete,
    onError: handleError,
  });

  // Derive isRunning from status.
  const isRunning = status === "pending" || status === "in-progress";

  const handleExport = useCallback(
    async (request: ExportRequest) => {
      setExportRequest(request);
      // Start will be triggered in effect when request changes.
    },
    [],
  );

  // Start export when request changes.
  useEffect(() => {
    if (exportRequest && !isRunning && !result) {
      start();
    }
  }, [exportRequest, isRunning, result, start]);

  const handleCancel = useCallback(() => {
    cancel();
    setProgress(undefined);
    setExportRequest(null);
  }, [cancel]);

  const handleDownloadFile = useCallback(
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

        if (!response.ok) {
          throw new Error(`Download failed: ${response.status}`);
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
        setError(err instanceof Error ? err.message : "Download failed");
      }
    },
    [accessToken, setError, handleUnauthorizedError],
  );

  const handleNewExport = useCallback(() => {
    setExportRequest(null);
  }, []);

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

  // Parse progress percentage.
  const progressValue = progress ? parseInt(progress.replace("%", ""), 10) : undefined;

  // Show export form or job status.
  return (
    <>
      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Box style={{ flex: 1 }}>
          <ExportForm
            onSubmit={handleExport}
            isSubmitting={isRunning}
            disabled={isRunning || !!result}
            resourceTypes={capabilities?.resourceTypes ?? []}
          />
        </Box>

        <Box style={{ flex: 1 }}>
          {(isRunning || result || error) && (
            <Card>
              <Flex direction="column" gap="3">
                <Flex justify="between" align="start">
                  <Box>
                    <Text weight="medium">
                      {exportRequest?.level === "system" && "System Export"}
                      {exportRequest?.level === "patient-type" && "Patient Type Export"}
                      {exportRequest?.level === "patient-instance" && "Patient Instance Export"}
                      {exportRequest?.level === "group" && "Group Export"}
                    </Text>
                    {exportRequest?.resourceTypes && exportRequest.resourceTypes.length > 0 && (
                      <Text size="1" color="gray" as="div">
                        Types: {exportRequest.resourceTypes.join(", ")}
                      </Text>
                    )}
                  </Box>
                  {isRunning && (
                    <Button size="1" variant="soft" color="red" onClick={handleCancel}>
                      <Cross2Icon />
                      Cancel
                    </Button>
                  )}
                </Flex>

                {isRunning && progressValue !== undefined && (
                  <Box>
                    <Flex justify="between" mb="1">
                      <Text size="1" color="gray">
                        Progress
                      </Text>
                      <Text size="1" color="gray">
                        {progressValue}%
                      </Text>
                    </Flex>
                    <Progress value={progressValue} />
                  </Box>
                )}

                {isRunning && progressValue === undefined && (
                  <Flex align="center" gap="2">
                    <ReloadIcon style={{ animation: "spin 1s linear infinite" }} />
                    <Text size="2" color="gray">
                      Processing...
                    </Text>
                  </Flex>
                )}

                {error && (
                  <Text size="2" color="red">
                    Error: {error}
                  </Text>
                )}

                {result?.output && (
                  <Box>
                    <Text size="2" weight="medium" mb="2">
                      Output files ({result.output.length})
                    </Text>
                    <Flex direction="column" gap="1">
                      {result.output.map((output, index) => (
                        <Flex key={index} justify="between" align="center">
                          <Text size="2">
                            {getFilenameFromUrl(output.url)}
                            {output.count !== undefined && (
                              <Text color="gray"> ({output.count} resources)</Text>
                            )}
                          </Text>
                          <Button
                            size="1"
                            variant="soft"
                            onClick={() =>
                              handleDownloadFile(output.url, getFilenameFromUrl(output.url))
                            }
                          >
                            <DownloadIcon />
                            Download
                          </Button>
                        </Flex>
                      ))}
                    </Flex>
                    <Flex justify="end" mt="3">
                      <Button variant="soft" onClick={handleNewExport}>
                        New Export
                      </Button>
                    </Flex>
                  </Box>
                )}
              </Flex>
            </Card>
          )}
        </Box>
      </Flex>
      <SessionExpiredDialog />
    </>
  );
}

/**
 * Page for managing bulk export operations.
 *
 * @author John Grimes
 */

import { Cross2Icon, DownloadIcon, ReloadIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, Progress, Spinner, Text } from "@radix-ui/themes";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { ExportForm } from "../components/export/ExportForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useBulkExport, useDownloadFile, useServerCapabilities } from "../hooks";
import type { ExportRequest } from "../types/export";
import type { BulkExportType } from "../types/hooks";

/**
 * Maps hook export type to display label.
 */
function getExportTypeLabel(type: BulkExportType): string {
  switch (type) {
    case "system":
      return "System Export";
    case "all-patients":
      return "Patient Type Export";
    case "patient":
      return "Patient Instance Export";
    case "group":
      return "Group Export";
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
  const { isAuthenticated, setError } = useAuth();
  const handleDownloadFile = useDownloadFile((err) => setError(err.message));

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // Use the bulk export hook. 401 errors are handled globally.
  const { startWith, reset, cancel, status, result, error, progress, request } = useBulkExport();

  // Derive isRunning from status.
  const isRunning = status === "pending" || status === "in-progress";

  const handleExport = (formRequest: ExportRequest) => {
    startWith({
      type: formRequest.level,
      resourceTypes: formRequest.resourceTypes,
      since: formRequest.since,
      patientId: formRequest.patientId,
      groupId: formRequest.groupId,
    });
  };

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
                    <Text weight="medium">{request?.type && getExportTypeLabel(request.type)}</Text>
                    {request?.resourceTypes && request.resourceTypes.length > 0 && (
                      <Text size="1" color="gray" as="div">
                        Types: {request.resourceTypes.join(", ")}
                      </Text>
                    )}
                  </Box>
                  {isRunning && (
                    <Button size="1" variant="soft" color="red" onClick={cancel}>
                      <Cross2Icon />
                      Cancel
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

                {result?.output && (
                  <Box>
                    <Text size="2" weight="medium" mb="2">
                      Output files ({result.output.length})
                    </Text>
                    <Flex direction="column" gap="1">
                      {result.output.map((output) => (
                        <Flex key={output.url} justify="between" align="center">
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
                      <Button variant="soft" onClick={reset}>
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

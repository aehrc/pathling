/**
 * Page for managing import operations.
 *
 * @author John Grimes
 */

import { Box, Button, Card, Flex, Progress, Spinner, Tabs, Text } from "@radix-ui/themes";
import { Cross2Icon, ReloadIcon } from "@radix-ui/react-icons";
import { useCallback } from "react";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { ImportForm } from "../components/import/ImportForm";
import { ImportPnpForm } from "../components/import/ImportPnpForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useImport, useImportPnp, useServerCapabilities, useUnauthorizedHandler } from "../hooks";
import type { ImportRequest } from "../types/import";
import type { ImportPnpRequest } from "../types/importPnp";

export function Import() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, setError } = useAuth();
  const handleUnauthorizedError = useUnauthorizedHandler();

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  const handleError = useCallback(
    (errorMessage: string) => {
      // Hook checks if message looks like 401 and handles it.
      handleUnauthorizedError(errorMessage);
      // Set error for non-401 errors.
      if (!errorMessage.includes("401") && !errorMessage.toLowerCase().includes("unauthorized")) {
        setError(errorMessage);
      }
    },
    [handleUnauthorizedError, setError],
  );

  // Use the import hooks.
  const standardImport = useImport({ onError: handleError });
  const pnpImport = useImportPnp({ onError: handleError });

  const handleImport = useCallback(
    (request: ImportRequest) => {
      standardImport.startWith({
        sources: request.input.map((i) => i.url),
        resourceTypes: request.input.map((i) => i.type),
      });
    },
    [standardImport],
  );

  const handleImportPnp = useCallback(
    (request: ImportPnpRequest) => {
      pnpImport.startWith({
        exportUrl: request.exportUrl,
      });
    },
    [pnpImport],
  );

  // Derive running states from status.
  const isStandardRunning =
    standardImport.status === "pending" || standardImport.status === "in-progress";
  const isPnpRunning = pnpImport.status === "pending" || pnpImport.status === "in-progress";
  const isRunning = isStandardRunning || isPnpRunning;

  // Derive completion state from status.
  const isStandardComplete = standardImport.status === "complete";
  const isPnpComplete = pnpImport.status === "complete";

  const handleCancelStandard = useCallback(() => {
    standardImport.cancel();
  }, [standardImport]);

  const handleCancelPnp = useCallback(() => {
    pnpImport.cancel();
  }, [pnpImport]);

  const handleNewStandardImport = useCallback(() => {
    standardImport.reset();
  }, [standardImport]);

  const handleNewPnpImport = useCallback(() => {
    pnpImport.reset();
  }, [pnpImport]);

  // Determine if we should show the status card for each import type.
  const showStandardStatus =
    isStandardRunning || isStandardComplete || standardImport.error;
  const showPnpStatus = isPnpRunning || isPnpComplete || pnpImport.error;

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

  // Show import form (either auth not required or user is authenticated).
  return (
    <>
      <Tabs.Root defaultValue="urls">
        <Tabs.List>
          <Tabs.Trigger value="urls" disabled={isRunning}>Import from URLs</Tabs.Trigger>
          <Tabs.Trigger value="fhir-server" disabled={isRunning}>Import from FHIR server</Tabs.Trigger>
        </Tabs.List>

        <Box pt="4">
          <Tabs.Content value="urls">
            <Flex gap="6" direction={{ initial: "column", md: "row" }}>
              <Box style={{ flex: 1 }}>
                <ImportForm
                  onSubmit={handleImport}
                  isSubmitting={isStandardRunning}
                  disabled={isRunning}
                  resourceTypes={capabilities?.resourceTypes ?? []}
                />
              </Box>
              <Box style={{ flex: 1 }}>
                {showStandardStatus && (
                  <Card>
                    <Flex direction="column" gap="3">
                      <Flex justify="between" align="start">
                        <Box>
                          <Text weight="medium">Standard Import</Text>
                          {standardImport.request && (
                            <Text size="1" color="gray" as="div">
                              Importing {standardImport.request.sources.length} source(s)
                            </Text>
                          )}
                        </Box>
                        {isStandardRunning && (
                          <Button size="1" variant="soft" color="red" onClick={handleCancelStandard}>
                            <Cross2Icon />
                            Cancel
                          </Button>
                        )}
                      </Flex>

                      {isStandardRunning && standardImport.progress !== undefined && (
                        <Box>
                          <Flex justify="between" mb="1">
                            <Text size="1" color="gray">Progress</Text>
                            <Text size="1" color="gray">{standardImport.progress}%</Text>
                          </Flex>
                          <Progress value={standardImport.progress} />
                        </Box>
                      )}

                      {isStandardRunning && standardImport.progress === undefined && (
                        <Flex align="center" gap="2">
                          <ReloadIcon style={{ animation: "spin 1s linear infinite" }} />
                          <Text size="2" color="gray">Processing...</Text>
                        </Flex>
                      )}

                      {standardImport.error && (
                        <Text size="2" color="red">Error: {standardImport.error}</Text>
                      )}

                      {isStandardComplete && !standardImport.error && (
                        <Box>
                          <Text size="2" color="green" mb="2">Import completed successfully</Text>
                          <Flex justify="end">
                            <Button variant="soft" onClick={handleNewStandardImport}>
                              New Import
                            </Button>
                          </Flex>
                        </Box>
                      )}
                    </Flex>
                  </Card>
                )}
              </Box>
            </Flex>
          </Tabs.Content>

          <Tabs.Content value="fhir-server">
            <Flex gap="6" direction={{ initial: "column", md: "row" }}>
              <Box style={{ flex: 1 }}>
                <ImportPnpForm
                  onSubmit={handleImportPnp}
                  isSubmitting={isPnpRunning}
                  disabled={isRunning}
                />
              </Box>
              <Box style={{ flex: 1 }}>
                {showPnpStatus && (
                  <Card>
                    <Flex direction="column" gap="3">
                      <Flex justify="between" align="start">
                        <Box>
                          <Text weight="medium">FHIR Server Import</Text>
                          {pnpImport.request && (
                            <Text size="1" color="gray" as="div">
                              Importing from {pnpImport.request.exportUrl}
                            </Text>
                          )}
                        </Box>
                        {isPnpRunning && (
                          <Button size="1" variant="soft" color="red" onClick={handleCancelPnp}>
                            <Cross2Icon />
                            Cancel
                          </Button>
                        )}
                      </Flex>

                      {isPnpRunning && pnpImport.progress !== undefined && (
                        <Box>
                          <Flex justify="between" mb="1">
                            <Text size="1" color="gray">Progress</Text>
                            <Text size="1" color="gray">{pnpImport.progress}%</Text>
                          </Flex>
                          <Progress value={pnpImport.progress} />
                        </Box>
                      )}

                      {isPnpRunning && pnpImport.progress === undefined && (
                        <Flex align="center" gap="2">
                          <ReloadIcon style={{ animation: "spin 1s linear infinite" }} />
                          <Text size="2" color="gray">Processing...</Text>
                        </Flex>
                      )}

                      {pnpImport.error && (
                        <Text size="2" color="red">Error: {pnpImport.error}</Text>
                      )}

                      {isPnpComplete && !pnpImport.error && (
                        <Box>
                          <Text size="2" color="green" mb="2">Import completed successfully</Text>
                          <Flex justify="end">
                            <Button variant="soft" onClick={handleNewPnpImport}>
                              New Import
                            </Button>
                          </Flex>
                        </Box>
                      )}
                    </Flex>
                  </Card>
                )}
              </Box>
            </Flex>
          </Tabs.Content>
        </Box>
      </Tabs.Root>
      <SessionExpiredDialog />
    </>
  );
}

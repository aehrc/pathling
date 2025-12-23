/**
 * Page for managing import operations.
 *
 * @author John Grimes
 */

import { Box, Button, Card, Flex, Progress, Spinner, Tabs, Text } from "@radix-ui/themes";
import { Cross2Icon, ReloadIcon } from "@radix-ui/react-icons";
import { useCallback, useEffect, useState } from "react";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { ImportForm } from "../components/import/ImportForm";
import { ImportPnpForm } from "../components/import/ImportPnpForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useImport, useImportPnp, useServerCapabilities, useUnauthorizedHandler } from "../hooks";
import type { ImportRequest } from "../types/import";
import type { ImportPnpRequest } from "../types/importPnp";

type ImportType = "standard" | "pnp";

interface ActiveImport {
  type: ImportType;
  description: string;
}

export function Import() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, setError } = useAuth();
  const handleUnauthorizedError = useUnauthorizedHandler();

  // Track the current import request and state.
  const [activeImport, setActiveImport] = useState<ActiveImport | null>(null);
  const [importSources, setImportSources] = useState<string[]>([]);
  const [importResourceTypes, setImportResourceTypes] = useState<string[]>([]);
  const [pnpSources, setPnpSources] = useState<string[]>([]);

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
  const standardImport = useImport({
    sources: importSources,
    resourceTypes: importResourceTypes,
    onError: handleError,
  });

  const pnpImport = useImportPnp({
    sources: pnpSources,
    onError: handleError,
  });

  const handleImport = useCallback(
    async (request: ImportRequest) => {
      setImportSources(request.input.map((i) => i.url));
      setImportResourceTypes(request.input.map((i) => i.type));
      setActiveImport({
        type: "standard",
        description: `Importing ${request.input.length} source(s)`,
      });
    },
    [],
  );

  const handleImportPnp = useCallback(
    async (request: ImportPnpRequest) => {
      setPnpSources([request.exportUrl]);
      setActiveImport({
        type: "pnp",
        description: `Importing from ${request.exportUrl}`,
      });
    },
    [],
  );

  // Derive running states from status.
  const isStandardRunning = standardImport.status === "pending" || standardImport.status === "in-progress";
  const isPnpRunning = pnpImport.status === "pending" || pnpImport.status === "in-progress";

  // Start import when sources change.
  useEffect(() => {
    if (activeImport?.type === "standard" && importSources.length > 0 && !isStandardRunning) {
      standardImport.start();
    }
  }, [activeImport, importSources, isStandardRunning, standardImport]);

  useEffect(() => {
    if (activeImport?.type === "pnp" && pnpSources.length > 0 && !isPnpRunning) {
      pnpImport.start();
    }
  }, [activeImport, pnpSources, isPnpRunning, pnpImport]);

  const handleCancel = useCallback(() => {
    if (activeImport?.type === "standard") {
      standardImport.cancel();
    } else {
      pnpImport.cancel();
    }
    setActiveImport(null);
    setImportSources([]);
    setPnpSources([]);
  }, [activeImport, standardImport, pnpImport]);

  const handleNewImport = useCallback(() => {
    setActiveImport(null);
    setImportSources([]);
    setPnpSources([]);
    setImportResourceTypes([]);
  }, []);

  // Determine current state.
  const isRunning = isStandardRunning || isPnpRunning;
  const error = standardImport.error || pnpImport.error;
  const progress = activeImport?.type === "standard" ? standardImport.progress : pnpImport.progress;
  const isComplete = (activeImport?.type === "standard" && !isStandardRunning && importSources.length > 0) ||
    (activeImport?.type === "pnp" && !isPnpRunning && pnpSources.length > 0);

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
                  isSubmitting={isRunning && activeImport?.type === "standard"}
                  disabled={isRunning}
                  resourceTypes={capabilities?.resourceTypes ?? []}
                />
              </Box>
              <Box style={{ flex: 1 }}>
                {activeImport?.type === "standard" && (isRunning || isComplete || error) && (
                  <Card>
                    <Flex direction="column" gap="3">
                      <Flex justify="between" align="start">
                        <Box>
                          <Text weight="medium">Standard Import</Text>
                          <Text size="1" color="gray" as="div">
                            {activeImport.description}
                          </Text>
                        </Box>
                        {isRunning && (
                          <Button size="1" variant="soft" color="red" onClick={handleCancel}>
                            <Cross2Icon />
                            Cancel
                          </Button>
                        )}
                      </Flex>

                      {isRunning && progress !== undefined && (
                        <Box>
                          <Flex justify="between" mb="1">
                            <Text size="1" color="gray">Progress</Text>
                            <Text size="1" color="gray">{progress}%</Text>
                          </Flex>
                          <Progress value={progress} />
                        </Box>
                      )}

                      {isRunning && progress === undefined && (
                        <Flex align="center" gap="2">
                          <ReloadIcon style={{ animation: "spin 1s linear infinite" }} />
                          <Text size="2" color="gray">Processing...</Text>
                        </Flex>
                      )}

                      {error && (
                        <Text size="2" color="red">Error: {error}</Text>
                      )}

                      {isComplete && !error && !isRunning && (
                        <Box>
                          <Text size="2" color="green" mb="2">Import completed successfully</Text>
                          <Flex justify="end">
                            <Button variant="soft" onClick={handleNewImport}>
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
                  isSubmitting={isRunning && activeImport?.type === "pnp"}
                  disabled={isRunning}
                />
              </Box>
              <Box style={{ flex: 1 }}>
                {activeImport?.type === "pnp" && (isRunning || isComplete || error) && (
                  <Card>
                    <Flex direction="column" gap="3">
                      <Flex justify="between" align="start">
                        <Box>
                          <Text weight="medium">FHIR Server Import</Text>
                          <Text size="1" color="gray" as="div">
                            {activeImport.description}
                          </Text>
                        </Box>
                        {isRunning && (
                          <Button size="1" variant="soft" color="red" onClick={handleCancel}>
                            <Cross2Icon />
                            Cancel
                          </Button>
                        )}
                      </Flex>

                      {isRunning && progress !== undefined && (
                        <Box>
                          <Flex justify="between" mb="1">
                            <Text size="1" color="gray">Progress</Text>
                            <Text size="1" color="gray">{progress}%</Text>
                          </Flex>
                          <Progress value={progress} />
                        </Box>
                      )}

                      {isRunning && progress === undefined && (
                        <Flex align="center" gap="2">
                          <ReloadIcon style={{ animation: "spin 1s linear infinite" }} />
                          <Text size="2" color="gray">Processing...</Text>
                        </Flex>
                      )}

                      {error && (
                        <Text size="2" color="red">Error: {error}</Text>
                      )}

                      {isComplete && !error && !isRunning && (
                        <Box>
                          <Text size="2" color="green" mb="2">Import completed successfully</Text>
                          <Flex justify="end">
                            <Button variant="soft" onClick={handleNewImport}>
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

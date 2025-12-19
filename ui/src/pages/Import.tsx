/**
 * Page for managing import operations.
 *
 * @author John Grimes
 */

import { Box, Flex, Spinner, Tabs, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useMemo, useRef } from "react";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { ImportForm } from "../components/import/ImportForm";
import { ImportJobList } from "../components/import/ImportJobList";
import { ImportPnpForm } from "../components/import/ImportPnpForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useJobs } from "../contexts/JobContext";
import { useServerCapabilities } from "../hooks/useServerCapabilities";
import { cancelImport, kickOffImport, kickOffImportPnp } from "../services/import";
import { UnauthorizedError } from "../types/errors";
import type { ImportRequest } from "../types/import";
import type { ImportPnpRequest } from "../types/importPnp";

export function Import() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, client, setError, clearSessionAndPromptLogin } = useAuth();
  const { addJob, updateJobStatus, updateJobError, getImportJobs, getImportPnpJobs } = useJobs();

  const importJobs = getImportJobs();
  const importPnpJobs = getImportPnpJobs();
  const unauthorizedHandledRef = useRef(false);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // Extract resource types from capabilities.
  const resourceTypes = useMemo(() => {
    if (!capabilities?.resources) return [];
    return capabilities.resources.map((r) => r.type).sort();
  }, [capabilities]);

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

  const handleImport = useCallback(
    async (request: ImportRequest) => {
      if (!fhirBaseUrl) return;

      try {
        const accessToken = client?.state.tokenResponse?.access_token;

        const { jobId, pollUrl } = await kickOffImport(fhirBaseUrl, accessToken, request);

        addJob({
          id: jobId,
          type: "import",
          pollUrl,
          status: "pending",
          progress: null,
          request,
          manifest: null,
          error: null,
        });
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        } else {
          setError(err instanceof Error ? err.message : "Failed to start import");
        }
      }
    },
    [client, fhirBaseUrl, addJob, setError, handleUnauthorizedError],
  );

  const handleImportPnp = useCallback(
    async (request: ImportPnpRequest) => {
      if (!fhirBaseUrl) return;

      try {
        const accessToken = client?.state.tokenResponse?.access_token;

        const { jobId, pollUrl } = await kickOffImportPnp(fhirBaseUrl, accessToken, request);

        addJob({
          id: jobId,
          type: "import-pnp",
          pollUrl,
          status: "pending",
          progress: null,
          request,
          manifest: null,
          error: null,
        });
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        } else {
          setError(err instanceof Error ? err.message : "Failed to start import");
        }
      }
    },
    [client, fhirBaseUrl, addJob, setError, handleUnauthorizedError],
  );

  const handleCancel = useCallback(
    async (jobId: string) => {
      if (!fhirBaseUrl) return;

      const job = [...importJobs, ...importPnpJobs].find((j) => j.id === jobId);
      if (!job) return;

      try {
        const accessToken = client?.state.tokenResponse?.access_token;
        // Import jobs always have a pollUrl.
        await cancelImport(fhirBaseUrl, accessToken, job.pollUrl!);
        updateJobStatus(jobId, "cancelled");
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        } else {
          updateJobError(jobId, err instanceof Error ? err.message : "Failed to cancel job");
        }
      }
    },
    [
      client,
      fhirBaseUrl,
      importJobs,
      importPnpJobs,
      updateJobStatus,
      updateJobError,
      handleUnauthorizedError,
    ],
  );

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
          <Tabs.Trigger value="urls">Import from URLs</Tabs.Trigger>
          <Tabs.Trigger value="fhir-server">Import from FHIR server</Tabs.Trigger>
        </Tabs.List>

        <Box pt="4">
          <Tabs.Content value="urls">
            <Flex gap="6" direction={{ initial: "column", md: "row" }}>
              <Box style={{ flex: 1 }}>
                <ImportForm
                  onSubmit={handleImport}
                  isSubmitting={false}
                  disabled={false}
                  resourceTypes={resourceTypes}
                />
              </Box>
              <Box style={{ flex: 1 }}>
                <ImportJobList jobs={importJobs} onCancel={handleCancel} />
              </Box>
            </Flex>
          </Tabs.Content>

          <Tabs.Content value="fhir-server">
            <Flex gap="6" direction={{ initial: "column", md: "row" }}>
              <Box style={{ flex: 1 }}>
                <ImportPnpForm onSubmit={handleImportPnp} isSubmitting={false} disabled={false} />
              </Box>
              <Box style={{ flex: 1 }}>
                <ImportJobList jobs={importPnpJobs} onCancel={handleCancel} />
              </Box>
            </Flex>
          </Tabs.Content>
        </Box>
      </Tabs.Root>
      <SessionExpiredDialog />
    </>
  );
}

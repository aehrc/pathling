/**
 * Page for managing import operations.
 *
 * @author John Grimes
 */

import { InfoCircledIcon, LockClosedIcon } from "@radix-ui/react-icons";
import { Box, Button, Callout, Flex, Heading, Spinner, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useRef } from "react";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { ImportForm } from "../components/import/ImportForm";
import { ImportJobList } from "../components/import/ImportJobList";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useJobs } from "../contexts/JobContext";
import { useServerCapabilities } from "../hooks/useServerCapabilities";
import { initiateAuth } from "../services/auth";
import { cancelImport, kickOffImport } from "../services/import";
import { UnauthorizedError } from "../types/errors";
import type { ImportRequest } from "../types/import";

export function Import() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, client, setLoading, setError, clearSessionAndPromptLogin } = useAuth();
  const { addJob, updateJobStatus, updateJobError, getImportJobs } = useJobs();

  const jobs = getImportJobs();
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

  const handleLogin = async () => {
    if (!fhirBaseUrl) return;
    setLoading(true);
    try {
      await initiateAuth(fhirBaseUrl);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Authentication failed");
    }
  };

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

  const handleCancel = useCallback(
    async (jobId: string) => {
      if (!fhirBaseUrl) return;

      const job = jobs.find((j) => j.id === jobId);
      if (!job) return;

      try {
        const accessToken = client?.state.tokenResponse?.access_token;

        await cancelImport(fhirBaseUrl, accessToken, job.pollUrl);
        updateJobStatus(jobId, "cancelled");
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        } else {
          updateJobError(jobId, err instanceof Error ? err.message : "Failed to cancel job");
        }
      }
    },
    [client, fhirBaseUrl, jobs, updateJobStatus, updateJobError, handleUnauthorizedError],
  );

  // Show loading state while checking server capabilities.
  if (isLoadingCapabilities) {
    return (
      <Box>
        <Heading size="6" mb="4">
          Import
        </Heading>
        <Flex align="center" gap="2">
          <Spinner />
          <Text>Checking server capabilities...</Text>
        </Flex>
        <SessionExpiredDialog />
      </Box>
    );
  }

  // Show login prompt if authentication is required but not authenticated.
  if (capabilities?.authRequired && !isAuthenticated) {
    return (
      <Box>
        <Heading size="6" mb="4">
          Import
        </Heading>

        <Callout.Root>
          <Callout.Icon>
            <InfoCircledIcon />
          </Callout.Icon>
          <Callout.Text>
            You need to authenticate with the FHIR server before you can start importing data.
          </Callout.Text>
        </Callout.Root>

        <Box mt="4">
          <Button size="3" onClick={handleLogin}>
            <LockClosedIcon />
            Login to {capabilities?.serverName ?? window.location.hostname}
          </Button>
        </Box>
        <SessionExpiredDialog />
      </Box>
    );
  }

  // Show import form (either auth not required or user is authenticated).
  return (
    <Box>
      <Heading size="6" mb="4">
        Import
      </Heading>

      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Box style={{ flex: 1 }}>
          <ImportForm onSubmit={handleImport} isSubmitting={false} disabled={false} />
        </Box>

        <Box style={{ flex: 1 }}>
          <ImportJobList jobs={jobs} onCancel={handleCancel} />
        </Box>
      </Flex>
      <SessionExpiredDialog />
    </Box>
  );
}

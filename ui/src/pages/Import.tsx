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
import { useAuth } from "../contexts/AuthContext";
import { useJobs } from "../contexts/JobContext";
import { useSettings } from "../contexts/SettingsContext";
import { checkServerCapabilities, initiateAuth } from "../services/auth";
import { cancelImport, kickOffImport, pollImportStatus } from "../services/import";
import { UnauthorizedError } from "../types/errors";
import type { ImportRequest } from "../types/import";

export function Import() {
  const { fhirBaseUrl } = useSettings();
  const {
    isAuthenticated,
    client,
    authRequired,
    setLoading,
    setError,
    setAuthRequired,
    clearSessionAndPromptLogin,
  } = useAuth();
  const {
    addJob,
    updateJobProgress,
    updateJobManifest,
    updateJobError,
    updateJobStatus,
    getImportJobs,
  } = useJobs();

  const jobs = getImportJobs();
  const pollIntervalRef = useRef<Map<string, number>>(new Map());
  const unauthorizedHandledRef = useRef(false);

  // Handle 401 errors by clearing session and prompting for re-authentication.
  const handleUnauthorizedError = useCallback(() => {
    if (unauthorizedHandledRef.current) return;
    unauthorizedHandledRef.current = true;

    // Stop all polling jobs.
    pollIntervalRef.current.forEach((intervalId) => {
      clearInterval(intervalId);
    });
    pollIntervalRef.current.clear();

    clearSessionAndPromptLogin();
  }, [clearSessionAndPromptLogin]);

  // Reset the unauthorized flag when user becomes authenticated.
  useEffect(() => {
    if (isAuthenticated) {
      unauthorizedHandledRef.current = false;
    }
  }, [isAuthenticated]);

  // Check server capabilities when FHIR URL changes.
  useEffect(() => {
    if (!fhirBaseUrl) return;

    const checkCapabilities = async () => {
      const capabilities = await checkServerCapabilities(fhirBaseUrl);
      setAuthRequired(capabilities.authRequired);
    };

    checkCapabilities();
  }, [fhirBaseUrl, setAuthRequired]);

  // Poll active jobs for progress updates.
  useEffect(() => {
    const activeJobs = jobs.filter(
      (job) => job.status === "pending" || job.status === "in_progress",
    );

    activeJobs.forEach((job) => {
      // Skip if already polling this job.
      if (pollIntervalRef.current.has(job.id)) {
        return;
      }

      const poll = async () => {
        if (!fhirBaseUrl) return;

        try {
          const accessToken = client?.state.tokenResponse?.access_token;

          const result = await pollImportStatus(fhirBaseUrl, accessToken, job.pollUrl);

          if (result.status === "completed" && result.manifest) {
            updateJobManifest(job.id, result.manifest);
            // Stop polling.
            const intervalId = pollIntervalRef.current.get(job.id);
            if (intervalId) {
              clearInterval(intervalId);
              pollIntervalRef.current.delete(job.id);
            }
          } else if (result.status === "in_progress") {
            if (result.progress !== undefined) {
              updateJobProgress(job.id, result.progress);
            }
            updateJobStatus(job.id, "in_progress");
          }
        } catch (err) {
          if (err instanceof UnauthorizedError) {
            handleUnauthorizedError();
          } else {
            updateJobError(job.id, err instanceof Error ? err.message : "Unknown error");
          }
          // Stop polling on error.
          const intervalId = pollIntervalRef.current.get(job.id);
          if (intervalId) {
            clearInterval(intervalId);
            pollIntervalRef.current.delete(job.id);
          }
        }
      };

      // Initial poll.
      poll();
      // Set up interval polling.
      const intervalId = window.setInterval(poll, 3000);
      pollIntervalRef.current.set(job.id, intervalId);
    });

    // Cleanup intervals for jobs that are no longer active.
    pollIntervalRef.current.forEach((intervalId, jobId) => {
      const job = jobs.find((j) => j.id === jobId);
      if (!job || (job.status !== "pending" && job.status !== "in_progress")) {
        clearInterval(intervalId);
        pollIntervalRef.current.delete(jobId);
      }
    });

    return () => {
      // Cleanup on unmount or when dependencies change.
      pollIntervalRef.current.forEach((intervalId) => {
        clearInterval(intervalId);
      });
      pollIntervalRef.current.clear();
    };
  }, [
    jobs,
    client,
    fhirBaseUrl,
    updateJobProgress,
    updateJobManifest,
    updateJobError,
    updateJobStatus,
    handleUnauthorizedError,
  ]);

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
  if (authRequired === null) {
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
  if (authRequired && !isAuthenticated) {
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
            Login with SMART on FHIR
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

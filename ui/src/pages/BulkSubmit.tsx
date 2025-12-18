/**
 * Page for managing bulk submit operations.
 *
 * @author John Grimes
 */

import { InfoCircledIcon, LockClosedIcon } from "@radix-ui/react-icons";
import { Box, Button, Callout, Flex, Heading, Spinner, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useRef } from "react";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { BulkSubmitJobList } from "../components/bulkSubmit/BulkSubmitJobList";
import { BulkSubmitMonitorForm } from "../components/bulkSubmit/BulkSubmitMonitorForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useJobs } from "../contexts/JobContext";
import { useServerCapabilities } from "../hooks/useServerCapabilities";
import { initiateAuth } from "../services/auth";
import { abortBulkSubmit, checkBulkSubmitStatus } from "../services/bulkSubmit";
import type { SubmitterIdentifier } from "../types/bulkSubmit";
import { UnauthorizedError } from "../types/errors";
import type { BulkSubmitJob } from "../types/job";

export function BulkSubmit() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, client, setLoading, setError, clearSessionAndPromptLogin } = useAuth();
  const { addJob, updateJobStatus, updateJobError, getBulkSubmitJobs } = useJobs();

  const bulkSubmitJobs = getBulkSubmitJobs();
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

  const handleMonitor = useCallback(
    async (submissionId: string, submitter: SubmitterIdentifier) => {
      if (!fhirBaseUrl) return;

      try {
        const accessToken = client?.state.tokenResponse?.access_token;

        // Get the status of the submission.
        const result = await checkBulkSubmitStatus(fhirBaseUrl, accessToken, {
          submissionId,
          submitter,
        });

        if (result.status === "completed") {
          // Submission is already complete, create a completed job with the manifest.
          addJob({
            id: crypto.randomUUID(),
            type: "bulk-submit",
            pollUrl: null,
            status: "completed",
            progress: null,
            request: {
              submissionId,
              submitter,
              submissionStatus: "complete",
            },
            submitter,
            submissionId,
            manifest: result.manifest,
            error: null,
          } as Omit<BulkSubmitJob, "createdAt">);
        } else if (result.status === "pending") {
          // Job is running, create a pending job to poll for progress.
          addJob({
            id: result.jobId,
            type: "bulk-submit",
            pollUrl: result.pollUrl,
            status: "pending",
            progress: null,
            request: {
              submissionId,
              submitter,
              submissionStatus: "complete",
            },
            submitter,
            submissionId,
            manifest: null,
            error: null,
          } as Omit<BulkSubmitJob, "createdAt">);
        } else {
          // Job hasn't started yet, create a pending job that will poll for status.
          addJob({
            id: crypto.randomUUID(),
            type: "bulk-submit",
            pollUrl: null,
            status: "pending",
            progress: null,
            request: {
              submissionId,
              submitter,
              submissionStatus: "complete",
            },
            submitter,
            submissionId,
            manifest: null,
            error: null,
          } as Omit<BulkSubmitJob, "createdAt">);
        }
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        } else {
          setError(err instanceof Error ? err.message : "Failed to monitor submission");
        }
      }
    },
    [client, fhirBaseUrl, addJob, setError, handleUnauthorizedError],
  );

  const handleAbort = useCallback(
    async (jobId: string) => {
      if (!fhirBaseUrl) return;

      const job = bulkSubmitJobs.find((j) => j.id === jobId);
      if (!job) return;

      try {
        const accessToken = client?.state.tokenResponse?.access_token;

        await abortBulkSubmit(fhirBaseUrl, accessToken, job.submissionId, job.submitter);
        updateJobStatus(jobId, "cancelled");
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        } else {
          updateJobError(jobId, err instanceof Error ? err.message : "Failed to abort");
        }
      }
    },
    [client, fhirBaseUrl, bulkSubmitJobs, updateJobStatus, updateJobError, handleUnauthorizedError],
  );

  // Show loading state while checking server capabilities.
  if (isLoadingCapabilities) {
    return (
      <Box>
        <Heading size="6" mb="4">
          Bulk submit
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
          Bulk submit
        </Heading>

        <Callout.Root>
          <Callout.Icon>
            <InfoCircledIcon />
          </Callout.Icon>
          <Callout.Text>You need to login before you can submit data.</Callout.Text>
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

  // Show bulk submit form.
  return (
    <Box>
      <Heading size="6" mb="4">
        Bulk submit
      </Heading>

      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Box style={{ flex: 1 }}>
          <BulkSubmitMonitorForm
            onMonitor={handleMonitor}
            isSubmitting={false}
            disabled={false}
          />
        </Box>
        <Box style={{ flex: 1 }}>
          <BulkSubmitJobList jobs={bulkSubmitJobs} onAbort={handleAbort} />
        </Box>
      </Flex>
      <SessionExpiredDialog />
    </Box>
  );
}

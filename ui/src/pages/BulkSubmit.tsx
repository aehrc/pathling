/**
 * Page for managing bulk submit operations.
 *
 * @author John Grimes
 */

import { InfoCircledIcon, LockClosedIcon } from "@radix-ui/react-icons";
import { Box, Button, Callout, Flex, Heading, Spinner, Tabs, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useRef } from "react";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { BulkSubmitForm } from "../components/bulkSubmit/BulkSubmitForm";
import { BulkSubmitJobList } from "../components/bulkSubmit/BulkSubmitJobList";
import { BulkSubmitMonitorForm } from "../components/bulkSubmit/BulkSubmitMonitorForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useJobs } from "../contexts/JobContext";
import { useServerCapabilities } from "../hooks/useServerCapabilities";
import { initiateAuth } from "../services/auth";
import { abortBulkSubmit, kickOffBulkSubmit } from "../services/bulkSubmit";
import { UnauthorizedError } from "../types/errors";
import type { BulkSubmitRequest, SubmitterIdentifier } from "../types/bulkSubmit";
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

  const handleSubmit = useCallback(
    async (request: BulkSubmitRequest) => {
      if (!fhirBaseUrl) return;

      try {
        const accessToken = client?.state.tokenResponse?.access_token;

        const { submissionId } = await kickOffBulkSubmit(fhirBaseUrl, accessToken, request);

        addJob({
          id: crypto.randomUUID(),
          type: "bulk-submit",
          pollUrl: `${fhirBaseUrl}/$bulk-submit-status`,
          status: "pending",
          progress: null,
          request,
          submitter: request.submitter,
          submissionId,
          manifest: null,
          error: null,
        } as Omit<BulkSubmitJob, "createdAt">);
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        } else {
          setError(err instanceof Error ? err.message : "Failed to submit");
        }
      }
    },
    [client, fhirBaseUrl, addJob, setError, handleUnauthorizedError],
  );

  const handleMonitor = useCallback(
    async (submissionId: string, submitter: SubmitterIdentifier) => {
      if (!fhirBaseUrl) return;

      // Create a job to monitor an existing submission.
      addJob({
        id: crypto.randomUUID(),
        type: "bulk-submit",
        pollUrl: `${fhirBaseUrl}/$bulk-submit-status`,
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
    },
    [fhirBaseUrl, addJob],
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
          Bulk Submit
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
          Bulk Submit
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

  // Show bulk submit forms.
  return (
    <Box>
      <Heading size="6" mb="4">
        Bulk Submit
      </Heading>

      <Tabs.Root defaultValue="create">
        <Tabs.List>
          <Tabs.Trigger value="create">Create Submission</Tabs.Trigger>
          <Tabs.Trigger value="monitor">Monitor Submission</Tabs.Trigger>
        </Tabs.List>

        <Box pt="4">
          <Tabs.Content value="create">
            <Flex gap="6" direction={{ initial: "column", md: "row" }}>
              <Box style={{ flex: 1 }}>
                <BulkSubmitForm onSubmit={handleSubmit} isSubmitting={false} disabled={false} />
              </Box>
              <Box style={{ flex: 1 }}>
                <BulkSubmitJobList jobs={bulkSubmitJobs} onAbort={handleAbort} />
              </Box>
            </Flex>
          </Tabs.Content>

          <Tabs.Content value="monitor">
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
          </Tabs.Content>
        </Box>
      </Tabs.Root>
      <SessionExpiredDialog />
    </Box>
  );
}

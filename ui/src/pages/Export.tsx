/**
 * Page for managing bulk export operations.
 *
 * @author John Grimes
 */

import { InfoCircledIcon, LockClosedIcon } from "@radix-ui/react-icons";
import { Box, Button, Callout, Flex, Heading, Spinner, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useRef } from "react";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { ExportForm } from "../components/export/ExportForm";
import { ExportJobList } from "../components/export/ExportJobList";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useJobs } from "../contexts/JobContext";
import { useServerCapabilities } from "../hooks/useServerCapabilities";
import { initiateAuth } from "../services/auth";
import { cancelJob as cancelJobApi, kickOffExportWithFetch } from "../services/export";
import { UnauthorizedError } from "../types/errors";
import type { ExportRequest } from "../types/export";

export function Export() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, client, setLoading, setError, clearSessionAndPromptLogin } = useAuth();
  const { addJob, updateJobStatus, updateJobError, getExportJobs } = useJobs();

  const jobs = getExportJobs();
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

  const handleExport = useCallback(
    async (request: ExportRequest) => {
      if (!fhirBaseUrl) return;

      try {
        const accessToken = client?.state.tokenResponse?.access_token;

        const { jobId, pollUrl } = await kickOffExportWithFetch(fhirBaseUrl, accessToken, request);

        addJob({
          id: jobId,
          type: "export",
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
          setError(err instanceof Error ? err.message : "Failed to start export");
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

        await cancelJobApi(fhirBaseUrl, accessToken, job.pollUrl);
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
    [client, setError, handleUnauthorizedError],
  );

  // Show loading state while checking server capabilities.
  if (isLoadingCapabilities) {
    return (
      <Box>
        <Heading size="6" mb="4">
          Bulk Export
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
          Bulk Export
        </Heading>

        <Callout.Root>
          <Callout.Icon>
            <InfoCircledIcon />
          </Callout.Icon>
          <Callout.Text>
            You need to authenticate with the FHIR server before you can start exporting data.
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

  // Show export form (either auth not required or user is authenticated).
  return (
    <Box>
      <Heading size="6" mb="4">
        Bulk Export
      </Heading>

      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Box style={{ flex: 1 }}>
          <ExportForm onSubmit={handleExport} isSubmitting={false} disabled={false} />
        </Box>

        <Box style={{ flex: 1 }}>
          <ExportJobList jobs={jobs} onCancel={handleCancel} onDownload={handleDownload} />
        </Box>
      </Flex>
      <SessionExpiredDialog />
    </Box>
  );
}

/**
 * Main dashboard page for managing bulk exports.
 *
 * @author John Grimes
 */

import { useEffect, useCallback, useRef } from "react";
import { Box, Button, Callout, Flex, Heading, Spinner, Text } from "@radix-ui/themes";
import { LockClosedIcon, InfoCircledIcon } from "@radix-ui/react-icons";
import { useSettings } from "../contexts/SettingsContext";
import { useAuth } from "../contexts/AuthContext";
import { useJobs } from "../contexts/JobContext";
import { ExportForm } from "../components/export/ExportForm";
import { ExportJobList } from "../components/export/ExportJobList";
import { initiateAuth, checkServerCapabilities } from "../services/auth";
import {
  kickOffExportWithFetch,
  pollJobStatus,
  cancelJob as cancelJobApi,
} from "../services/export";
import type { ExportRequest } from "../types/export";

export function Dashboard() {
  const { fhirBaseUrl } = useSettings();
  const {
    isAuthenticated,
    client,
    authRequired,
    setLoading,
    setError,
    setAuthRequired,
  } = useAuth();
  const {
    jobs,
    addJob,
    updateJobProgress,
    updateJobManifest,
    updateJobError,
    updateJobStatus,
  } = useJobs();

  const pollIntervalRef = useRef<Map<string, number>>(new Map());

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
      (job) => job.status === "pending" || job.status === "in_progress"
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

          const result = await pollJobStatus(fhirBaseUrl, accessToken, job.pollUrl);

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
          updateJobError(
            job.id,
            err instanceof Error ? err.message : "Unknown error"
          );
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

  const handleExport = useCallback(
    async (request: ExportRequest) => {
      if (!fhirBaseUrl) return;

      try {
        const accessToken = client?.state.tokenResponse?.access_token;

        const { jobId, pollUrl } = await kickOffExportWithFetch(
          fhirBaseUrl,
          accessToken,
          request
        );

        addJob({
          id: jobId,
          pollUrl,
          status: "pending",
          progress: null,
          request,
          manifest: null,
          error: null,
        });
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "Failed to start export"
        );
      }
    },
    [client, fhirBaseUrl, addJob, setError]
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
        updateJobError(
          jobId,
          err instanceof Error ? err.message : "Failed to cancel job"
        );
      }
    },
    [client, fhirBaseUrl, jobs, updateJobStatus, updateJobError]
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
        setError(
          err instanceof Error ? err.message : "Download failed"
        );
      }
    },
    [client, setError]
  );

  // Show loading state while checking server capabilities.
  if (authRequired === null) {
    return (
      <Box>
        <Heading size="6" mb="4">
          Bulk Export
        </Heading>
        <Flex align="center" gap="2">
          <Spinner />
          <Text>Checking server capabilities...</Text>
        </Flex>
      </Box>
    );
  }

  // Show login prompt if authentication is required but not authenticated.
  if (authRequired && !isAuthenticated) {
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
            You need to authenticate with the FHIR server before you can start
            exporting data.
          </Callout.Text>
        </Callout.Root>

        <Box mt="4">
          <Button size="3" onClick={handleLogin}>
            <LockClosedIcon />
            Login with SMART on FHIR
          </Button>
        </Box>
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
          <ExportForm
            onSubmit={handleExport}
            isSubmitting={false}
            disabled={false}
          />
        </Box>

        <Box style={{ flex: 1 }}>
          <ExportJobList
            jobs={jobs}
            onCancel={handleCancel}
            onDownload={handleDownload}
          />
        </Box>
      </Flex>
    </Box>
  );
}

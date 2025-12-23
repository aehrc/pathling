/**
 * Page for managing bulk submit operations.
 *
 * @author John Grimes
 */

import { Box, Button, Card, Flex, Progress, Spinner, Text } from "@radix-ui/themes";
import { Cross2Icon, ReloadIcon } from "@radix-ui/react-icons";
import { useCallback, useEffect, useRef, useState } from "react";
import { bulkSubmit, bulkSubmitStatus } from "../api";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { BulkSubmitMonitorForm } from "../components/bulkSubmit/BulkSubmitMonitorForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useServerCapabilities, useUnauthorizedHandler } from "../hooks";
import type { SubmitterIdentifier, StatusManifest } from "../types/bulkSubmit";
import { UnauthorizedError } from "../types/errors";

interface MonitoringState {
  submissionId: string;
  submitter: SubmitterIdentifier;
  status: "pending" | "in-progress" | "completed" | "completed-with-errors" | "aborted" | "failed";
  progress?: string;
  manifest?: StatusManifest;
  error?: string;
}

export function BulkSubmit() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, client, setError } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const handleUnauthorizedError = useUnauthorizedHandler();
  const pollingRef = useRef<NodeJS.Timeout | null>(null);

  const [monitoring, setMonitoring] = useState<MonitoringState | null>(null);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // Poll for status updates.
  const pollStatus = useCallback(async () => {
    if (!monitoring || !fhirBaseUrl) return;

    try {
      const result = await bulkSubmitStatus(fhirBaseUrl, {
        submitter: monitoring.submitter,
        submissionId: monitoring.submissionId,
        accessToken,
      });

      setMonitoring((prev) => {
        if (!prev) return null;
        if (result.status === "completed" || result.status === "completed-with-errors" || result.status === "aborted") {
          // Stop polling when complete.
          if (pollingRef.current) {
            clearInterval(pollingRef.current);
            pollingRef.current = null;
          }
          return {
            ...prev,
            status: result.status,
            manifest: result.manifest,
            progress: undefined,
          };
        }
        return {
          ...prev,
          status: result.status,
          progress: result.progress,
        };
      });
    } catch (err) {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
        pollingRef.current = null;
      }
      if (err instanceof UnauthorizedError) {
        handleUnauthorizedError();
      } else {
        setMonitoring((prev) =>
          prev
            ? {
                ...prev,
                status: "failed",
                error: err instanceof Error ? err.message : "Failed to check status",
              }
            : null,
        );
      }
    }
  }, [monitoring, fhirBaseUrl, accessToken, handleUnauthorizedError]);

  // Start polling when monitoring begins.
  useEffect(() => {
    if (monitoring && monitoring.status !== "completed" && monitoring.status !== "completed-with-errors" && monitoring.status !== "aborted" && monitoring.status !== "failed") {
      // Initial poll.
      pollStatus();
      // Set up interval polling.
      pollingRef.current = setInterval(pollStatus, 3000);
    }
    return () => {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
        pollingRef.current = null;
      }
    };
  }, [monitoring?.submissionId, pollStatus]);

  const handleMonitor = async (submissionId: string, submitter: SubmitterIdentifier) => {
    setMonitoring({
      submissionId,
      submitter,
      status: "pending",
    });
  };

  const handleAbort = useCallback(async () => {
    if (!monitoring || !fhirBaseUrl) return;

    try {
      await bulkSubmit(fhirBaseUrl, {
        submitter: monitoring.submitter,
        submissionId: monitoring.submissionId,
        submissionStatus: "aborted",
        accessToken,
      });
      setMonitoring((prev) => (prev ? { ...prev, status: "aborted" } : null));
    } catch (err) {
      if (err instanceof UnauthorizedError) {
        handleUnauthorizedError();
      } else {
        setError(err instanceof Error ? err.message : "Failed to abort");
      }
    }
  }, [monitoring, fhirBaseUrl, accessToken, handleUnauthorizedError, setError]);

  const handleNewMonitor = () => {
    setMonitoring(null);
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

  const isActive = monitoring && (monitoring.status === "pending" || monitoring.status === "in-progress");
  const progressValue = monitoring?.progress ? parseInt(monitoring.progress.replace("%", ""), 10) : undefined;

  // Show bulk submit form.
  return (
    <>
      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Box style={{ flex: 1 }}>
          <BulkSubmitMonitorForm
            onMonitor={handleMonitor}
            isSubmitting={!!isActive}
            disabled={!!monitoring}
          />
        </Box>
        <Box style={{ flex: 1 }}>
          {monitoring && (
            <Card>
              <Flex direction="column" gap="3">
                <Flex justify="between" align="start">
                  <Box>
                    <Text weight="medium">Bulk Submit Monitor</Text>
                    <Text size="1" color="gray" as="div">
                      Submission: {monitoring.submissionId}
                    </Text>
                    <Text size="1" color="gray" as="div">
                      Submitter: {monitoring.submitter.value}
                    </Text>
                  </Box>
                  {isActive && (
                    <Button size="1" variant="soft" color="red" onClick={handleAbort}>
                      <Cross2Icon />
                      Abort
                    </Button>
                  )}
                </Flex>

                {isActive && progressValue !== undefined && (
                  <Box>
                    <Flex justify="between" mb="1">
                      <Text size="1" color="gray">Progress</Text>
                      <Text size="1" color="gray">{progressValue}%</Text>
                    </Flex>
                    <Progress value={progressValue} />
                  </Box>
                )}

                {isActive && progressValue === undefined && (
                  <Flex align="center" gap="2">
                    <ReloadIcon style={{ animation: "spin 1s linear infinite" }} />
                    <Text size="2" color="gray">Monitoring...</Text>
                  </Flex>
                )}

                {monitoring.status === "failed" && monitoring.error && (
                  <Text size="2" color="red">Error: {monitoring.error}</Text>
                )}

                {monitoring.status === "aborted" && (
                  <Text size="2" color="orange">Submission was aborted</Text>
                )}

                {(monitoring.status === "completed" || monitoring.status === "completed-with-errors") && (
                  <Box>
                    <Text size="2" color={monitoring.status === "completed" ? "green" : "orange"} mb="2">
                      {monitoring.status === "completed" ? "Submission completed successfully" : "Submission completed with errors"}
                    </Text>
                    {monitoring.manifest && (
                      <Text size="1" color="gray" as="div">
                        Transaction time: {monitoring.manifest.transactionTime}
                      </Text>
                    )}
                    <Flex justify="end" mt="3">
                      <Button variant="soft" onClick={handleNewMonitor}>
                        Monitor Another
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

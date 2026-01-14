/**
 * Page for managing bulk submit operations.
 *
 * @author John Grimes
 */

import { Cross2Icon, ReloadIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, Progress, Spinner, Text } from "@radix-ui/themes";

import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { BulkSubmitMonitorForm } from "../components/bulkSubmit/BulkSubmitMonitorForm";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useBulkSubmitMonitor, useServerCapabilities } from "../hooks";

import type { SubmitterIdentifier } from "../types/bulkSubmit";

/**
 *
 */
export function BulkSubmit() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, setError } = useAuth();

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // Monitor bulk submit operations. 401 errors handled globally.
  const monitor = useBulkSubmitMonitor({
    onError: (error) => setError(error.message),
  });

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

  // Derive display state from the hook.
  const isMonitoring = monitor.status !== "idle";
  const isActive = monitor.status === "pending" || monitor.status === "in-progress";
  const isComplete = monitor.status === "complete";
  const isCancelled = monitor.status === "cancelled";
  const isError = monitor.status === "error";
  const hasErrors = isComplete && monitor.result?.error && monitor.result.error.length > 0;

  // Show bulk submit form.
  return (
    <>
      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Box style={{ flex: 1 }}>
          <BulkSubmitMonitorForm
            onMonitor={(submissionId: string, submitter: SubmitterIdentifier) => {
              monitor.startWith({ submissionId, submitter });
            }}
            isSubmitting={isActive}
            disabled={isMonitoring}
          />
        </Box>
        <Box style={{ flex: 1 }}>
          {isMonitoring && (
            <Card>
              <Flex direction="column" gap="3">
                <Flex justify="between" align="start">
                  <Box>
                    <Text weight="medium">Bulk Submit Monitor</Text>
                    <Text size="1" color="gray" as="div">
                      Submission: {monitor.request?.submissionId}
                    </Text>
                    <Text size="1" color="gray" as="div">
                      Submitter: {monitor.request?.submitter.value}
                    </Text>
                  </Box>
                  {isActive && (
                    <Button size="1" variant="soft" color="red" onClick={monitor.cancel}>
                      <Cross2Icon />
                      Abort
                    </Button>
                  )}
                </Flex>

                {isActive && monitor.progress !== undefined && (
                  <Box>
                    <Flex justify="between" mb="1">
                      <Text size="1" color="gray">
                        Progress
                      </Text>
                      <Text size="1" color="gray">
                        {monitor.progress}%
                      </Text>
                    </Flex>
                    <Progress value={monitor.progress} />
                  </Box>
                )}

                {isActive && monitor.progress === undefined && (
                  <Flex align="center" gap="2">
                    <ReloadIcon style={{ animation: "spin 1s linear infinite" }} />
                    <Text size="2" color="gray">
                      Monitoring...
                    </Text>
                  </Flex>
                )}

                {isError && monitor.error && (
                  <Text size="2" color="red">
                    Error: {monitor.error.message}
                  </Text>
                )}

                {isCancelled && (
                  <Text size="2" color="orange">
                    Submission was aborted
                  </Text>
                )}

                {isComplete && (
                  <Box>
                    <Text size="2" color={hasErrors ? "orange" : "green"} mb="2">
                      {hasErrors
                        ? "Submission completed with errors"
                        : "Submission completed successfully"}
                    </Text>
                    {monitor.result && (
                      <Text size="1" color="gray" as="div">
                        Transaction time: {monitor.result.transactionTime}
                      </Text>
                    )}
                    <Flex justify="end" mt="3">
                      <Button variant="soft" onClick={monitor.reset}>
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

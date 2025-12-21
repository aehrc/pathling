/**
 * Page for executing SQL on FHIR ViewDefinitions.
 *
 * @author John Grimes
 */

import { Box, Flex, Spinner, Text } from "@radix-ui/themes";
import { useCallback, useEffect, useRef, useState } from "react";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { SqlOnFhirForm } from "../components/sqlOnFhir/SqlOnFhirForm";
import { SqlOnFhirResultTable } from "../components/sqlOnFhir/SqlOnFhirResultTable";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useServerCapabilities } from "../hooks/useServerCapabilities";
import {
  executeInlineViewDefinition,
  executeStoredViewDefinition,
} from "../services/sqlOnFhir";
import { UnauthorizedError } from "../types/errors";
import type { ViewDefinitionExecuteRequest, ViewDefinitionResult } from "../types/sqlOnFhir";

export function SqlOnFhir() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, client, clearSessionAndPromptLogin } = useAuth();

  const [executionResult, setExecutionResult] = useState<ViewDefinitionResult | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [executionError, setExecutionError] = useState<Error | null>(null);
  const [hasExecuted, setHasExecuted] = useState(false);
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

  const handleExecute = useCallback(
    async (request: ViewDefinitionExecuteRequest) => {
      setIsExecuting(true);
      setExecutionError(null);
      setHasExecuted(true);

      try {
        const accessToken = client?.state.tokenResponse?.access_token;
        let result: ViewDefinitionResult;

        if (request.mode === "stored" && request.viewDefinitionId) {
          result = await executeStoredViewDefinition(
            fhirBaseUrl,
            accessToken,
            request.viewDefinitionId,
          );
        } else if (request.mode === "inline" && request.viewDefinitionJson) {
          result = await executeInlineViewDefinition(
            fhirBaseUrl,
            accessToken,
            request.viewDefinitionJson,
          );
        } else {
          throw new Error("Invalid request: missing view definition ID or JSON");
        }

        setExecutionResult(result);
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        } else {
          setExecutionError(err instanceof Error ? err : new Error("Execution failed"));
        }
      } finally {
        setIsExecuting(false);
      }
    },
    [client, fhirBaseUrl, handleUnauthorizedError],
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

  // Determine actual error to display (ignore unauthorized since it's handled separately).
  const displayError =
    executionError && !(executionError instanceof UnauthorizedError) ? executionError : null;

  return (
    <>
      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Box style={{ flex: 1 }}>
          <SqlOnFhirForm
            onExecute={handleExecute}
            isExecuting={isExecuting}
            disabled={false}
          />
        </Box>

        <Box style={{ flex: 1 }}>
          <SqlOnFhirResultTable
            rows={executionResult?.rows}
            columns={executionResult?.columns}
            isLoading={isExecuting}
            error={displayError}
            hasExecuted={hasExecuted}
          />
        </Box>
      </Flex>
      <SessionExpiredDialog />
    </>
  );
}

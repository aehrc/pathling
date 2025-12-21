/**
 * Page for executing SQL on FHIR ViewDefinitions.
 *
 * @author John Grimes
 */

import { Box, Flex, Spinner, Text } from "@radix-ui/themes";
import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";
import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { SqlOnFhirForm } from "../components/sqlOnFhir/SqlOnFhirForm";
import { SqlOnFhirResultTable } from "../components/sqlOnFhir/SqlOnFhirResultTable";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useServerCapabilities } from "../hooks/useServerCapabilities";
import {
  createViewDefinition,
  executeInlineViewDefinition,
  executeStoredViewDefinition,
} from "../services/sqlOnFhir";
import { UnauthorizedError } from "../types/errors";
import type {
  CreateViewDefinitionResult,
  ViewDefinitionExecuteRequest,
  ViewDefinitionResult,
} from "../types/sqlOnFhir";

export function SqlOnFhir() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated, client, clearSessionAndPromptLogin } = useAuth();
  const queryClient = useQueryClient();

  const [executionResult, setExecutionResult] = useState<ViewDefinitionResult | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [executionError, setExecutionError] = useState<Error | null>(null);
  const [hasExecuted, setHasExecuted] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
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

  const handleSaveToServer = useCallback(
    async (json: string): Promise<CreateViewDefinitionResult> => {
      setIsSaving(true);
      try {
        const accessToken = client?.state.tokenResponse?.access_token;
        const result = await createViewDefinition(fhirBaseUrl, accessToken, json);
        // Invalidate the ViewDefinitions cache to refresh the list.
        await queryClient.invalidateQueries({ queryKey: ["viewDefinitions"] });
        return result;
      } catch (err) {
        if (err instanceof UnauthorizedError) {
          handleUnauthorizedError();
        }
        throw err;
      } finally {
        setIsSaving(false);
      }
    },
    [client, fhirBaseUrl, handleUnauthorizedError, queryClient],
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
            onSaveToServer={handleSaveToServer}
            isExecuting={isExecuting}
            isSaving={isSaving}
            disabled={false}
          />
        </Box>

        <Box style={{ flex: 1, overflowX: "auto" }}>
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

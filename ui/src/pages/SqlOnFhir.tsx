/**
 * Page for executing SQL on FHIR ViewDefinitions.
 *
 * @author John Grimes
 */

import { Box, Flex, Spinner, Text } from "@radix-ui/themes";
import { useState } from "react";

import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { SqlOnFhirForm } from "../components/sqlOnFhir/SqlOnFhirForm";
import { ViewCard } from "../components/sqlOnFhir/ViewCard";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useSaveViewDefinition, useServerCapabilities } from "../hooks";

import type { ViewRunRequest } from "../types/hooks";
import type { ViewJob } from "../types/viewJob";

/**
 *
 */
export function SqlOnFhir() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated } = useAuth();

  // Track all view query jobs.
  const [queries, setQueries] = useState<ViewJob[]>([]);

  // Track error messages for display.
  const [, setError] = useState<string | null>(null);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // Mutation for saving a ViewDefinition to the server. 401 errors handled globally.
  const { mutateAsync: saveViewDefinition, isPending: isSaving } = useSaveViewDefinition();

  /**
   * Handles execution of a ViewDefinition query.
   *
   * @param request - The view run request configuration.
   */
  const handleExecute = (request: ViewRunRequest) => {
    const newQuery: ViewJob = {
      id: crypto.randomUUID(),
      mode: request.mode,
      viewDefinitionId: request.viewDefinitionId,
      viewDefinitionJson: request.viewDefinitionJson,
      limit: request.limit,
      createdAt: new Date(),
    };
    // Prepend new queries so most recent appears first.
    setQueries((prev) => [newQuery, ...prev]);
  };

  /**
   * Handles closing/removing a query card.
   *
   * @param id - The ID of the query to close.
   */
  const handleCloseQuery = (id: string) => {
    setQueries((prev) => prev.filter((query) => query.id !== id));
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

  return (
    <>
      <Flex gap="6" direction={{ initial: "column", md: "row" }}>
        <Box style={{ flex: 1 }}>
          <SqlOnFhirForm
            onExecute={handleExecute}
            onSaveToServer={saveViewDefinition}
            isExecuting={false}
            isSaving={isSaving}
          />
        </Box>

        <Flex direction="column" gap="3" style={{ flex: 1, overflow: "hidden" }}>
          {queries.map((job) => (
            <ViewCard
              key={job.id}
              job={job}
              onError={(message) => setError(message)}
              onClose={() => handleCloseQuery(job.id)}
            />
          ))}
        </Flex>
      </Flex>

      <SessionExpiredDialog />
    </>
  );
}

/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Page component for executing SQL on FHIR ViewDefinitions.
 *
 * @returns The SQL on FHIR page component.
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

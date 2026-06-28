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
 * Page for executing the SQL on FHIR `$viewdefinition-run` and
 * `$sqlquery-run` operations.
 *
 * @author John Grimes
 */

import { Box, Flex, Heading, Spinner, Text } from "@radix-ui/themes";
import { useState } from "react";

import { LoginRequired } from "../components/auth/LoginRequired";
import { SessionExpiredDialog } from "../components/auth/SessionExpiredDialog";
import { SqlOnFhirForm } from "../components/sqlOnFhir/SqlOnFhirForm";
import { SqlQueryCard } from "../components/sqlOnFhir/SqlQueryCard";
import { extractRequestSql } from "../components/sqlOnFhir/sqlQueryFormHelpers";
import { ViewCard } from "../components/sqlOnFhir/ViewCard";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { useSaveSqlQueryLibrary, useSaveViewDefinition, useServerCapabilities } from "../hooks";

import type { SqlOnFhirMode } from "../components/sqlOnFhir/SqlOnFhirForm";
import type { ViewRunRequest } from "../hooks";
import type { SqlQueryJob, SqlQueryRequest } from "../types/sqlQuery";
import type { ViewJob } from "../types/viewJob";

interface PageJob {
  type: "view" | "sql-query";
  /** Underlying job. */
  job: ViewJob | SqlQueryJob;
}

/**
 * Page component for executing SQL on FHIR operations.
 *
 * @returns The SQL on FHIR page.
 */
export function SqlOnFhir() {
  const { fhirBaseUrl } = config;
  const { isAuthenticated } = useAuth();

  const [mode, setMode] = useState<SqlOnFhirMode>("view-definition");

  // Track all view query jobs and SQL query jobs as a single timeline so
  // they can be sorted by createdAt regardless of source.
  const [pageJobs, setPageJobs] = useState<PageJob[]>([]);

  const [, setError] = useState<string | null>(null);

  // Fetch server capabilities to determine if auth is required.
  const { data: capabilities, isLoading: isLoadingCapabilities } =
    useServerCapabilities(fhirBaseUrl);

  // Mutations: ViewDefinition save and SQL query Library save.
  const { mutateAsync: saveViewDefinition, isPending: isSavingViewDefinition } =
    useSaveViewDefinition();
  const { mutateAsync: saveSqlQueryLibrary, isPending: isSavingSqlQueryLibrary } =
    useSaveSqlQueryLibrary();

  /**
   * Adds a ViewDefinition execution to the result column.
   *
   * @param request - The view run request configuration.
   */
  const handleExecuteViewDefinition = (request: ViewRunRequest) => {
    const newJob: ViewJob = {
      id: crypto.randomUUID(),
      mode: request.mode,
      viewDefinitionId: request.viewDefinitionId,
      viewDefinitionJson: request.viewDefinitionJson,
      limit: request.limit,
      createdAt: new Date(),
    };
    setPageJobs((prev) => [{ type: "view", job: newJob }, ...prev]);
  };

  /**
   * Adds a SQL query execution to the result column.
   *
   * @param request - The SQL query request.
   */
  const handleExecuteSqlQuery = (request: SqlQueryRequest) => {
    const newJob: SqlQueryJob = {
      id: crypto.randomUUID(),
      mode: request.mode,
      request,
      sql: extractRequestSql(request),
      createdAt: new Date(),
    };
    setPageJobs((prev) => [{ type: "sql-query", job: newJob }, ...prev]);
  };

  /**
   * Removes a result card from the column.
   *
   * @param id - The job ID of the card to remove.
   */
  const handleCloseJob = (id: string) => {
    setPageJobs((prev) => prev.filter((entry) => entry.job.id !== id));
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
      <Flex direction="column" gap="4">
        <Heading size="6">SQL on FHIR</Heading>

        <Flex gap="6" direction={{ initial: "column", md: "row" }}>
          <Box style={{ flex: 1 }}>
            <SqlOnFhirForm
              mode={mode}
              onModeChange={setMode}
              onExecuteViewDefinition={handleExecuteViewDefinition}
              onSaveViewDefinition={saveViewDefinition}
              onExecuteSqlQuery={handleExecuteSqlQuery}
              onSaveSqlQueryLibrary={saveSqlQueryLibrary}
              isViewDefinitionExecuting={false}
              isViewDefinitionSaving={isSavingViewDefinition}
              isSqlQueryExecuting={false}
              isSqlQuerySaving={isSavingSqlQueryLibrary}
            />
          </Box>

          <Flex direction="column" gap="3" style={{ flex: 1, overflow: "hidden" }}>
            {pageJobs.map((entry) =>
              entry.type === "view" ? (
                <ViewCard
                  key={entry.job.id}
                  job={entry.job as ViewJob}
                  onError={(message) => setError(message)}
                  onClose={() => handleCloseJob(entry.job.id)}
                />
              ) : (
                <SqlQueryCard
                  key={entry.job.id}
                  job={entry.job as SqlQueryJob}
                  onError={(message) => setError(message)}
                  onClose={() => handleCloseJob(entry.job.id)}
                />
              ),
            )}
          </Flex>
        </Flex>
      </Flex>

      <SessionExpiredDialog />
    </>
  );
}

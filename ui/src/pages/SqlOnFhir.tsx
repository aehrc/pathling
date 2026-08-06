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
 * Page for the SQL on FHIR data operations: running a subject through
 * `$sql-run`, and exporting one or many through `$sql-export`.
 *
 * @author John Grimes
 */

import { Box, Flex, Heading } from "@radix-ui/themes";
import { useState } from "react";

import { CapabilityGuard } from "../components/auth/CapabilityGuard";
import { ExportSetPanel } from "../components/sqlOnFhir/ExportSetPanel";
import { SqlExportCardWrapper } from "../components/sqlOnFhir/SqlExportCardWrapper";
import { SqlOnFhirForm } from "../components/sqlOnFhir/SqlOnFhirForm";
import { SqlQueryCard } from "../components/sqlOnFhir/SqlQueryCard";
import { extractRequestSql } from "../components/sqlOnFhir/sqlQueryFormHelpers";
import { ViewCard } from "../components/sqlOnFhir/ViewCard";
import { useSaveSqlQueryLibrary, useSaveViewDefinition } from "../hooks";
import {
  buildExportSetRequest,
  captureQueryEntry,
  captureViewEntry,
  findNameCollisions,
  parseIdList,
  removeEntry,
  renameEntry,
} from "../hooks/exportSetHelpers";

import type { SqlOnFhirMode } from "../components/sqlOnFhir/SqlOnFhirForm";
import type { ExportSetEntry } from "../hooks/exportSetHelpers";
import type { SqlExportRequest } from "../hooks/useSqlExport";
import type { SqlExportFormat } from "../types/sqlExport";
import type { SqlQueryJob, SqlQueryRequest } from "../types/sqlQuery";
import type { ViewJob, ViewRunRequest } from "../types/viewJob";

interface PageJob {
  type: "view" | "sql-query";
  /** Underlying job. */
  job: ViewJob | SqlQueryJob;
}

/** An export set job started from the panel, shown in the results column. */
interface ExportSetJob {
  id: string;
  request: SqlExportRequest;
  createdAt: Date;
}

/**
 * Page component for executing SQL on FHIR operations.
 *
 * @returns The SQL on FHIR page.
 */
export function SqlOnFhir() {
  const [mode, setMode] = useState<SqlOnFhirMode>("view-definition");

  // The export set: subjects captured from the form, plus the job-wide
  // settings that apply to all of them.
  const [exportSet, setExportSet] = useState<ExportSetEntry[]>([]);
  const [exportSetFormat, setExportSetFormat] = useState<SqlExportFormat>("ndjson");
  const [exportSetFilters, setExportSetFilters] = useState({
    patients: "",
    groups: "",
    since: "",
  });
  const [exportSetJobs, setExportSetJobs] = useState<ExportSetJob[]>([]);

  // Track all view query jobs and SQL query jobs as a single timeline so
  // they can be sorted by createdAt regardless of source.
  const [pageJobs, setPageJobs] = useState<PageJob[]>([]);

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
   * Captures the current ViewDefinition into the export set.
   *
   * @param request - The view run request the form describes.
   */
  const handleAddViewToExportSet = (request: ViewRunRequest) => {
    setExportSet((prev) => [...prev, captureViewEntry(crypto.randomUUID(), request, prev)]);
  };

  /**
   * Captures the current SQL query, with its bindings, into the export set.
   *
   * @param request - The SQL query request the form describes.
   */
  const handleAddQueryToExportSet = (request: SqlQueryRequest) => {
    setExportSet((prev) => [...prev, captureQueryEntry(crypto.randomUUID(), request, prev)]);
  };

  /**
   * Starts one export job carrying every entry in the set.
   */
  const handleExportSet = () => {
    const request = buildExportSetRequest(exportSet, exportSetFormat, {
      patientIds: parseIdList(exportSetFilters.patients),
      groupIds: parseIdList(exportSetFilters.groups),
      since: exportSetFilters.since,
    });
    setExportSetJobs((prev) => [
      { id: crypto.randomUUID(), request, createdAt: new Date() },
      ...prev,
    ]);
  };

  /**
   * Removes a result card from the column.
   *
   * @param id - The job ID of the card to remove.
   */
  const handleCloseJob = (id: string) => {
    setPageJobs((prev) => prev.filter((entry) => entry.job.id !== id));
  };

  return (
    <CapabilityGuard>
      {() => (
        <Flex direction="column" gap="4">
          <Heading size="6">SQL on FHIR</Heading>

          <Flex gap="6" direction={{ initial: "column", md: "row" }}>
            {/* The min-width of zero lets the form column shrink to share width
                evenly with the results column, rather than being held open by
                wide content such as long view references. */}
            <Box style={{ flex: 1, minWidth: 0 }}>
              <SqlOnFhirForm
                mode={mode}
                onModeChange={setMode}
                onExecuteViewDefinition={handleExecuteViewDefinition}
                onAddViewToExportSet={handleAddViewToExportSet}
                onSaveViewDefinition={saveViewDefinition}
                onExecuteSqlQuery={handleExecuteSqlQuery}
                onAddQueryToExportSet={handleAddQueryToExportSet}
                onSaveSqlQueryLibrary={saveSqlQueryLibrary}
                isViewDefinitionExecuting={false}
                isViewDefinitionSaving={isSavingViewDefinition}
                isSqlQueryExecuting={false}
                isSqlQuerySaving={isSavingSqlQueryLibrary}
              />

              <Box mt="4">
                <ExportSetPanel
                  entries={exportSet}
                  format={exportSetFormat}
                  filters={exportSetFilters}
                  collisions={findNameCollisions(exportSet)}
                  onRename={(id, name) => setExportSet((prev) => renameEntry(prev, id, name))}
                  onRemove={(id) => setExportSet((prev) => removeEntry(prev, id))}
                  onClear={() => setExportSet([])}
                  onFormatChange={setExportSetFormat}
                  onFilterChange={(field, value) =>
                    setExportSetFilters((prev) => ({ ...prev, [field]: value }))
                  }
                  onExport={handleExportSet}
                />
              </Box>
            </Box>

            <Flex direction="column" gap="3" style={{ flex: 1, overflow: "hidden" }}>
              {exportSetJobs.map((job) => (
                <SqlExportCardWrapper
                  key={job.id}
                  subjects={job.request.subjects}
                  format={job.request.format}
                  header={job.request.header}
                  patientIds={job.request.patientIds}
                  groupIds={job.request.groupIds}
                  since={job.request.since}
                  createdAt={job.createdAt}
                  onClose={() =>
                    setExportSetJobs((prev) => prev.filter((entry) => entry.id !== job.id))
                  }
                />
              ))}
              {pageJobs.map((entry) =>
                entry.type === "view" ? (
                  <ViewCard
                    key={entry.job.id}
                    job={entry.job as ViewJob}
                    onClose={() => handleCloseJob(entry.job.id)}
                  />
                ) : (
                  <SqlQueryCard
                    key={entry.job.id}
                    job={entry.job as SqlQueryJob}
                    onClose={() => handleCloseJob(entry.job.id)}
                  />
                ),
              )}
            </Flex>
          </Flex>
        </Flex>
      )}
    </CapabilityGuard>
  );
}

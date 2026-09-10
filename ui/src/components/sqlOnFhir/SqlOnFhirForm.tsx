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
 * Top-level shell for the SQL on FHIR page form. Hosts the mode switch
 * between a ViewDefinition subject (ViewDefinitionForm) and a SQL query
 * subject (SqlQueryForm), both run through `$sql-run`.
 *
 * @author John Grimes
 */

import { Box, Tabs } from "@radix-ui/themes";

import { SqlQueryForm } from "./SqlQueryForm";
import { ViewDefinitionForm } from "./ViewDefinitionForm";

import type { CreateViewDefinitionResult } from "../../types/sqlOnFhir";
import type {
  SaveSqlQueryLibraryResult,
  SqlQueryLibrary,
  SqlQueryRequest,
} from "../../types/sqlQuery";
import type { ViewRunRequest } from "../../types/viewJob";

/**
 * Mode of the SQL on FHIR page form.
 */
export type SqlOnFhirMode = "view-definition" | "sql-query";

interface SqlOnFhirFormProps {
  /** The currently selected mode. */
  mode: SqlOnFhirMode;
  /** Callback fired when the mode changes. */
  onModeChange: (mode: SqlOnFhirMode) => void;
  /** Callback fired when the user executes a ViewDefinition. */
  onExecuteViewDefinition: (request: ViewRunRequest) => void;
  /** Callback fired when the user adds the current ViewDefinition to the export set. */
  onAddViewToExportSet: (request: ViewRunRequest) => void;
  /** Callback fired when the user saves an inline ViewDefinition to the server. */
  onSaveViewDefinition: (json: string) => Promise<CreateViewDefinitionResult>;
  /** Callback fired when the user executes a SQL query. */
  onExecuteSqlQuery: (request: SqlQueryRequest) => void;
  /** Callback fired when the user adds the current SQL query to the export set. */
  onAddQueryToExportSet: (request: SqlQueryRequest) => void;
  /** Callback fired when the user saves an inline SQL query Library. */
  onSaveSqlQueryLibrary: (library: SqlQueryLibrary) => Promise<SaveSqlQueryLibraryResult>;
  /** Whether ViewDefinition execution is in progress. */
  isViewDefinitionExecuting: boolean;
  /** Whether ViewDefinition save is in progress. */
  isViewDefinitionSaving: boolean;
  /** Whether SQL query execution is in progress. */
  isSqlQueryExecuting: boolean;
  /** Whether SQL query Library save is in progress. */
  isSqlQuerySaving: boolean;
}

/**
 * Renders the SQL on FHIR mode switch and the active form variant.
 *
 * @param props - The component props.
 * @param props.mode - The currently selected mode.
 * @param props.onModeChange - Callback fired when the mode changes.
 * @param props.onExecuteViewDefinition - Callback fired when the user executes a ViewDefinition.
 * @param props.onAddViewToExportSet - Callback fired when the user adds the current ViewDefinition to the export set.
 * @param props.onSaveViewDefinition - Callback fired when the user saves an inline ViewDefinition to the server.
 * @param props.onExecuteSqlQuery - Callback fired when the user executes a SQL query.
 * @param props.onAddQueryToExportSet - Callback fired when the user adds the current SQL query to the export set.
 * @param props.onSaveSqlQueryLibrary - Callback fired when the user saves an inline SQL query Library.
 * @param props.isViewDefinitionExecuting - Whether ViewDefinition execution is in progress.
 * @param props.isViewDefinitionSaving - Whether ViewDefinition save is in progress.
 * @param props.isSqlQueryExecuting - Whether SQL query execution is in progress.
 * @param props.isSqlQuerySaving - Whether SQL query Library save is in progress.
 * @returns The form shell.
 */
export function SqlOnFhirForm({
  mode,
  onModeChange,
  onExecuteViewDefinition,
  onAddViewToExportSet,
  onSaveViewDefinition,
  onExecuteSqlQuery,
  onAddQueryToExportSet,
  onSaveSqlQueryLibrary,
  isViewDefinitionExecuting,
  isViewDefinitionSaving,
  isSqlQueryExecuting,
  isSqlQuerySaving,
}: Readonly<SqlOnFhirFormProps>) {
  return (
    <Tabs.Root value={mode} onValueChange={(value) => onModeChange(value as SqlOnFhirMode)}>
      <Tabs.List>
        <Tabs.Trigger value="view-definition">View definition</Tabs.Trigger>
        <Tabs.Trigger value="sql-query">SQL query</Tabs.Trigger>
      </Tabs.List>

      <Box pt="4">
        <Tabs.Content value="view-definition">
          <ViewDefinitionForm
            onExecute={onExecuteViewDefinition}
            onAddToExportSet={onAddViewToExportSet}
            onSaveToServer={onSaveViewDefinition}
            isExecuting={isViewDefinitionExecuting}
            isSaving={isViewDefinitionSaving}
          />
        </Tabs.Content>
        <Tabs.Content value="sql-query">
          <SqlQueryForm
            onExecute={onExecuteSqlQuery}
            onAddToExportSet={onAddQueryToExportSet}
            onSaveToServer={onSaveSqlQueryLibrary}
            isExecuting={isSqlQueryExecuting}
            isSaving={isSqlQuerySaving}
          />
        </Tabs.Content>
      </Box>
    </Tabs.Root>
  );
}

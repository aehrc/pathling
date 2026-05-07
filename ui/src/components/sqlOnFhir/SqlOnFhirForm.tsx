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
 * between `$viewdefinition-run` (ViewDefinitionForm) and `$sqlquery-run`
 * (SqlQueryForm).
 *
 * @author John Grimes
 */

import { Box, Tabs } from "@radix-ui/themes";

import { SqlQueryForm } from "./SqlQueryForm";
import { ViewDefinitionForm } from "./ViewDefinitionForm";

import type { ViewRunRequest } from "../../hooks";
import type { CreateViewDefinitionResult } from "../../types/sqlOnFhir";
import type {
  SaveSqlQueryLibraryResult,
  SqlQueryLibrary,
  SqlQueryRequest,
} from "../../types/sqlQuery";

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
  /** Callback fired when the user saves an inline ViewDefinition to the server. */
  onSaveViewDefinition: (json: string) => Promise<CreateViewDefinitionResult>;
  /** Callback fired when the user executes a SQL query. */
  onExecuteSqlQuery: (request: SqlQueryRequest) => void;
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
 * @param props.mode
 * @param props.onModeChange
 * @param props.onExecuteViewDefinition
 * @param props.onSaveViewDefinition
 * @param props.onExecuteSqlQuery
 * @param props.onSaveSqlQueryLibrary
 * @param props.isViewDefinitionExecuting
 * @param props.isViewDefinitionSaving
 * @param props.isSqlQueryExecuting
 * @param props.isSqlQuerySaving
 * @returns The form shell.
 */
export function SqlOnFhirForm({
  mode,
  onModeChange,
  onExecuteViewDefinition,
  onSaveViewDefinition,
  onExecuteSqlQuery,
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
            onSaveToServer={onSaveViewDefinition}
            isExecuting={isViewDefinitionExecuting}
            isSaving={isViewDefinitionSaving}
          />
        </Tabs.Content>
        <Tabs.Content value="sql-query">
          <SqlQueryForm
            onExecute={onExecuteSqlQuery}
            onSaveToServer={onSaveSqlQueryLibrary}
            isExecuting={isSqlQueryExecuting}
            isSaving={isSqlQuerySaving}
          />
        </Tabs.Content>
      </Box>
    </Tabs.Root>
  );
}

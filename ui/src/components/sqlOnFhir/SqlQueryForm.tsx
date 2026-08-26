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
 * Form for executing the SQL on FHIR `$sql-run` operation.
 *
 * Hosts a stored/inline tab pair - each owning its own parameter
 * presentation - and the output controls, then dispatches Execute and Save
 * actions through the supplied callbacks.
 *
 * @author John Grimes
 */

import { PlayIcon, PlusIcon, UploadIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, Heading, Tabs } from "@radix-ui/themes";
import { useState } from "react";

import {
  areRuntimeBindingsValid,
  buildInlineSqlQueryLibrary,
  buildParameterTypes,
  canExecuteInlineForm,
  canSaveInlineForm,
  findDuplicateParameterNames,
  rowsToBindings,
} from "./sqlQueryFormHelpers";
import { SqlQueryInlineTab } from "./SqlQueryInlineTab";
import { SqlQueryOutputControls } from "./SqlQueryOutputControls";
import { SqlQueryStoredTab } from "./SqlQueryStoredTab";
import { useSqlQueryLibraries, useSqlViews, useViewDefinitions } from "../../hooks";
import { ErrorCallout } from "../error/ErrorCallout";

import type {
  SaveSqlQueryLibraryResult,
  SqlQueryLibrary,
  SqlQueryOutputFormat,
  SqlQueryParameterDeclaration,
  SqlQueryParameterType,
  SqlQueryRelatedArtifact,
  SqlQueryRequest,
  SqlQueryRuntimeBindings as SqlQueryRuntimeBindingsState,
} from "../../types/sqlQuery";

type LibrarySource = "stored" | "inline";

interface SqlQueryFormProps {
  /** Callback fired when the user clicks Execute. */
  onExecute: (request: SqlQueryRequest) => void;
  /** Callback fired when the user adds the current query to the export set. */
  onAddToExportSet?: (request: SqlQueryRequest) => void;
  /** Callback fired to save an inline Library to the server. */
  onSaveToServer: (library: SqlQueryLibrary) => Promise<SaveSqlQueryLibraryResult>;
  /** Whether a query is currently executing. */
  isExecuting: boolean;
  /** Whether a save is currently in progress. */
  isSaving: boolean;
  /** Optional disable for the whole form. */
  disabled?: boolean;
}

/**
 * Renders the SQL query form.
 *
 * @param props - The component props.
 * @param props.onExecute - Callback fired when the user clicks Execute.
 * @param props.onAddToExportSet - Callback fired when the user adds the current query to the export set.
 * @param props.onSaveToServer - Callback fired to save an inline Library to the server.
 * @param props.isExecuting - Whether a query is currently executing.
 * @param props.isSaving - Whether a save is currently in progress.
 * @param props.disabled - Optional disable for the whole form.
 * @returns The form card.
 */
export function SqlQueryForm({
  onExecute,
  onAddToExportSet,
  onSaveToServer,
  isExecuting,
  isSaving,
  disabled = false,
}: Readonly<SqlQueryFormProps>) {
  const [source, setSource] = useState<LibrarySource>("stored");
  const [selectedLibraryId, setSelectedLibraryId] = useState<string>("");

  // Inline-mode authoring state.
  const [title, setTitle] = useState<string>("");
  const [sql, setSql] = useState<string>("");
  const [tables, setTables] = useState<SqlQueryRelatedArtifact[]>([]);
  const [parameters, setParameters] = useState<SqlQueryParameterDeclaration[]>([]);

  // Runtime bindings, output format and execution options.
  const [bindings, setBindings] = useState<SqlQueryRuntimeBindingsState>({});
  const [format, setFormat] = useState<SqlQueryOutputFormat>("ndjson");
  const [limit, setLimit] = useState<string>("");
  const [csvHeader, setCsvHeader] = useState<boolean>(true);

  // Save error to surface inline (Execute errors are surfaced in the result card).
  const [saveError, setSaveError] = useState<Error | null>(null);

  const { data: storedLibraries, isLoading: isLoadingLibraries } = useSqlQueryLibraries();
  const { data: storedViews, isLoading: isLoadingViews } = useSqlViews();
  const { data: viewDefinitions } = useViewDefinitions();

  // Derived: declared parameters surfaced through the runtime bindings panel.
  // The selected source may be a SQLQuery or a SQLView, so locate it across
  // both stored lists.
  const activeStoredLibrary = [...(storedLibraries ?? []), ...(storedViews ?? [])].find(
    (lib) => lib.id === selectedLibraryId,
  );
  const declaredParameters: Array<{
    name: string;
    type: SqlQueryParameterType;
  }> =
    source === "stored"
      ? (activeStoredLibrary?.parameters ?? [])
      : parameters
          .filter((p) => p.name.trim() !== "")
          .map((p) => ({ name: p.name.trim(), type: p.type }));

  const handleBindingChange = (name: string, value: string) => {
    setBindings((prev) => ({ ...prev, [name]: value }));
  };

  const inlineInput = { title, sql, tables, parameters };

  const baseRequestOptions = () => {
    // The result card shows at most 10 rows as a preview, so cap the request
    // accordingly. Full-result downloads will be handled by a future SQL
    // query export operation.
    const parsedLimit = limit.trim() === "" ? undefined : Number.parseInt(limit, 10);
    const requestLimit = parsedLimit === undefined ? 10 : Math.min(parsedLimit, 10);
    // The inline tab's rows carry their own values, so they are the source of
    // the inline request's bindings; the stored tab uses the name-keyed map.
    return {
      format,
      limit: requestLimit,
      header: format === "csv" ? csvHeader : undefined,
      bindings: source === "stored" ? bindings : rowsToBindings(parameters),
      parameterTypes: buildParameterTypes(declaredParameters),
    };
  };

  /**
   * Builds the request the form currently describes, or undefined when it
   * describes nothing runnable.
   *
   * @returns The request, or undefined.
   */
  const buildRequest = (): SqlQueryRequest | undefined => {
    if (source === "stored") {
      if (!selectedLibraryId) return undefined;
      return {
        mode: "stored",
        libraryId: selectedLibraryId,
        // Carry the resolved SQL for display only; the server receives just
        // the reference.
        sql: activeStoredLibrary?.sql,
        ...baseRequestOptions(),
      };
    }
    if (!canExecuteInlineForm(inlineInput)) return undefined;
    return {
      mode: "inline",
      library: buildInlineSqlQueryLibrary(inlineInput),
      ...baseRequestOptions(),
    };
  };

  const handleExecute = () => {
    const request = buildRequest();
    if (request) {
      onExecute(request);
    }
  };

  const handleAddToExportSet = () => {
    const request = buildRequest();
    if (request) {
      onAddToExportSet?.(request);
    }
  };

  const handleSaveToServer = async () => {
    setSaveError(null);
    if (!canSaveInlineForm(inlineInput)) return;
    try {
      const library = buildInlineSqlQueryLibrary(inlineInput);
      const result = await onSaveToServer(library);
      setSource("stored");
      setSelectedLibraryId(result.id);
    } catch (err) {
      setSaveError(err instanceof Error ? err : new Error("Failed to save"));
    }
  };

  const limitInvalid =
    limit.trim() !== "" && (!/^[0-9]+$/.test(limit.trim()) || Number.parseInt(limit, 10) <= 0);

  const bindingsValid = areRuntimeBindingsValid(declaredParameters, bindings);

  const canExecute =
    !disabled &&
    !isExecuting &&
    !limitInvalid &&
    bindingsValid &&
    (source === "stored" ? selectedLibraryId !== "" : canExecuteInlineForm(inlineInput));

  const canSave = !disabled && !isSaving && source === "inline" && canSaveInlineForm(inlineInput);

  return (
    <Card>
      <Flex direction="column" gap="4">
        <Heading size="4">SQL query</Heading>

        <Tabs.Root value={source} onValueChange={(value) => setSource(value as LibrarySource)}>
          <Tabs.List>
            <Tabs.Trigger value="stored">Select query</Tabs.Trigger>
            <Tabs.Trigger value="inline">Provide SQL</Tabs.Trigger>
          </Tabs.List>

          <Box pt="4">
            <Tabs.Content value="stored">
              <SqlQueryStoredTab
                queries={storedLibraries}
                views={storedViews}
                isLoading={isLoadingLibraries || isLoadingViews}
                selectedId={selectedLibraryId}
                onSelect={setSelectedLibraryId}
                bindings={bindings}
                onBindingChange={handleBindingChange}
                disabled={disabled || isExecuting}
              />
            </Tabs.Content>
            <Tabs.Content value="inline">
              <SqlQueryInlineTab
                title={title}
                onTitleChange={setTitle}
                sql={sql}
                onSqlChange={setSql}
                tables={tables}
                onTablesChange={setTables}
                parameters={parameters}
                onParametersChange={setParameters}
                duplicateNames={findDuplicateParameterNames(parameters)}
                viewDefinitions={(viewDefinitions ?? []).map((vd) => ({
                  id: vd.id,
                  name: vd.name,
                  url: vd.url,
                }))}
                sqlViews={(storedViews ?? []).map((view) => ({
                  id: view.id,
                  name: view.title,
                  url: view.url,
                }))}
                disabled={disabled || isExecuting}
              />
              {saveError && <ErrorCallout message={saveError.message} size="1" mt="3" />}
            </Tabs.Content>
          </Box>
        </Tabs.Root>

        <SqlQueryOutputControls
          format={format}
          onFormatChange={setFormat}
          limit={limit}
          onLimitChange={setLimit}
          header={csvHeader}
          onHeaderChange={setCsvHeader}
          disabled={disabled || isExecuting}
        />

        <Flex gap="3">
          <Button
            size="2"
            onClick={handleExecute}
            disabled={!canExecute}
            style={{ flex: 1, whiteSpace: "nowrap" }}
          >
            <PlayIcon />
            {isExecuting ? "Executing..." : "Execute"}
          </Button>
          {onAddToExportSet && (
            <Button
              size="2"
              variant="soft"
              onClick={handleAddToExportSet}
              disabled={!canExecute}
              style={{ whiteSpace: "nowrap" }}
            >
              <PlusIcon />
              Add to export set
            </Button>
          )}
          {source === "inline" && (
            <Button
              size="2"
              variant="soft"
              onClick={handleSaveToServer}
              disabled={!canSave}
              style={{ flex: 1, whiteSpace: "nowrap" }}
            >
              <UploadIcon />
              {isSaving ? "Saving..." : "Save to server"}
            </Button>
          )}
        </Flex>
      </Flex>
    </Card>
  );
}

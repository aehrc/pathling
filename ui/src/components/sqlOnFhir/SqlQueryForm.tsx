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
 * Form for executing the SQL on FHIR `$sqlquery-run` operation.
 *
 * Hosts a stored/inline tab pair, runtime bindings and output controls,
 * then dispatches Execute and Save actions through the supplied
 * callbacks.
 *
 * @author John Grimes
 */

import { PlayIcon, UploadIcon } from "@radix-ui/react-icons";
import { Box, Button, Callout, Card, Flex, Heading, Tabs } from "@radix-ui/themes";
import { useState } from "react";

import { useSqlQueryLibraries, useViewDefinitions } from "../../hooks";
import { FieldLabel } from "../FieldLabel";
import {
  areRuntimeBindingsValid,
  buildInlineSqlQueryLibrary,
  buildParameterTypes,
  canExecuteInlineForm,
  canSaveInlineForm,
} from "./sqlQueryFormHelpers";
import { SqlQueryInlineTab } from "./SqlQueryInlineTab";
import { SqlQueryOutputControls } from "./SqlQueryOutputControls";
import { SqlQueryRuntimeBindings } from "./SqlQueryRuntimeBindings";
import { SqlQueryStoredTab } from "./SqlQueryStoredTab";

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
 * @param props.onSaveToServer - Callback fired to save an inline Library to the server.
 * @param props.isExecuting - Whether a query is currently executing.
 * @param props.isSaving - Whether a save is currently in progress.
 * @param props.disabled - Optional disable for the whole form.
 * @returns The form card.
 */
export function SqlQueryForm({
  onExecute,
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
  const { data: viewDefinitions } = useViewDefinitions();

  // Derived: declared parameters surfaced through the runtime bindings panel.
  const activeStoredLibrary = storedLibraries?.find((lib) => lib.id === selectedLibraryId);
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
    return {
      format,
      limit: requestLimit,
      header: format === "csv" ? csvHeader : undefined,
      bindings,
      parameterTypes: buildParameterTypes(declaredParameters),
    };
  };

  const handleExecute = () => {
    if (source === "stored") {
      if (!selectedLibraryId) return;
      const request: SqlQueryRequest = {
        mode: "stored",
        libraryId: selectedLibraryId,
        ...baseRequestOptions(),
      };
      onExecute(request);
      return;
    }
    if (!canExecuteInlineForm(inlineInput)) return;
    const library = buildInlineSqlQueryLibrary(inlineInput);
    const request: SqlQueryRequest = {
      mode: "inline",
      library,
      ...baseRequestOptions(),
    };
    onExecute(request);
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
                libraries={storedLibraries}
                isLoading={isLoadingLibraries}
                selectedId={selectedLibraryId}
                onSelect={setSelectedLibraryId}
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
                viewDefinitions={(viewDefinitions ?? []).map((vd) => ({
                  id: vd.id,
                  name: vd.name,
                }))}
                disabled={disabled || isExecuting}
              />
              {saveError && (
                <Callout.Root color="red" mt="3" size="1">
                  <Callout.Text>{saveError.message}</Callout.Text>
                </Callout.Root>
              )}
            </Tabs.Content>
          </Box>
        </Tabs.Root>

        <Box>
          <FieldLabel mb="2">Runtime parameter values</FieldLabel>
          <SqlQueryRuntimeBindings
            parameters={declaredParameters}
            bindings={bindings}
            onChange={handleBindingChange}
            disabled={disabled || isExecuting}
          />
        </Box>

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
          <Button size="3" onClick={handleExecute} disabled={!canExecute} style={{ flex: 1 }}>
            <PlayIcon />
            {isExecuting ? "Executing..." : "Execute"}
          </Button>
          {source === "inline" && (
            <Button
              size="3"
              variant="soft"
              onClick={handleSaveToServer}
              disabled={!canSave}
              style={{ flex: 1 }}
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

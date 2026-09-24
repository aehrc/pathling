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
 * "Provide SQL" tab body for the SQL query form: SQL editor, views editor,
 * terminology editor and parameters editor.
 *
 * @author John Grimes
 */

import { PlusIcon, TrashIcon } from "@radix-ui/react-icons";
import { Box, Button, Flex, IconButton, Select, Text, TextArea, TextField } from "@radix-ui/themes";

import { findSourceByUrl } from "../../hooks/sqlQueryHelpers";
import { FieldGuidance } from "../FieldGuidance";
import { FieldLabel } from "../FieldLabel";
import { ParameterValueInput } from "./ParameterValueInput";

import type {
  SourceOption,
  SqlQueryParameterDeclaration,
  SqlQueryParameterType,
  SqlQueryRelatedArtifact,
} from "../../types/sqlQuery";

const PARAMETER_TYPES: SqlQueryParameterType[] = [
  "string",
  "code",
  "integer",
  "decimal",
  "boolean",
  "date",
  "dateTime",
];

/** A table source that carries a canonical URL, and so can be referenced. */
type ReferenceableSource = SourceOption & { url: string };

/**
 * Reports whether a source carries a canonical URL. A source without one can
 * never satisfy a canonical dependency reference, so it is not offered.
 *
 * @param source - The source to test.
 * @returns True when the source has a non-empty URL.
 */
function isReferenceable(source: SourceOption): source is ReferenceableSource {
  return Boolean(source.url);
}

/**
 * Renders a grouped list of selectable table sources for the source picker,
 * each bound by its canonical URL.
 *
 * @param props - The component props.
 * @param props.label - The group heading (e.g. "View definitions").
 * @param props.sources - The referenceable sources to list in this group.
 * @returns The select group, or null when there are no sources.
 */
function SourceSelectGroup({
  label,
  sources,
}: Readonly<{ label: string; sources: ReferenceableSource[] }>) {
  if (sources.length === 0) {
    return null;
  }
  return (
    <Select.Group>
      <Select.Label>{label}</Select.Label>
      {sources.map((source) => (
        <Select.Item key={source.id} value={source.url}>
          {source.name}
        </Select.Item>
      ))}
    </Select.Group>
  );
}

interface SqlQueryInlineTabProps {
  /** Library title (used for `Library.title` on save). */
  title: string;
  /** Callback fired when the title changes. */
  onTitleChange: (title: string) => void;
  /** SQL text. */
  sql: string;
  /** Callback fired when the SQL changes. */
  onSqlChange: (sql: string) => void;
  /** Configured view rows (related artefacts). */
  tables: SqlQueryRelatedArtifact[];
  /** Callback fired when the view rows change. */
  onTablesChange: (tables: SqlQueryRelatedArtifact[]) => void;
  /** Configured terminology rows (related artefacts). */
  terminology: SqlQueryRelatedArtifact[];
  /** Callback fired when the terminology rows change. */
  onTerminologyChange: (terminology: SqlQueryRelatedArtifact[]) => void;
  /** Configured declared parameters. */
  parameters: SqlQueryParameterDeclaration[];
  /** Callback fired when the parameters list changes. */
  onParametersChange: (parameters: SqlQueryParameterDeclaration[]) => void;
  /** Parameter names declared by more than one row. */
  duplicateNames: ReadonlySet<string>;
  /** Available stored ViewDefinitions for the source selector. */
  viewDefinitions: SourceOption[];
  /** Available stored SQLViews for the source selector. */
  sqlViews: SourceOption[];
  /** Whether the controls should be disabled. */
  disabled?: boolean;
}

/**
 * Renders the "Provide SQL" tab body.
 *
 * @param props - The component props.
 * @param props.title - Library title (used for `Library.title` on save).
 * @param props.onTitleChange - Callback fired when the title changes.
 * @param props.sql - SQL text.
 * @param props.onSqlChange - Callback fired when the SQL changes.
 * @param props.tables - Configured view rows (related artefacts).
 * @param props.onTablesChange - Callback fired when the view rows change.
 * @param props.terminology - Configured terminology rows (related artefacts).
 * @param props.onTerminologyChange - Callback fired when the terminology rows change.
 * @param props.parameters - Configured declared parameters.
 * @param props.onParametersChange - Callback fired when the parameters list changes.
 * @param props.duplicateNames - Parameter names declared by more than one row.
 * @param props.viewDefinitions - Available stored ViewDefinitions for the source selector.
 * @param props.sqlViews - Available stored SQLViews for the source selector.
 * @param props.disabled - Whether the controls should be disabled.
 * @returns The tab body.
 */
export function SqlQueryInlineTab({
  title,
  onTitleChange,
  sql,
  onSqlChange,
  tables,
  onTablesChange,
  terminology,
  onTerminologyChange,
  parameters,
  onParametersChange,
  duplicateNames,
  viewDefinitions,
  sqlViews,
  disabled = false,
}: Readonly<SqlQueryInlineTabProps>) {
  const viewDefinitionSources = viewDefinitions.filter(isReferenceable);
  const sqlViewSources = sqlViews.filter(isReferenceable);
  const allSources = [...viewDefinitionSources, ...sqlViewSources];
  const hasSources = allSources.length > 0;

  const handleAddTable = () => {
    onTablesChange([
      ...tables,
      {
        rowId: crypto.randomUUID(),
        label: "",
        referenceUrl: "",
      },
    ]);
  };

  const handleRemoveTable = (rowId: string) => {
    onTablesChange(tables.filter((t) => t.rowId !== rowId));
  };

  const handleUpdateTable = (rowId: string, update: Partial<SqlQueryRelatedArtifact>) => {
    onTablesChange(tables.map((t) => (t.rowId === rowId ? { ...t, ...update } : t)));
  };

  const handleAddTerminology = () => {
    onTerminologyChange([
      ...terminology,
      { rowId: crypto.randomUUID(), label: "", referenceUrl: "" },
    ]);
  };

  const handleRemoveTerminology = (rowId: string) => {
    onTerminologyChange(terminology.filter((t) => t.rowId !== rowId));
  };

  const handleUpdateTerminology = (rowId: string, update: Partial<SqlQueryRelatedArtifact>) => {
    onTerminologyChange(terminology.map((t) => (t.rowId === rowId ? { ...t, ...update } : t)));
  };

  const handleAddParameter = () => {
    onParametersChange([
      ...parameters,
      {
        rowId: crypto.randomUUID(),
        name: "",
        type: "string",
        value: "",
      },
    ]);
  };

  const handleRemoveParameter = (rowId: string) => {
    onParametersChange(parameters.filter((p) => p.rowId !== rowId));
  };

  const handleUpdateParameter = (rowId: string, update: Partial<SqlQueryParameterDeclaration>) => {
    onParametersChange(parameters.map((p) => (p.rowId === rowId ? { ...p, ...update } : p)));
  };

  return (
    <Flex direction="column" gap="4">
      <Box>
        <FieldLabel mb="1" optional>
          Title
        </FieldLabel>
        <TextField.Root
          value={title}
          placeholder="e.g. patients-by-condition"
          onChange={(e) => onTitleChange(e.target.value)}
          disabled={disabled}
          aria-label="Library title"
        />
        <FieldGuidance>
          Used as `Library.title` when saving to the server. Required to enable the Save action.
        </FieldGuidance>
      </Box>

      <Box>
        <FieldLabel mb="1">SQL</FieldLabel>
        <TextArea
          size="1"
          resize="vertical"
          rows={10}
          placeholder="SELECT ..."
          value={sql}
          onChange={(e) => onSqlChange(e.target.value)}
          disabled={disabled}
          style={{ fontFamily: "monospace" }}
          aria-label="SQL"
        />
        <FieldGuidance>
          The SQL is encoded as Base64 in `Library.content[0].data`. The plain text is also kept in
          the `sql-text` extension.
        </FieldGuidance>
      </Box>

      <Box>
        <FieldLabel mb="1" optional>
          Views
        </FieldLabel>
        {tables.length === 0 && (
          <FieldGuidance>
            Each view maps a label to a stored ViewDefinition or SQLView. The query needs at least
            one view or terminology dependency.
          </FieldGuidance>
        )}
        <Flex direction="column" gap="2" mt="1">
          {tables.map((table, index) => (
            <Flex key={table.rowId} gap="2" align="end" wrap="wrap">
              <Box style={{ flex: 1, minWidth: "10rem" }}>
                {index === 0 && (
                  <Text size="1" color="gray" as="div" mb="1">
                    Label
                  </Text>
                )}
                <TextField.Root
                  value={table.label}
                  placeholder="e.g. patients"
                  onChange={(e) => handleUpdateTable(table.rowId, { label: e.target.value })}
                  disabled={disabled}
                  aria-label={`Label for view ${index + 1}`}
                />
              </Box>
              <Box style={{ flex: 1, minWidth: "12rem" }}>
                {index === 0 && (
                  <Text size="1" color="gray" as="div" mb="1">
                    Source
                  </Text>
                )}
                <Select.Root
                  value={
                    findSourceByUrl(allSources, table.referenceUrl) ? table.referenceUrl : undefined
                  }
                  onValueChange={(value) => handleUpdateTable(table.rowId, { referenceUrl: value })}
                  disabled={disabled || !hasSources}
                >
                  <Select.Trigger
                    style={{ width: "100%" }}
                    placeholder={hasSources ? "Select a source" : "Nothing to reference"}
                    aria-label={`Source for view ${index + 1}`}
                  />
                  <Select.Content>
                    <SourceSelectGroup label="View definitions" sources={viewDefinitionSources} />
                    <SourceSelectGroup label="SQL views" sources={sqlViewSources} />
                  </Select.Content>
                </Select.Root>
                {table.referenceUrl && !findSourceByUrl(allSources, table.referenceUrl) && (
                  <Text size="1" color="amber" as="div" mt="1">
                    Source not found:{" "}
                    <code style={{ wordBreak: "break-all" }}>{table.referenceUrl}</code>
                  </Text>
                )}
              </Box>
              <IconButton
                size="2"
                variant="soft"
                color="gray"
                aria-label={`Remove view ${index + 1}`}
                onClick={() => handleRemoveTable(table.rowId)}
                disabled={disabled}
              >
                <TrashIcon />
              </IconButton>
            </Flex>
          ))}
        </Flex>
        <Box mt="2">
          <Button size="2" variant="soft" onClick={handleAddTable} disabled={disabled}>
            <PlusIcon />
            Add view
          </Button>
        </Box>
      </Box>

      <Box>
        <FieldLabel mb="1" optional>
          Terminology
        </FieldLabel>
        <FieldGuidance>
          Each row maps a label to the canonical URL of a value set or concept map.
        </FieldGuidance>
        <Flex direction="column" gap="2" mt="1">
          {terminology.map((row, index) => (
            <Flex key={row.rowId} gap="2" align="end" wrap="wrap">
              <Box style={{ flex: 1, minWidth: "10rem" }}>
                {index === 0 && (
                  <Text size="1" color="gray" as="div" mb="1">
                    Label
                  </Text>
                )}
                <TextField.Root
                  value={row.label}
                  placeholder="e.g. cvd_codes"
                  onChange={(e) => handleUpdateTerminology(row.rowId, { label: e.target.value })}
                  disabled={disabled}
                  aria-label={`Label for terminology ${index + 1}`}
                />
              </Box>
              <Box style={{ flex: 2, minWidth: "16rem" }}>
                {index === 0 && (
                  <Text size="1" color="gray" as="div" mb="1">
                    Canonical URL
                  </Text>
                )}
                <TextField.Root
                  value={row.referenceUrl}
                  placeholder="e.g. http://example.org/ValueSet/cvd"
                  onChange={(e) =>
                    handleUpdateTerminology(row.rowId, { referenceUrl: e.target.value })
                  }
                  disabled={disabled}
                  aria-label={`Canonical URL for terminology ${index + 1}`}
                />
              </Box>
              <IconButton
                size="2"
                variant="soft"
                color="gray"
                aria-label={`Remove terminology ${index + 1}`}
                onClick={() => handleRemoveTerminology(row.rowId)}
                disabled={disabled}
              >
                <TrashIcon />
              </IconButton>
            </Flex>
          ))}
        </Flex>
        <Box mt="2">
          <Button size="2" variant="soft" onClick={handleAddTerminology} disabled={disabled}>
            <PlusIcon />
            Add value set or concept map
          </Button>
        </Box>
      </Box>

      <Box>
        <FieldLabel mb="1" optional>
          Parameters
        </FieldLabel>
        {parameters.length === 0 && (
          <FieldGuidance>
            Declare the parameters the SQL binds, and the value to bind on this run. Each becomes a
            `Library.parameter` entry with `use=in`; values are never saved.
          </FieldGuidance>
        )}
        <Flex direction="column" gap="2" mt="1">
          {parameters.map((param, index) => {
            const declaredName = param.name.trim();
            // One name can only bind one value, so a name declared twice is
            // ambiguous and both rows carry the message.
            const duplicate = declaredName !== "" && duplicateNames.has(declaredName);
            return (
              <Flex key={param.rowId} gap="2" align="end" wrap="wrap">
                <Box style={{ flex: 1, minWidth: "9rem" }}>
                  {index === 0 && (
                    <Text size="1" color="gray" as="div" mb="1">
                      Name
                    </Text>
                  )}
                  <TextField.Root
                    value={param.name}
                    placeholder="e.g. patient_id"
                    onChange={(e) =>
                      handleUpdateParameter(param.rowId, {
                        name: e.target.value,
                      })
                    }
                    disabled={disabled}
                    aria-label={`Name for parameter ${index + 1}`}
                    color={duplicate ? "red" : undefined}
                  />
                  {duplicate && (
                    <Text size="1" color="red" as="div" mt="1">
                      {`Duplicate parameter name: ${declaredName}.`}
                    </Text>
                  )}
                </Box>
                <Box style={{ width: "9rem" }}>
                  {index === 0 && (
                    <Text size="1" color="gray" as="div" mb="1">
                      Type
                    </Text>
                  )}
                  <Select.Root
                    value={param.type}
                    onValueChange={(value) =>
                      handleUpdateParameter(param.rowId, {
                        type: value as SqlQueryParameterType,
                      })
                    }
                    disabled={disabled}
                  >
                    <Select.Trigger
                      style={{ width: "100%" }}
                      aria-label={`Type for parameter ${index + 1}`}
                    />
                    <Select.Content>
                      {PARAMETER_TYPES.map((type) => (
                        <Select.Item key={type} value={type}>
                          {type}
                        </Select.Item>
                      ))}
                    </Select.Content>
                  </Select.Root>
                </Box>
                <Box style={{ flex: 1, minWidth: "9rem" }}>
                  {index === 0 && (
                    <Text size="1" color="gray" as="div" mb="1">
                      Value
                    </Text>
                  )}
                  <ParameterValueInput
                    type={param.type}
                    value={param.value}
                    onChange={(value) => handleUpdateParameter(param.rowId, { value })}
                    disabled={disabled}
                    // An unnamed row declares nothing, so it binds nothing and
                    // its value is not required.
                    required={declaredName !== ""}
                    ariaLabel={`Value for parameter ${index + 1}`}
                  />
                </Box>
                <IconButton
                  size="2"
                  variant="soft"
                  color="gray"
                  aria-label={`Remove parameter ${index + 1}`}
                  onClick={() => handleRemoveParameter(param.rowId)}
                  disabled={disabled}
                >
                  <TrashIcon />
                </IconButton>
              </Flex>
            );
          })}
        </Flex>
        <Box mt="2">
          <Button size="2" variant="soft" onClick={handleAddParameter} disabled={disabled}>
            <PlusIcon />
            Add parameter
          </Button>
        </Box>
      </Box>
    </Flex>
  );
}

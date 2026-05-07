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
 * "Provide Library" tab body for the SQL query form: SQL editor, tables
 * editor and parameter declarations editor.
 *
 * @author John Grimes
 */

import { PlusIcon, TrashIcon } from "@radix-ui/react-icons";
import { Box, Button, Flex, IconButton, Select, Text, TextArea, TextField } from "@radix-ui/themes";

import { FieldGuidance } from "../FieldGuidance";
import { FieldLabel } from "../FieldLabel";

import type {
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

interface ViewDefinitionOption {
  id: string;
  name: string;
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
  /** Configured tables (related artefacts). */
  tables: SqlQueryRelatedArtifact[];
  /** Callback fired when the tables list changes. */
  onTablesChange: (tables: SqlQueryRelatedArtifact[]) => void;
  /** Configured declared parameters. */
  parameters: SqlQueryParameterDeclaration[];
  /** Callback fired when the parameters list changes. */
  onParametersChange: (parameters: SqlQueryParameterDeclaration[]) => void;
  /** Available stored ViewDefinitions for the table selector. */
  viewDefinitions: ViewDefinitionOption[];
  /** Whether the controls should be disabled. */
  disabled?: boolean;
}

/**
 * Renders the "Provide Library" tab body.
 *
 * @param props - The component props.
 * @param props.title - Library title (used for `Library.title` on save).
 * @param props.onTitleChange - Callback fired when the title changes.
 * @param props.sql - SQL text.
 * @param props.onSqlChange - Callback fired when the SQL changes.
 * @param props.tables - Configured tables (related artefacts).
 * @param props.onTablesChange - Callback fired when the tables list changes.
 * @param props.parameters - Configured declared parameters.
 * @param props.onParametersChange - Callback fired when the parameters list changes.
 * @param props.viewDefinitions - Available stored ViewDefinitions for the table selector.
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
  parameters,
  onParametersChange,
  viewDefinitions,
  disabled = false,
}: Readonly<SqlQueryInlineTabProps>) {
  const handleAddTable = () => {
    onTablesChange([
      ...tables,
      {
        rowId: crypto.randomUUID(),
        label: "",
        viewDefinitionId: "",
      },
    ]);
  };

  const handleRemoveTable = (rowId: string) => {
    onTablesChange(tables.filter((t) => t.rowId !== rowId));
  };

  const handleUpdateTable = (rowId: string, update: Partial<SqlQueryRelatedArtifact>) => {
    onTablesChange(tables.map((t) => (t.rowId === rowId ? { ...t, ...update } : t)));
  };

  const handleAddParameter = () => {
    onParametersChange([
      ...parameters,
      {
        rowId: crypto.randomUUID(),
        name: "",
        type: "string",
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
        <FieldLabel mb="1">Tables</FieldLabel>
        {tables.length === 0 && (
          <FieldGuidance>
            Add at least one table; each maps a label to a stored ViewDefinition.
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
                  aria-label={`Label for table ${index + 1}`}
                />
              </Box>
              <Box style={{ flex: 1, minWidth: "12rem" }}>
                {index === 0 && (
                  <Text size="1" color="gray" as="div" mb="1">
                    View definition
                  </Text>
                )}
                <Select.Root
                  value={table.viewDefinitionId === "" ? undefined : table.viewDefinitionId}
                  onValueChange={(value) =>
                    handleUpdateTable(table.rowId, {
                      viewDefinitionId: value,
                    })
                  }
                  disabled={disabled || viewDefinitions.length === 0}
                >
                  <Select.Trigger
                    style={{ width: "100%" }}
                    placeholder={
                      viewDefinitions.length === 0
                        ? "No view definitions"
                        : "Select view definition"
                    }
                    aria-label={`View definition for table ${index + 1}`}
                  />
                  <Select.Content>
                    {viewDefinitions.map((vd) => (
                      <Select.Item key={vd.id} value={vd.id}>
                        {vd.name}
                      </Select.Item>
                    ))}
                  </Select.Content>
                </Select.Root>
              </Box>
              <IconButton
                size="2"
                variant="soft"
                color="gray"
                aria-label={`Remove table ${index + 1}`}
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
            Add table
          </Button>
        </Box>
      </Box>

      <Box>
        <FieldLabel mb="1" optional>
          Parameters
        </FieldLabel>
        {parameters.length === 0 && (
          <FieldGuidance>
            Declare runtime parameters for the SQL. Each becomes a `Library.parameter` entry with
            `use=in`.
          </FieldGuidance>
        )}
        <Flex direction="column" gap="2" mt="1">
          {parameters.map((param, index) => (
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
                />
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
                    Default (optional)
                  </Text>
                )}
                <TextField.Root
                  value={param.defaultValue ?? ""}
                  placeholder="(none)"
                  onChange={(e) =>
                    handleUpdateParameter(param.rowId, {
                      defaultValue: e.target.value,
                    })
                  }
                  disabled={disabled}
                  aria-label={`Default value for parameter ${index + 1}`}
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
          ))}
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

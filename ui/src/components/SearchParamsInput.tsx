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
 * Reusable search parameter rows input component.
 *
 * @author John Grimes
 */

import { Cross2Icon, PlusIcon } from "@radix-ui/react-icons";
import { Badge, Box, Button, Flex, IconButton, Select, Text, TextField } from "@radix-ui/themes";

import { FieldGuidance } from "./FieldGuidance";

import type { SearchParamCapability } from "../hooks/useServerCapabilities";

/**
 * Represents a single search parameter row with a unique ID, parameter name,
 * and value.
 */
export interface SearchParamRowData {
  /** Unique identifier for the row. */
  id: number;
  /** The selected search parameter name. */
  paramName: string;
  /** The search parameter value. */
  value: string;
}

interface SearchParamsInputProps {
  /** Available search parameters to show in the dropdown. */
  availableParams: SearchParamCapability[];
  /** Current row state. */
  rows: SearchParamRowData[];
  /** Callback when rows change (add, remove, or edit). */
  onChange: (rows: SearchParamRowData[]) => void;
  /** Optional keyboard event handler to attach to value inputs. */
  onKeyDown?: (e: React.KeyboardEvent) => void;
}

/** Counter shared across all instances for generating unique row IDs. */
let nextId = 1000;

/**
 * Generates a unique ID for a new search parameter row.
 *
 * @returns A unique numeric ID.
 */
export function generateRowId(): number {
  return nextId++;
}

/**
 * Creates a new empty search parameter row.
 *
 * @returns A new row with an empty parameter name and value.
 */
export function createEmptyRow(): SearchParamRowData {
  return { id: generateRowId(), paramName: "", value: "" };
}

/**
 * Stateless component for rendering search parameter name-value rows with
 * add/remove controls.
 *
 * @param props - The component props.
 * @param props.availableParams - Available parameters for the dropdown.
 * @param props.rows - Current row state.
 * @param props.onChange - Callback when rows change.
 * @param props.onKeyDown - Optional keyboard event handler for value inputs.
 * @returns The search parameters input component.
 */
export function SearchParamsInput({
  availableParams,
  rows,
  onChange,
  onKeyDown,
}: Readonly<SearchParamsInputProps>) {
  const addRow = () => {
    onChange([...rows, createEmptyRow()]);
  };

  const removeRow = (id: number) => {
    if (rows.length > 1) {
      onChange(rows.filter((row) => row.id !== id));
    }
  };

  const updateRow = (id: number, field: "paramName" | "value", val: string) => {
    onChange(rows.map((row) => (row.id === id ? { ...row, [field]: val } : row)));
  };

  return (
    <Box>
      <Flex justify="between" align="center" mb="2">
        <Text as="label" size="2" weight="medium">
          Search parameters
        </Text>
        <Button size="1" variant="soft" onClick={addRow}>
          <PlusIcon />
          Add parameter
        </Button>
      </Flex>
      <Flex direction="column" gap="2">
        {rows.map((row) => (
          <Flex key={row.id} gap="2" align="center">
            <Box style={{ flex: 1 }}>
              <Select.Root
                value={row.paramName}
                onValueChange={(val) => updateRow(row.id, "paramName", val)}
              >
                <Select.Trigger placeholder="Select parameter..." style={{ width: "100%" }} />
                <Select.Content>
                  {availableParams.map((param) => (
                    <Select.Item key={param.name} value={param.name}>
                      <Flex gap="2" align="center">
                        {param.name}
                        <Badge size="1" variant="surface">
                          {param.type}
                        </Badge>
                      </Flex>
                    </Select.Item>
                  ))}
                </Select.Content>
              </Select.Root>
            </Box>
            <Box style={{ flex: 1 }}>
              <TextField.Root
                placeholder="e.g., male"
                value={row.value}
                onChange={(e) => updateRow(row.id, "value", e.target.value)}
                onKeyDown={onKeyDown}
                style={{ minWidth: "100px" }}
              />
            </Box>
            <IconButton
              size="2"
              variant="soft"
              color="red"
              onClick={() => removeRow(row.id)}
              disabled={rows.length === 1}
            >
              <Cross2Icon />
            </IconButton>
          </Flex>
        ))}
      </Flex>
      <FieldGuidance>
        Search parameters use standard FHIR search syntax and are combined with AND logic.
      </FieldGuidance>
    </Box>
  );
}

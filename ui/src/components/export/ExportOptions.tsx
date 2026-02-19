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
 * Reusable export options form fields for configuring bulk export parameters.
 *
 * @author John Grimes
 */

import { Cross2Icon, PlusIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, IconButton, Select, TextField } from "@radix-ui/themes";

import { ResourceTypePicker } from "./ResourceTypePicker";
import { OUTPUT_FORMATS } from "../../types/exportOptions";
import { FieldGuidance } from "../FieldGuidance";
import { FieldLabel } from "../FieldLabel";
import { SearchParamsInput, createEmptyRow } from "../SearchParamsInput";

import type { SearchParamCapability } from "../../hooks/useServerCapabilities";
import type { ExportOptionsValues, TypeFilterState } from "../../types/exportOptions";

interface ExportOptionsProps {
  /** Available resource types for selection. */
  resourceTypes: string[];
  /** Current export options values. */
  values: ExportOptionsValues;
  /** Callback when any option value changes. */
  onChange: (values: ExportOptionsValues) => void;
  /** Whether to hide the outputFormat field. */
  hideOutputFormat?: boolean;
  /** Search parameters per resource type from the CapabilityStatement. */
  searchParams?: Record<string, SearchParamCapability[]>;
}

/**
 * Form fields for configuring bulk export options including resource types,
 * date filters, elements, output format, and type filters.
 *
 * @param props - The component props.
 * @param props.resourceTypes - Available resource types for selection.
 * @param props.values - Current export options values.
 * @param props.onChange - Callback when any option value changes.
 * @param props.hideOutputFormat - Whether to hide the outputFormat field.
 * @param props.searchParams - Search parameters per resource type.
 * @returns The export options component.
 */
export function ExportOptions({
  resourceTypes,
  values,
  onChange,
  hideOutputFormat = false,
  searchParams,
}: Readonly<ExportOptionsProps>) {
  /**
   * Updates a single option value.
   *
   * @param key - The option key to update.
   * @param value - The new value.
   */
  const updateOption = <K extends keyof ExportOptionsValues>(
    key: K,
    value: ExportOptionsValues[K],
  ) => {
    onChange({ ...values, [key]: value });
  };

  /**
   * Updates the type filters list.
   *
   * @param updater - Function that produces the new type filters array.
   */
  const updateTypeFilters = (updater: (prev: TypeFilterState[]) => TypeFilterState[]) => {
    updateOption("typeFilters", updater(values.typeFilters));
  };

  const addTypeFilter = () => {
    updateTypeFilters((prev) => [
      ...prev,
      { id: crypto.randomUUID(), resourceType: "", rows: [createEmptyRow()] },
    ]);
  };

  const removeTypeFilter = (id: string) => {
    updateTypeFilters((prev) => prev.filter((entry) => entry.id !== id));
  };

  const updateTypeFilterResourceType = (id: string, resourceType: string) => {
    updateTypeFilters((prev) =>
      prev.map((entry) =>
        entry.id === id ? { ...entry, resourceType, rows: [createEmptyRow()] } : entry,
      ),
    );
  };

  const updateTypeFilterRows = (id: string, rows: TypeFilterState["rows"]) => {
    updateTypeFilters((prev) =>
      prev.map((entry) => (entry.id === id ? { ...entry, rows } : entry)),
    );
  };

  return (
    <Flex direction="column" gap="4">
      <ResourceTypePicker
        resourceTypes={resourceTypes}
        selectedTypes={values.types}
        onSelectedTypesChange={(types) => updateOption("types", types)}
      />

      <Flex gap="4" wrap="wrap">
        <Box style={{ flex: 1 }}>
          <FieldLabel>Since</FieldLabel>
          <TextField.Root
            type="datetime-local"
            value={values.since}
            onChange={(e) => updateOption("since", e.target.value)}
          />
          <FieldGuidance>Only resources updated after this time.</FieldGuidance>
        </Box>
        <Box style={{ flex: 1 }}>
          <FieldLabel>Until</FieldLabel>
          <TextField.Root
            type="datetime-local"
            value={values.until}
            onChange={(e) => updateOption("until", e.target.value)}
          />
          <FieldGuidance>Only resources updated before this time.</FieldGuidance>
        </Box>
      </Flex>

      <Box>
        <FieldLabel optional>Elements</FieldLabel>
        <TextField.Root
          placeholder="e.g., id,meta,name"
          value={values.elements}
          onChange={(e) => updateOption("elements", e.target.value)}
        />
        <FieldGuidance>Comma-separated list of element names to include.</FieldGuidance>
      </Box>

      {!hideOutputFormat && (
        <Box>
          <FieldLabel>Output format</FieldLabel>
          <Select.Root
            value={values.outputFormat}
            onValueChange={(value) => updateOption("outputFormat", value)}
          >
            <Select.Trigger style={{ width: "100%" }} placeholder="Default (NDJSON)" />
            <Select.Content>
              {OUTPUT_FORMATS.map((format) => (
                <Select.Item key={format.value} value={format.value}>
                  {format.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
          <FieldGuidance>Output format for the export data.</FieldGuidance>
        </Box>
      )}

      <Box>
        <Flex justify="between" align="center" mb="2">
          <FieldLabel mb="0">Type filters</FieldLabel>
          <Button size="1" variant="soft" onClick={addTypeFilter}>
            <PlusIcon />
            Add type filter
          </Button>
        </Flex>
        {values.typeFilters.length > 0 ? (
          <Flex direction="column" gap="3">
            {values.typeFilters.map((entry) => (
              <Card key={entry.id} variant="surface">
                <Flex direction="column" gap="3">
                  <Flex justify="between" align="center">
                    <Box style={{ flex: 1 }}>
                      <Select.Root
                        value={entry.resourceType}
                        onValueChange={(value) => updateTypeFilterResourceType(entry.id, value)}
                      >
                        <Select.Trigger
                          placeholder="Select resource type..."
                          style={{ width: "100%" }}
                        />
                        <Select.Content>
                          {resourceTypes.map((rt) => (
                            <Select.Item key={rt} value={rt}>
                              {rt}
                            </Select.Item>
                          ))}
                        </Select.Content>
                      </Select.Root>
                    </Box>
                    <IconButton
                      size="2"
                      variant="soft"
                      color="red"
                      ml="2"
                      onClick={() => removeTypeFilter(entry.id)}
                    >
                      <Cross2Icon />
                    </IconButton>
                  </Flex>
                  {entry.resourceType && (
                    <SearchParamsInput
                      availableParams={searchParams?.[entry.resourceType] ?? []}
                      rows={entry.rows}
                      onChange={(rows) => updateTypeFilterRows(entry.id, rows)}
                    />
                  )}
                </Flex>
              </Card>
            ))}
          </Flex>
        ) : (
          <FieldGuidance mt="0">
            No type filters configured. Type filters allow filtering exported resources using FHIR
            search parameters.
          </FieldGuidance>
        )}
      </Box>
    </Flex>
  );
}

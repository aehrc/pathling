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
 * Form for configuring and starting a bulk export job.
 *
 * @author John Grimes
 */

import { Cross2Icon, PlayIcon, PlusIcon } from "@radix-ui/react-icons";
import {
  Box,
  Button,
  Card,
  Flex,
  Heading,
  IconButton,
  Select,
  Text,
  TextField,
} from "@radix-ui/themes";
import { useState } from "react";

import { ExportOptions } from "./ExportOptions";
import { DEFAULT_EXPORT_OPTIONS } from "../../types/exportOptions";
import { SearchParamsInput, createEmptyRow } from "../SearchParamsInput";

import type { SearchParamCapability } from "../../hooks/useServerCapabilities";
import type { ExportLevel, ExportRequest, TypeFilterEntry } from "../../types/export";
import type { ExportOptionsValues } from "../../types/exportOptions";
import type { SearchParamRowData } from "../SearchParamsInput";

interface ExportFormProps {
  onSubmit: (request: ExportRequest) => void;
  resourceTypes: string[];
  /** Search parameters per resource type from the CapabilityStatement. */
  searchParams?: Record<string, SearchParamCapability[]>;
}

/** Internal state for a single type filter entry. */
interface TypeFilterState {
  id: string;
  resourceType: string;
  rows: SearchParamRowData[];
}

const EXPORT_LEVELS: { value: ExportLevel; label: string }[] = [
  { value: "system", label: "All data in system" },
  { value: "all-patients", label: "All patient data" },
  { value: "patient", label: "Data for single patient" },
  { value: "group", label: "Data for patients in group" },
];

/**
 * Serialises type filter entries into the format expected by the ExportRequest.
 * Entries with no resource type selected or no non-empty rows are excluded.
 *
 * @param entries - The internal type filter state entries.
 * @returns Array of TypeFilterEntry objects, or undefined if empty.
 */
function serialiseTypeFilters(entries: TypeFilterState[]): TypeFilterEntry[] | undefined {
  const result: TypeFilterEntry[] = [];
  for (const entry of entries) {
    if (!entry.resourceType) continue;
    const params: Record<string, string[]> = {};
    for (const row of entry.rows) {
      if (row.paramName && row.value) {
        if (!params[row.paramName]) {
          params[row.paramName] = [];
        }
        params[row.paramName].push(row.value);
      }
    }
    result.push({ resourceType: entry.resourceType, params });
  }
  return result.length > 0 ? result : undefined;
}

/**
 * Form for configuring and starting a bulk data export.
 *
 * @param root0 - The component props.
 * @param root0.onSubmit - Callback when export is submitted.
 * @param root0.resourceTypes - Available resource types for selection.
 * @param root0.searchParams - Search parameters per resource type.
 * @returns The export form component.
 */
export function ExportForm({ onSubmit, resourceTypes, searchParams }: Readonly<ExportFormProps>) {
  const [level, setLevel] = useState<ExportLevel>("system");
  const [exportOptions, setExportOptions] = useState<ExportOptionsValues>(DEFAULT_EXPORT_OPTIONS);
  const [patientId, setPatientId] = useState("");
  const [groupId, setGroupId] = useState("");
  const [typeFilters, setTypeFilters] = useState<TypeFilterState[]>([]);

  const addTypeFilter = () => {
    setTypeFilters((prev) => [
      ...prev,
      { id: crypto.randomUUID(), resourceType: "", rows: [createEmptyRow()] },
    ]);
  };

  const removeTypeFilter = (id: string) => {
    setTypeFilters((prev) => prev.filter((entry) => entry.id !== id));
  };

  const updateTypeFilterResourceType = (id: string, resourceType: string) => {
    setTypeFilters((prev) =>
      prev.map((entry) =>
        entry.id === id ? { ...entry, resourceType, rows: [createEmptyRow()] } : entry,
      ),
    );
  };

  const updateTypeFilterRows = (id: string, rows: SearchParamRowData[]) => {
    setTypeFilters((prev) => prev.map((entry) => (entry.id === id ? { ...entry, rows } : entry)));
  };

  const handleSubmit = () => {
    const request: ExportRequest = {
      level,
      resourceTypes: exportOptions.types.length > 0 ? exportOptions.types : undefined,
      since: exportOptions.since || undefined,
      until: exportOptions.until || undefined,
      elements: exportOptions.elements || undefined,
      patientId: level === "patient" ? patientId : undefined,
      groupId: level === "group" ? groupId : undefined,
      outputFormat: exportOptions.outputFormat || undefined,
      typeFilters: serialiseTypeFilters(typeFilters),
    };
    onSubmit(request);
  };

  return (
    <Card>
      <Flex direction="column" gap="4">
        <Heading size="4">New export</Heading>

        <Box>
          <Text as="label" size="2" weight="medium" mb="1">
            Export level
          </Text>
          <Select.Root value={level} onValueChange={(value) => setLevel(value as ExportLevel)}>
            <Select.Trigger style={{ width: "100%" }} />
            <Select.Content>
              {EXPORT_LEVELS.map((l) => (
                <Select.Item key={l.value} value={l.value}>
                  {l.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        </Box>

        {level === "patient" && (
          <Box>
            <Text as="label" size="2" weight="medium" mb="1">
              Patient ID
            </Text>
            <TextField.Root
              placeholder="e.g., patient-123"
              value={patientId}
              onChange={(e) => setPatientId(e.target.value)}
            />
          </Box>
        )}

        {level === "group" && (
          <Box>
            <Text as="label" size="2" weight="medium" mb="1">
              Group ID
            </Text>
            <TextField.Root
              placeholder="e.g., group-456"
              value={groupId}
              onChange={(e) => setGroupId(e.target.value)}
            />
          </Box>
        )}

        <ExportOptions
          resourceTypes={resourceTypes}
          values={exportOptions}
          onChange={setExportOptions}
        />

        <Box>
          <Flex justify="between" align="center" mb="2">
            <Text as="label" size="2" weight="medium">
              Type filters
            </Text>
            <Button size="1" variant="soft" onClick={addTypeFilter}>
              <PlusIcon />
              Add type filter
            </Button>
          </Flex>
          {typeFilters.length > 0 ? (
            <Flex direction="column" gap="3">
              {typeFilters.map((entry) => (
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
            <Text size="1" color="gray">
              No type filters configured. Type filters allow filtering exported resources using FHIR
              search parameters.
            </Text>
          )}
        </Box>

        <Button
          size="3"
          onClick={handleSubmit}
          disabled={(level === "patient" && !patientId) || (level === "group" && !groupId)}
        >
          <PlayIcon />
          Start export
        </Button>
      </Flex>
    </Card>
  );
}

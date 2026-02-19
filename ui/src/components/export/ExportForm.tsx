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

import { PlayIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, Heading, Select, Text, TextField } from "@radix-ui/themes";
import { useState } from "react";

import { ExportOptions } from "./ExportOptions";
import { DEFAULT_EXPORT_OPTIONS, serialiseTypeFilterState } from "../../types/exportOptions";

import type { SearchParamCapability } from "../../hooks/useServerCapabilities";
import type { ExportLevel, ExportRequest } from "../../types/export";
import type { ExportOptionsValues } from "../../types/exportOptions";

interface ExportFormProps {
  onSubmit: (request: ExportRequest) => void;
  resourceTypes: string[];
  /** Search parameters per resource type from the CapabilityStatement. */
  searchParams?: Record<string, SearchParamCapability[]>;
}

const EXPORT_LEVELS: { value: ExportLevel; label: string }[] = [
  { value: "system", label: "All data in system" },
  { value: "all-patients", label: "All patient data" },
  { value: "patient", label: "Data for single patient" },
  { value: "group", label: "Data for patients in group" },
];

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
      typeFilters: serialiseTypeFilterState(exportOptions.typeFilters),
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
          searchParams={searchParams}
        />

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

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
 * Form for configuring and starting a ping and pull import job.
 *
 * @author John Grimes
 */

import * as Collapsible from "@radix-ui/react-collapsible";
import { ChevronDownIcon, ChevronRightIcon, UploadIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, Heading, Select, Text, TextField } from "@radix-ui/themes";
import { useState } from "react";

import { SaveModeField } from "./SaveModeField";
import { serialiseTypeFilters } from "../../types/export";
import { DEFAULT_EXPORT_OPTIONS, serialiseTypeFilterState } from "../../types/exportOptions";
import { IMPORT_FORMATS } from "../../types/import";
import { ExportOptions } from "../export/ExportOptions";
import { FieldGuidance } from "../FieldGuidance";

import type { SearchParamCapability } from "../../hooks/useServerCapabilities";
import type { ExportOptionsValues } from "../../types/exportOptions";
import type { ImportFormat, SaveMode } from "../../types/import";
import type { ExportType, ImportPnpRequest } from "../../types/importPnp";

interface ImportPnpFormProps {
  onSubmit: (request: ImportPnpRequest) => void;
  isSubmitting: boolean;
  disabled: boolean;
  resourceTypes: string[];
  /** Search parameters per resource type from the CapabilityStatement. */
  searchParams?: Record<string, SearchParamCapability[]>;
}

/**
 * Form for configuring and starting a ping-and-pull import.
 *
 * @param root0 - The component props.
 * @param root0.onSubmit - Callback when import is submitted.
 * @param root0.isSubmitting - Whether an import is in progress.
 * @param root0.disabled - Whether the form is disabled.
 * @param root0.resourceTypes - Available resource types for selection.
 * @param root0.searchParams - Search parameters per resource type.
 * @returns The import PnP form component.
 */
export function ImportPnpForm({
  onSubmit,
  isSubmitting,
  disabled,
  resourceTypes,
  searchParams,
}: Readonly<ImportPnpFormProps>) {
  const [exportUrl, setExportUrl] = useState("");
  const [saveMode, setSaveMode] = useState<SaveMode>("overwrite");
  const [inputFormat, setInputFormat] = useState<ImportFormat>("application/fhir+ndjson");
  const [exportOptions, setExportOptions] = useState<ExportOptionsValues>(DEFAULT_EXPORT_OPTIONS);
  const [exportOptionsOpen, setExportOptionsOpen] = useState(false);
  const exportType: ExportType = "dynamic";

  const handleSubmit = () => {
    // Serialise type filter state into TypeFilterEntry[], then into strings.
    const typeFilterEntries = serialiseTypeFilterState(exportOptions.typeFilters);
    const typeFilterStrings = serialiseTypeFilters(typeFilterEntries);

    const request: ImportPnpRequest = {
      exportUrl,
      exportType,
      saveMode,
      inputFormat,
      // Export passthrough options.
      types: exportOptions.types.length > 0 ? exportOptions.types : undefined,
      since: exportOptions.since || undefined,
      until: exportOptions.until || undefined,
      elements: exportOptions.elements || undefined,
      typeFilters: typeFilterStrings,
    };
    onSubmit(request);
  };

  const isValid = exportUrl.trim() !== "";

  return (
    <Card>
      <Flex direction="column" gap="4">
        <Heading size="4">New import</Heading>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Input format
            </Text>
          </Box>
          <Select.Root
            value={inputFormat}
            onValueChange={(value) => setInputFormat(value as ImportFormat)}
          >
            <Select.Trigger style={{ width: "100%" }} />
            <Select.Content>
              {IMPORT_FORMATS.map((format) => (
                <Select.Item key={format.value} value={format.value}>
                  {format.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        </Box>

        <Box>
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Export URL
            </Text>
          </Box>
          <TextField.Root
            placeholder="e.g., https://example.org/fhir/$export"
            value={exportUrl}
            onChange={(e) => setExportUrl(e.target.value)}
          />
          <FieldGuidance>The bulk export endpoint URL of the remote FHIR server.</FieldGuidance>
        </Box>

        <SaveModeField value={saveMode} onChange={setSaveMode} />

        <Collapsible.Root open={exportOptionsOpen} onOpenChange={setExportOptionsOpen}>
          <Collapsible.Trigger asChild>
            <Flex align="center" gap="2" style={{ cursor: "pointer" }} py="2">
              {exportOptionsOpen ? <ChevronDownIcon /> : <ChevronRightIcon />}
              <Text size="2" weight="medium">
                Export options
              </Text>
            </Flex>
          </Collapsible.Trigger>

          <Collapsible.Content>
            <Box pt="2">
              <ExportOptions
                resourceTypes={resourceTypes}
                values={exportOptions}
                onChange={setExportOptions}
                hideOutputFormat
                searchParams={searchParams}
              />
            </Box>
          </Collapsible.Content>
        </Collapsible.Root>

        <Button size="3" onClick={handleSubmit} disabled={disabled || isSubmitting || !isValid}>
          <UploadIcon />
          {isSubmitting ? "Starting import..." : "Start import"}
        </Button>
      </Flex>
    </Card>
  );
}

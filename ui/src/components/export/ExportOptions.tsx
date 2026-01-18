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

import { Box, Flex, Select, Text, TextField } from "@radix-ui/themes";

import { ResourceTypePicker } from "./ResourceTypePicker";
import { OUTPUT_FORMATS } from "../../types/exportOptions";

import type { ExportOptionsValues } from "../../types/exportOptions";


interface ExportOptionsProps {
  /** Available resource types for selection. */
  resourceTypes: string[];
  /** Current export options values. */
  values: ExportOptionsValues;
  /** Callback when any option value changes. */
  onChange: (values: ExportOptionsValues) => void;
  /** Whether to show extended options (outputFormat, typeFilters, includeAssociatedData). */
  showExtendedOptions?: boolean;
  /** Whether to hide the outputFormat field. */
  hideOutputFormat?: boolean;
}

/**
 * Form fields for configuring bulk export options including resource types,
 * date filters, elements, and advanced options.
 *
 * @param props - The component props.
 * @param props.resourceTypes - Available resource types for selection.
 * @param props.values - Current export options values.
 * @param props.onChange - Callback when any option value changes.
 * @param props.showExtendedOptions - Whether to show extended options.
 * @param props.hideOutputFormat - Whether to hide the outputFormat field.
 * @returns The export options component.
 */
export function ExportOptions({
  resourceTypes,
  values,
  onChange,
  showExtendedOptions = false,
  hideOutputFormat = false,
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

  return (
    <Flex direction="column" gap="4">
      <ResourceTypePicker
        resourceTypes={resourceTypes}
        selectedTypes={values.types}
        onSelectedTypesChange={(types) => updateOption("types", types)}
      />

      <Flex gap="4" wrap="wrap">
        <Box style={{ flex: 1 }}>
          <Text as="label" size="2" weight="medium" mb="1">
            Since
          </Text>
          <TextField.Root
            type="datetime-local"
            value={values.since}
            onChange={(e) => updateOption("since", e.target.value)}
          />
          <Text size="1" color="gray" mt="1">
            Only resources updated after this time.
          </Text>
        </Box>
        <Box style={{ flex: 1 }}>
          <Text as="label" size="2" weight="medium" mb="1">
            Until
          </Text>
          <TextField.Root
            type="datetime-local"
            value={values.until}
            onChange={(e) => updateOption("until", e.target.value)}
          />
          <Text size="1" color="gray" mt="1">
            Only resources updated before this time.
          </Text>
        </Box>
      </Flex>

      <Box>
        <Text as="label" size="2" weight="medium" mb="1">
          Elements (optional)
        </Text>
        <TextField.Root
          placeholder="e.g., id,meta,name"
          value={values.elements}
          onChange={(e) => updateOption("elements", e.target.value)}
        />
        <Text size="1" color="gray" mt="1">
          Comma-separated list of element names to include.
        </Text>
      </Box>

      {!hideOutputFormat && (
        <Box>
          <Text as="label" size="2" weight="medium" mb="1">
            Output format
          </Text>
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
          <Text size="1" color="gray" mt="1">
            Output format for the export data.
          </Text>
        </Box>
      )}

      {showExtendedOptions && (
        <>
          <Box>
            <Text as="label" size="2" weight="medium" mb="1">
              Type filters (optional)
            </Text>
            <TextField.Root
              placeholder="e.g., Patient?active=true,Observation?status=final"
              value={values.typeFilters}
              onChange={(e) => updateOption("typeFilters", e.target.value)}
            />
            <Text size="1" color="gray" mt="1">
              Comma-separated FHIR search queries to filter resources.
            </Text>
          </Box>

          <Box>
            <Text as="label" size="2" weight="medium" mb="1">
              Include associated data (optional)
            </Text>
            <TextField.Root
              placeholder="e.g., LatestProvenanceResources,RelevantProvenanceResources"
              value={values.includeAssociatedData}
              onChange={(e) => updateOption("includeAssociatedData", e.target.value)}
            />
            <Text size="1" color="gray" mt="1">
              Comma-separated list of pre-defined associated data sets.
            </Text>
          </Box>
        </>
      )}
    </Flex>
  );
}

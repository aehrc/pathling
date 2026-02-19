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
 * Reusable resource type selection picker for export forms.
 *
 * @author John Grimes
 */

import { Box, CheckboxCards, Flex, ScrollArea, Text } from "@radix-ui/themes";

import { FieldGuidance } from "../FieldGuidance";

interface ResourceTypePickerProps {
  resourceTypes: string[];
  selectedTypes: string[];
  onSelectedTypesChange: (types: string[]) => void;
}

/**
 * Renders a checkbox card selection for choosing FHIR resource types to export.
 *
 * @param props - The component props.
 * @param props.resourceTypes - The available resource types to choose from.
 * @param props.selectedTypes - The currently selected resource types.
 * @param props.onSelectedTypesChange - Callback when the selection changes.
 * @returns The resource type picker component.
 */
export function ResourceTypePicker({
  resourceTypes,
  selectedTypes,
  onSelectedTypesChange,
}: Readonly<ResourceTypePickerProps>) {
  const clearAllTypes = () => {
    onSelectedTypesChange([]);
  };

  return (
    <Box>
      <Flex justify="between" align="center" mb="2">
        <Flex gap="2" align="baseline">
          <Text as="label" size="2" weight="medium">
            Resource types
          </Text>
          <FieldGuidance mt="0">(leave empty to export all)</FieldGuidance>
        </Flex>
        <Text size="1" color="teal" style={{ cursor: "pointer" }} onClick={clearAllTypes}>
          Clear
        </Text>
      </Flex>
      <ScrollArea
        style={{
          maxHeight: 200,
          border: "1px solid var(--gray-5)",
          borderRadius: "var(--radius-2)",
        }}
      >
        <Box p="2">
          <CheckboxCards.Root
            size="1"
            value={selectedTypes}
            onValueChange={onSelectedTypesChange}
            gap="2"
            style={{ display: "flex", flexWrap: "wrap" }}
          >
            {resourceTypes.map((type) => (
              <CheckboxCards.Item key={type} value={type}>
                <Text size="1">{type}</Text>
              </CheckboxCards.Item>
            ))}
          </CheckboxCards.Root>
        </Box>
      </ScrollArea>
    </Box>
  );
}

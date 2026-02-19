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
 * Reusable save mode selection field for import forms.
 *
 * @author John Grimes
 */

import { Box, Flex, RadioCards, Text } from "@radix-ui/themes";

import { SAVE_MODES } from "../../types/import";
import { FieldLabel } from "../FieldLabel";

import type { SaveMode } from "../../types/import";

interface SaveModeFieldProps {
  value: SaveMode;
  onChange: (mode: SaveMode) => void;
}

/**
 * Renders a radio card selection for import save modes.
 *
 * @param props - The component props.
 * @param props.value - The currently selected save mode.
 * @param props.onChange - Callback when the save mode changes.
 * @returns The save mode field component.
 */
export function SaveModeField({ value, onChange }: SaveModeFieldProps) {
  return (
    <Box>
      <FieldLabel mb="2">Save mode</FieldLabel>
      <RadioCards.Root
        value={value}
        onValueChange={(v) => onChange(v as SaveMode)}
        columns="2"
        gap="2"
      >
        {SAVE_MODES.map((option) => (
          <RadioCards.Item value={option.value} key={option.value}>
            <Flex direction="column" width="100%">
              <Text weight="medium">{option.label}</Text>
              <Text size="1" color="gray">
                {option.description}
              </Text>
            </Flex>
          </RadioCards.Item>
        ))}
      </RadioCards.Root>
    </Box>
  );
}

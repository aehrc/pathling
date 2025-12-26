/**
 * Reusable save mode selection field for import forms.
 *
 * @author John Grimes
 */

import { Box, Flex, RadioCards, Text } from "@radix-ui/themes";
import type { SaveMode } from "../../types/import";
import { SAVE_MODES } from "../../types/import";

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
      <Box mb="2">
        <Text as="label" size="2" weight="medium">
          Save mode
        </Text>
      </Box>
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

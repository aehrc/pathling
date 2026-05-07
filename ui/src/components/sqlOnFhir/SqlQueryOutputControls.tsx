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
 * Output format, row-limit and CSV-header controls for the SQL query
 * form.
 *
 * @author John Grimes
 */

import { Box, Flex, Select, Switch, TextField } from "@radix-ui/themes";

import { FieldGuidance } from "../FieldGuidance";
import { FieldLabel } from "../FieldLabel";

import type { SqlQueryOutputFormat } from "../../types/sqlQuery";

interface SqlQueryOutputControlsProps {
  /** The currently selected output format. */
  format: SqlQueryOutputFormat;
  /** Callback fired when the format changes. */
  onFormatChange: (format: SqlQueryOutputFormat) => void;
  /** Row limit captured as a string so empty input is preserved. */
  limit: string;
  /** Callback fired when the limit changes. */
  onLimitChange: (limit: string) => void;
  /** Whether the CSV header switch is on. */
  header: boolean;
  /** Callback fired when the header switch toggles. */
  onHeaderChange: (header: boolean) => void;
  /** Whether the controls should be disabled (e.g. while executing). */
  disabled?: boolean;
}

const OUTPUT_FORMATS: SqlQueryOutputFormat[] = ["ndjson", "csv", "json", "parquet", "fhir"];

/**
 * Renders the output format select, row-limit input and (for CSV) the
 * header switch.
 *
 * @param props - The component props.
 * @param props.format - The currently selected output format.
 * @param props.onFormatChange - Callback fired when the format changes.
 * @param props.limit - Row limit captured as a string so empty input is preserved.
 * @param props.onLimitChange - Callback fired when the limit changes.
 * @param props.header - Whether the CSV header switch is on.
 * @param props.onHeaderChange - Callback fired when the header switch toggles.
 * @param props.disabled - Whether the controls should be disabled.
 * @returns The output controls.
 */
export function SqlQueryOutputControls({
  format,
  onFormatChange,
  limit,
  onLimitChange,
  header,
  onHeaderChange,
  disabled = false,
}: Readonly<SqlQueryOutputControlsProps>) {
  return (
    <Flex direction="column" gap="3">
      <Flex gap="3" wrap="wrap" align="end">
        <Box style={{ minWidth: "12rem" }}>
          <FieldLabel mb="1">Output format</FieldLabel>
          <Select.Root
            value={format}
            onValueChange={(value) => onFormatChange(value as SqlQueryOutputFormat)}
            disabled={disabled}
          >
            <Select.Trigger aria-label="Output format" style={{ width: "100%" }} />
            <Select.Content>
              {OUTPUT_FORMATS.map((f) => (
                <Select.Item key={f} value={f}>
                  {f}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        </Box>
        <Box style={{ width: "8rem" }}>
          <FieldLabel mb="1" optional>
            Row limit
          </FieldLabel>
          <TextField.Root
            inputMode="numeric"
            placeholder="(none)"
            value={limit}
            onChange={(e) => onLimitChange(e.target.value)}
            disabled={disabled}
            aria-label="Row limit"
          />
        </Box>
        {format === "csv" && (
          <Box>
            <FieldLabel mb="1">CSV header row</FieldLabel>
            <Flex align="center" gap="2">
              <Switch
                checked={header}
                onCheckedChange={onHeaderChange}
                disabled={disabled}
                aria-label="Include CSV header row"
              />
            </Flex>
          </Box>
        )}
      </Flex>
      <FieldGuidance>
        Output format is one of csv, ndjson, json, parquet or fhir. The CSV header switch is shown
        only for csv.
      </FieldGuidance>
    </Flex>
  );
}

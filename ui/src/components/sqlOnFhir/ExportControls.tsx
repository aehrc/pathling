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
 * Component for view export controls (format selection and export button).
 *
 * @author John Grimes
 */

import { DownloadIcon } from "@radix-ui/react-icons";
import { Button, Flex, Select } from "@radix-ui/themes";
import { useState } from "react";

import type { ViewExportFormat } from "../../types/viewExport";

interface ExportControlsProps {
  onExport: (format: ViewExportFormat) => void;
  disabled?: boolean;
}

const FORMAT_OPTIONS: { value: ViewExportFormat; label: string }[] = [
  { value: "ndjson", label: "NDJSON" },
  { value: "csv", label: "CSV" },
  { value: "parquet", label: "Parquet" },
];

/**
 * Controls for selecting export format and triggering export.
 *
 * @param root0 - The component props.
 * @param root0.onExport - Callback when export is triggered.
 * @param root0.disabled - Whether the controls are disabled.
 * @returns The export controls component.
 */
export function ExportControls({ onExport, disabled }: ExportControlsProps) {
  const [format, setFormat] = useState<ViewExportFormat>("ndjson");

  return (
    <Flex gap="2" align="center">
      <Select.Root
        value={format}
        onValueChange={(value) => setFormat(value as ViewExportFormat)}
        size="1"
        disabled={disabled}
      >
        <Select.Trigger />
        <Select.Content>
          {FORMAT_OPTIONS.map((option) => (
            <Select.Item key={option.value} value={option.value}>
              {option.label}
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Root>
      <Button size="1" variant="soft" onClick={() => onExport(format)} disabled={disabled}>
        <DownloadIcon />
        Export
      </Button>
    </Flex>
  );
}

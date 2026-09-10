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
 * Shared, height-bounded SQL display.
 *
 * Renders SQL text in a read-only, scrollable text area with an overlaid copy
 * control. Used both for previewing the SQL of a stored query or view in the
 * form and for echoing the submitted SQL in a result card, so the two surfaces
 * stay visually consistent and neither grows unbounded with long queries.
 *
 * @author John Grimes
 */

import { CopyIcon } from "@radix-ui/react-icons";
import { Box, IconButton, TextArea, Tooltip } from "@radix-ui/themes";

import { useClipboard } from "../../hooks";

interface SqlPreviewProps {
  /** The SQL text to display. */
  sql: string;
  /** Accessible label for the read-only text area. */
  ariaLabel: string;
  /** Number of visible text rows before the area scrolls. Defaults to 10. */
  rows?: number;
}

/**
 * Renders SQL in a read-only, height-bounded text area with a copy control.
 *
 * @param props - The component props.
 * @param props.sql - The SQL text to display.
 * @param props.ariaLabel - Accessible label for the read-only text area.
 * @param props.rows - Number of visible text rows before scrolling. Defaults to 10.
 * @returns The SQL preview element.
 */
export function SqlPreview({ sql, ariaLabel, rows = 10 }: Readonly<SqlPreviewProps>) {
  const copyToClipboard = useClipboard();

  return (
    <Box style={{ position: "relative" }}>
      <Tooltip content="Copy SQL to clipboard">
        <IconButton
          size="1"
          variant="ghost"
          aria-label="Copy SQL to clipboard"
          onClick={() => copyToClipboard(sql)}
          style={{
            position: "absolute",
            top: 8,
            right: 8,
            zIndex: 1,
          }}
        >
          <CopyIcon />
        </IconButton>
      </Tooltip>
      <TextArea
        readOnly
        size="1"
        rows={rows}
        value={sql}
        style={{ fontFamily: "monospace" }}
        aria-label={ariaLabel}
      />
    </Box>
  );
}

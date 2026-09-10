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
 * The export set: a basket of captured subjects, their editable output names,
 * and the job-wide filters and format that apply to all of them. Exporting the
 * set starts one `$sql-export` job carrying every entry.
 *
 * @author John Grimes
 */

import { Cross2Icon } from "@radix-ui/react-icons";
import {
  Badge,
  Button,
  Card,
  Flex,
  Heading,
  IconButton,
  Select,
  Text,
  TextField,
} from "@radix-ui/themes";

import { ErrorCallout } from "../error/ErrorCallout";

import type { ExportSetEntry } from "../../hooks/exportSetHelpers";
import type { SqlExportFormat } from "../../types/sqlExport";

interface ExportSetPanelProps {
  /** The captured entries, in the order they were added. */
  entries: ExportSetEntry[];
  /** The chosen output format. */
  format: SqlExportFormat;
  /** The job-wide filters, as typed. */
  filters: { patients: string; groups: string; since: string };
  /** The names shared by more than one entry, or blank names. */
  collisions: string[];
  /** Renames one entry. */
  onRename: (id: string, name: string) => void;
  /** Removes one entry. */
  onRemove: (id: string) => void;
  /** Empties the set. */
  onClear: () => void;
  /** Changes the output format. */
  onFormatChange: (format: SqlExportFormat) => void;
  /** Changes one of the job-wide filters. */
  onFilterChange: (field: keyof ExportSetPanelProps["filters"], value: string) => void;
  /** Exports the whole set as one job. */
  onExport: () => void;
}

/** The formats an export can write. */
const FORMATS: SqlExportFormat[] = ["ndjson", "csv", "parquet"];

/**
 * Renders the export set, or nothing at all when it is empty.
 *
 * @param props - Component props.
 * @param props.entries - The captured entries.
 * @param props.format - The chosen output format.
 * @param props.filters - The job-wide filters, as typed.
 * @param props.collisions - The colliding or blank names.
 * @param props.onRename - Renames one entry.
 * @param props.onRemove - Removes one entry.
 * @param props.onClear - Empties the set.
 * @param props.onFormatChange - Changes the output format.
 * @param props.onFilterChange - Changes one of the job-wide filters.
 * @param props.onExport - Exports the whole set as one job.
 * @returns The panel, or null when the set is empty.
 */
export function ExportSetPanel({
  entries,
  format,
  filters,
  collisions,
  onRename,
  onRemove,
  onClear,
  onFormatChange,
  onFilterChange,
  onExport,
}: Readonly<ExportSetPanelProps>) {
  // An empty basket has nothing to show and no action to offer.
  if (entries.length === 0) {
    return null;
  }

  const hasCollisions = collisions.length > 0;

  return (
    <Card>
      <Flex direction="column" gap="3">
        <Flex align="center" justify="between">
          <Heading size="3">Export set ({entries.length})</Heading>
          <Button size="1" variant="soft" color="gray" onClick={onClear}>
            Clear all
          </Button>
        </Flex>

        <Flex direction="column" gap="2">
          {entries.map((entry) => (
            <Flex key={entry.id} align="center" gap="2">
              <Badge color={entry.kind === "view" ? "blue" : "purple"}>{entry.kind}</Badge>
              <TextField.Root
                style={{ flex: 1 }}
                size="1"
                value={entry.name}
                aria-label={`Output name for ${entry.kind} entry`}
                onChange={(event) => onRename(entry.id, event.target.value)}
              />
              <IconButton
                size="1"
                variant="soft"
                color="gray"
                aria-label={`Remove ${entry.name || "entry"} from the export set`}
                onClick={() => onRemove(entry.id)}
              >
                <Cross2Icon />
              </IconButton>
            </Flex>
          ))}
        </Flex>

        {hasCollisions && (
          <ErrorCallout
            size="1"
            message={
              collisions.includes("")
                ? "Every entry needs a name before the set can be exported."
                : `Each entry needs a distinct name; ${collisions.join(", ")} is used more than once.`
            }
          />
        )}

        <Flex gap="2" wrap="wrap">
          <Flex direction="column" gap="1" style={{ flex: 1, minWidth: "8rem" }}>
            <Text as="label" size="2" weight="medium" htmlFor="export-set-patients">
              Patients
            </Text>
            <TextField.Root
              id="export-set-patients"
              size="1"
              placeholder="p1, p2"
              value={filters.patients}
              onChange={(event) => onFilterChange("patients", event.target.value)}
            />
          </Flex>
          <Flex direction="column" gap="1" style={{ flex: 1, minWidth: "8rem" }}>
            <Text as="label" size="2" weight="medium" htmlFor="export-set-groups">
              Groups
            </Text>
            <TextField.Root
              id="export-set-groups"
              size="1"
              placeholder="g1"
              value={filters.groups}
              onChange={(event) => onFilterChange("groups", event.target.value)}
            />
          </Flex>
          <Flex direction="column" gap="1" style={{ flex: 1, minWidth: "10rem" }}>
            <Text as="label" size="2" weight="medium" htmlFor="export-set-since">
              Since
            </Text>
            <TextField.Root
              id="export-set-since"
              size="1"
              placeholder="2026-01-01T00:00:00Z"
              value={filters.since}
              onChange={(event) => onFilterChange("since", event.target.value)}
            />
          </Flex>
        </Flex>
        <Text size="1" color="gray">
          Filters apply to every subject in the job. All optional.
        </Text>

        <Flex align="center" justify="between" gap="2">
          <Select.Root
            size="1"
            value={format}
            onValueChange={(value) => onFormatChange(value as SqlExportFormat)}
          >
            <Select.Trigger aria-label="Export set format" />
            <Select.Content>
              {FORMATS.map((option) => (
                <Select.Item key={option} value={option}>
                  {option}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
          <Button size="1" disabled={hasCollisions} onClick={onExport}>
            Export set
          </Button>
        </Flex>
      </Flex>
    </Card>
  );
}

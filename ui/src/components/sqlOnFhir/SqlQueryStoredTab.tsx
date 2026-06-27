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
 * "Select query" tab body for the SQL query form: a grouped picker offering
 * stored SQLQueries and SQLViews, a decoded SQL preview and read-only
 * summaries of referenced views and declared parameters.
 *
 * @author John Grimes
 */

import { CopyIcon } from "@radix-ui/react-icons";
import {
  Badge,
  Box,
  Code,
  Flex,
  IconButton,
  Select,
  Spinner,
  Text,
  TextArea,
  Tooltip,
} from "@radix-ui/themes";

import { useClipboard } from "../../hooks";
import { FieldGuidance } from "../FieldGuidance";
import { FieldLabel } from "../FieldLabel";

import type { SqlQueryLibrarySummary } from "../../types/sqlQuery";

interface SqlQueryStoredTabProps {
  /** Stored SQLQuery summaries; undefined while loading. */
  queries: SqlQueryLibrarySummary[] | undefined;
  /** Stored SQLView summaries; undefined while loading. */
  views: SqlQueryLibrarySummary[] | undefined;
  /** Whether either stored list is still loading. */
  isLoading: boolean;
  /** Currently selected logical ID, or empty string when nothing is selected. */
  selectedId: string;
  /** Callback fired when the user picks a source. */
  onSelect: (id: string) => void;
  /** Whether the controls should be disabled. */
  disabled?: boolean;
}

/**
 * Renders the "Select query" tab body.
 *
 * @param props - The component props.
 * @param props.queries - Stored SQLQuery summaries.
 * @param props.views - Stored SQLView summaries.
 * @param props.isLoading - Whether either stored list is loading.
 * @param props.selectedId - The currently selected logical ID.
 * @param props.onSelect - Callback fired when the user picks a source.
 * @param props.disabled - Whether the controls should be disabled.
 * @returns The tab body.
 */
export function SqlQueryStoredTab({
  queries,
  views,
  isLoading,
  selectedId,
  onSelect,
  disabled = false,
}: Readonly<SqlQueryStoredTabProps>) {
  const copyToClipboard = useClipboard();

  const hasQueries = (queries?.length ?? 0) > 0;
  const hasViews = (views?.length ?? 0) > 0;

  // The selection is by logical id, unique across the Library store, so the
  // active summary is located across both stored lists.
  const selectedLibrary = [...(queries ?? []), ...(views ?? [])].find(
    (lib) => lib.id === selectedId,
  );

  if (isLoading) {
    return (
      <Flex align="center" gap="2" py="2">
        <Spinner size="1" />
        <Text size="2" color="gray">
          Loading stored queries and views...
        </Text>
      </Flex>
    );
  }

  if (!hasQueries && !hasViews) {
    return (
      <FieldGuidance>
        No stored SQL queries or SQL views found on the server. Use the "Provide SQL" tab to author
        one.
      </FieldGuidance>
    );
  }

  return (
    <Flex direction="column" gap="3">
      <Box>
        <FieldLabel mb="2">SQL query source</FieldLabel>
        <Select.Root
          value={selectedId === "" ? undefined : selectedId}
          onValueChange={onSelect}
          disabled={disabled}
        >
          <Select.Trigger
            style={{ width: "100%" }}
            placeholder="Select a query or view"
            aria-label="SQL query source"
          />
          <Select.Content>
            {hasQueries && (
              <Select.Group>
                <Select.Label>SQL queries</Select.Label>
                {queries!.map((lib) => (
                  <Select.Item key={lib.id} value={lib.id}>
                    {lib.title}
                  </Select.Item>
                ))}
              </Select.Group>
            )}
            {hasViews && (
              <Select.Group>
                <Select.Label>SQL views</Select.Label>
                {views!.map((lib) => (
                  <Select.Item key={lib.id} value={lib.id}>
                    {lib.title}
                  </Select.Item>
                ))}
              </Select.Group>
            )}
          </Select.Content>
        </Select.Root>
      </Box>

      {selectedLibrary ? (
        <>
          <Box>
            <FieldLabel mb="1">SQL preview</FieldLabel>
            <Box style={{ position: "relative" }}>
              <Tooltip content="Copy SQL to clipboard">
                <IconButton
                  size="1"
                  variant="ghost"
                  aria-label="Copy SQL to clipboard"
                  onClick={() => copyToClipboard(selectedLibrary.sql)}
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
                rows={10}
                value={selectedLibrary.sql}
                style={{ fontFamily: "monospace" }}
                aria-label="Decoded SQL preview"
              />
            </Box>
          </Box>

          <Box>
            <FieldLabel mb="1">Views</FieldLabel>
            {selectedLibrary.relatedArtifacts.length > 0 ? (
              <Flex direction="column" gap="1">
                {selectedLibrary.relatedArtifacts.map((ra) => (
                  <Flex key={`${ra.label}|${ra.reference}`} align="center" gap="2">
                    <Code size="2">{ra.label}</Code>
                    <Text size="2" color="gray">
                      &rarr;
                    </Text>
                    <Code size="2">{ra.reference}</Code>
                  </Flex>
                ))}
              </Flex>
            ) : (
              <FieldGuidance>None.</FieldGuidance>
            )}
          </Box>

          <Box>
            <FieldLabel mb="1">Declared parameters</FieldLabel>
            {selectedLibrary.parameters.length > 0 ? (
              <Flex gap="1" wrap="wrap">
                {selectedLibrary.parameters.map((p) => (
                  <Badge key={p.name} color="gray">
                    {p.name}
                    <Text size="1" color="gray">
                      :{p.type}
                    </Text>
                  </Badge>
                ))}
              </Flex>
            ) : (
              <FieldGuidance>None.</FieldGuidance>
            )}
          </Box>
        </>
      ) : (
        <FieldGuidance>
          Select a stored query or view to preview its SQL and bind runtime parameters.
        </FieldGuidance>
      )}
    </Flex>
  );
}

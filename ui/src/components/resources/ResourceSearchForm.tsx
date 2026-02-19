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
 * Form for configuring and executing a FHIR resource search.
 *
 * @author John Grimes
 */

import { Cross2Icon, MagnifyingGlassIcon, PlusIcon } from "@radix-ui/react-icons";
import { Box, Button, Card, Flex, Heading, IconButton, Select, TextField } from "@radix-ui/themes";
import { useRef, useState } from "react";

import { FieldGuidance } from "../FieldGuidance";
import { FieldLabel } from "../FieldLabel";
import { SearchParamsInput, createEmptyRow } from "../SearchParamsInput";

import type { SearchParamCapability } from "../../hooks/useServerCapabilities";
import type { SearchRequest } from "../../types/search";
import type { SearchParamRowData } from "../SearchParamsInput";

interface FilterInputWithId {
  id: number;
  expression: string;
}

interface ResourceSearchFormProps {
  onSubmit: (request: SearchRequest) => void;
  isLoading: boolean;
  disabled: boolean;
  resourceTypes: string[];
  /** Search parameters keyed by resource type, from the CapabilityStatement. */
  searchParams?: Record<string, SearchParamCapability[]>;
}

/**
 * Form for searching FHIR resources with FHIRPath filters and standard search
 * parameters.
 *
 * @param root0 - The component props.
 * @param root0.onSubmit - Callback when search is submitted.
 * @param root0.isLoading - Whether search is in progress.
 * @param root0.disabled - Whether the form is disabled.
 * @param root0.resourceTypes - Available resource types for selection.
 * @param root0.searchParams - Search parameters keyed by resource type.
 * @returns The resource search form component.
 */
export function ResourceSearchForm({
  onSubmit,
  isLoading,
  disabled,
  resourceTypes,
  searchParams,
}: ResourceSearchFormProps) {
  const idCounter = useRef(1);
  const [resourceType, setResourceType] = useState<string>("Patient");
  const [filters, setFilters] = useState<FilterInputWithId[]>([{ id: 0, expression: "" }]);
  const [paramRows, setParamRows] = useState<SearchParamRowData[]>([
    { id: 0, paramName: "", value: "" },
  ]);

  // Available search parameters for the currently selected resource type.
  const availableParams = searchParams?.[resourceType] ?? [];

  // Reset all form fields when the resource type changes, since the previous
  // search criteria are unlikely to be relevant to the new type.
  const prevResourceType = useRef(resourceType);
  if (prevResourceType.current !== resourceType) {
    prevResourceType.current = resourceType;
    idCounter.current = 1;
    setFilters([{ id: 0, expression: "" }]);
    setParamRows([createEmptyRow()]);
  }

  const handleSubmit = () => {
    // Build params from search parameter rows.
    const params: Record<string, string[]> = {};
    for (const row of paramRows) {
      if (row.paramName && row.value.trim()) {
        if (!params[row.paramName]) {
          params[row.paramName] = [];
        }
        params[row.paramName].push(row.value.trim());
      }
    }

    const request: SearchRequest = {
      resourceType,
      filters: filters.map((f) => f.expression).filter((e) => e.trim() !== ""),
      params,
    };
    onSubmit(request);
  };

  const addFilter = () => {
    setFilters([...filters, { id: idCounter.current++, expression: "" }]);
  };

  const removeFilter = (id: number) => {
    if (filters.length > 1) {
      setFilters(filters.filter((filter) => filter.id !== id));
    }
  };

  const updateFilter = (id: number, expression: string) => {
    setFilters(filters.map((f) => (f.id === id ? { ...f, expression } : f)));
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !disabled && !isLoading) {
      handleSubmit();
    }
  };

  return (
    <Card>
      <Flex direction="column" gap="4">
        <Heading size="4">Search resources</Heading>

        <Box>
          <FieldLabel mb="2">Resource type</FieldLabel>
          <Select.Root value={resourceType} onValueChange={setResourceType}>
            <Select.Trigger style={{ width: "100%" }} />
            <Select.Content>
              {resourceTypes.map((type) => (
                <Select.Item key={type} value={type}>
                  {type}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        </Box>

        {searchParams && (
          <SearchParamsInput
            availableParams={availableParams}
            rows={paramRows}
            onChange={setParamRows}
            onKeyDown={handleKeyDown}
          />
        )}

        <Box>
          <Flex justify="between" align="center" mb="2">
            <FieldLabel mb="0">FHIRPath filters</FieldLabel>
            <Button size="1" variant="soft" onClick={addFilter}>
              <PlusIcon />
              Add filter
            </Button>
          </Flex>
          <Flex direction="column" gap="2">
            {filters.map((filter) => (
              <Flex key={filter.id} gap="2" align="center">
                <Box style={{ flex: 1 }}>
                  <TextField.Root
                    placeholder="e.g., gender = 'female'"
                    value={filter.expression}
                    onChange={(e) => updateFilter(filter.id, e.target.value)}
                    onKeyDown={handleKeyDown}
                  />
                </Box>
                <IconButton
                  size="2"
                  variant="soft"
                  color="red"
                  onClick={() => removeFilter(filter.id)}
                  disabled={filters.length === 1}
                >
                  <Cross2Icon />
                </IconButton>
              </Flex>
            ))}
          </Flex>
          <FieldGuidance>
            Filter expressions are combined with AND logic. Leave empty to return all resources.
          </FieldGuidance>
        </Box>

        <Button size="3" onClick={handleSubmit} disabled={disabled || isLoading}>
          <MagnifyingGlassIcon />
          {isLoading ? "Searching..." : "Search"}
        </Button>
      </Flex>
    </Card>
  );
}

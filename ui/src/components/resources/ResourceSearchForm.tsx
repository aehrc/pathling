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
import {
  Box,
  Button,
  Card,
  Flex,
  Heading,
  IconButton,
  Select,
  Text,
  TextField,
} from "@radix-ui/themes";
import { useRef, useState } from "react";

import type { SearchRequest } from "../../types/search";

interface FilterInputWithId {
  id: number;
  expression: string;
}

interface ResourceSearchFormProps {
  onSubmit: (request: SearchRequest) => void;
  isLoading: boolean;
  disabled: boolean;
  resourceTypes: string[];
}

/**
 * Form for searching FHIR resources with FHIRPath filters.
 *
 * @param root0 - The component props.
 * @param root0.onSubmit - Callback when search is submitted.
 * @param root0.isLoading - Whether search is in progress.
 * @param root0.disabled - Whether the form is disabled.
 * @param root0.resourceTypes - Available resource types for selection.
 * @returns The resource search form component.
 */
export function ResourceSearchForm({
  onSubmit,
  isLoading,
  disabled,
  resourceTypes,
}: ResourceSearchFormProps) {
  const idCounter = useRef(1);
  const [resourceType, setResourceType] = useState<string>("Patient");
  const [filters, setFilters] = useState<FilterInputWithId[]>([{ id: 0, expression: "" }]);

  const handleSubmit = () => {
    const request: SearchRequest = {
      resourceType,
      filters: filters.map((f) => f.expression).filter((e) => e.trim() !== ""),
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
          <Box mb="2">
            <Text as="label" size="2" weight="medium">
              Resource type
            </Text>
          </Box>
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

        <Box>
          <Flex justify="between" align="center" mb="2">
            <Text as="label" size="2" weight="medium">
              FHIRPath filters
            </Text>
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
          <Text size="1" color="gray" mt="1">
            Filter expressions are combined with AND logic. Leave empty to return all resources.
          </Text>
        </Box>

        <Button size="3" onClick={handleSubmit} disabled={disabled || isLoading}>
          <MagnifyingGlassIcon />
          {isLoading ? "Searching..." : "Search"}
        </Button>
      </Flex>
    </Card>
  );
}

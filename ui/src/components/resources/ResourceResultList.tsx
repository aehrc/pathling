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
 * Component for displaying a list of FHIR resource search results.
 *
 * @author John Grimes
 */

import { ExclamationTriangleIcon, MagnifyingGlassIcon } from "@radix-ui/react-icons";
import { Badge, Box, Callout, Flex, Heading, Spinner, Text } from "@radix-ui/themes";

import { ResourceCard } from "./ResourceCard";

import type { Resource } from "fhir/r4";

interface ResourceResultListProps {
  resources: Resource[] | undefined;
  total: number | undefined;
  isLoading: boolean;
  error: Error | null;
  hasSearched: boolean;
  fhirBaseUrl: string;
  onDelete: (resourceType: string, resourceId: string, summary: string | null) => void;
}

/**
 * Displays search results as a list of resource cards.
 *
 * @param root0 - The component props.
 * @param root0.resources - Array of FHIR resources to display.
 * @param root0.total - Total count from server (may exceed displayed count).
 * @param root0.isLoading - Whether search is in progress.
 * @param root0.error - Error from failed search, if any.
 * @param root0.hasSearched - Whether a search has been executed.
 * @param root0.fhirBaseUrl - Base URL of the FHIR server.
 * @param root0.onDelete - Callback when delete is requested.
 * @returns The resource result list component.
 */
export function ResourceResultList({
  resources,
  total,
  isLoading,
  error,
  hasSearched,
  fhirBaseUrl,
  onDelete,
}: ResourceResultListProps) {
  // Initial state before any search.
  if (!hasSearched) {
    return (
      <Box>
        <Heading size="4" mb="4">
          Results
        </Heading>
        <Flex align="center" justify="center" py="8" direction="column" gap="2">
          <MagnifyingGlassIcon width={32} height={32} color="var(--gray-8)" />
          <Text color="gray">Select a resource type and search to view results.</Text>
        </Flex>
      </Box>
    );
  }

  // Loading state.
  if (isLoading) {
    return (
      <Box>
        <Heading size="4" mb="4">
          Results
        </Heading>
        <Flex align="center" gap="2" py="4">
          <Spinner />
          <Text>Searching...</Text>
        </Flex>
      </Box>
    );
  }

  // Error state.
  if (error) {
    return (
      <Box>
        <Heading size="4" mb="4">
          Results
        </Heading>
        <Callout.Root color="red">
          <Callout.Icon>
            <ExclamationTriangleIcon />
          </Callout.Icon>
          <Callout.Text>{error.message}</Callout.Text>
        </Callout.Root>
      </Box>
    );
  }

  // Empty results.
  if (!resources || resources.length === 0) {
    return (
      <Box>
        <Heading size="4" mb="4">
          Results
        </Heading>
        <Flex align="center" justify="center" py="8" direction="column" gap="2">
          <Text color="gray">No resources found matching your criteria.</Text>
        </Flex>
      </Box>
    );
  }

  // Results list.
  return (
    <Box>
      <Flex align="center" gap="2" mb="4">
        <Heading size="4">Results</Heading>
        <Badge color="gray">
          {total !== undefined
            ? `${total} total, first ${resources.length} shown`
            : `${resources.length} shown`}
        </Badge>
      </Flex>
      <Flex direction="column" gap="3">
        {resources.map((resource) => (
          <ResourceCard
            key={`${resource.resourceType}/${resource.id}`}
            resource={resource}
            fhirBaseUrl={fhirBaseUrl}
            onDelete={onDelete}
          />
        ))}
      </Flex>
    </Box>
  );
}

/**
 * Component for displaying a list of FHIR resource search results.
 *
 * @author John Grimes
 */

import { ExclamationTriangleIcon, MagnifyingGlassIcon } from "@radix-ui/react-icons";
import { Badge, Box, Callout, Flex, Heading, Spinner, Text } from "@radix-ui/themes";
import type { Resource } from "fhir/r4";
import { ResourceCard } from "./ResourceCard";

interface ResourceResultListProps {
  resources: Resource[] | undefined;
  total: number | undefined;
  isLoading: boolean;
  error: Error | null;
  hasSearched: boolean;
}

export function ResourceResultList({
  resources,
  total,
  isLoading,
  error,
  hasSearched,
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
        <Badge color="gray">{total !== undefined ? `${total} total` : `${resources.length} shown`}</Badge>
      </Flex>
      <Flex direction="column" gap="3">
        {resources.map((resource) => (
          <ResourceCard key={`${resource.resourceType}/${resource.id}`} resource={resource} />
        ))}
      </Flex>
    </Box>
  );
}

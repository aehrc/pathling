/**
 * List component for displaying all import jobs.
 *
 * @author John Grimes
 */

import { Box, Flex, Heading, Text } from "@radix-ui/themes";
import { ImportJobCard } from "./ImportJobCard";
import type { ImportJob } from "../../types/job";

interface ImportJobListProps {
  jobs: ImportJob[];
  onCancel: (id: string) => void;
}

export function ImportJobList({ jobs, onCancel }: ImportJobListProps) {
  if (jobs.length === 0) {
    return (
      <Box py="6">
        <Text color="gray" align="center" as="div">
          No import jobs yet. Start a new import using the form above.
        </Text>
      </Box>
    );
  }

  return (
    <Box>
      <Heading size="4" mb="3">
        Import Jobs
      </Heading>
      <Flex direction="column" gap="3">
        {jobs.map((job) => (
          <ImportJobCard key={job.id} job={job} onCancel={onCancel} />
        ))}
      </Flex>
    </Box>
  );
}

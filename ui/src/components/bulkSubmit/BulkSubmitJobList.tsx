/**
 * List component for displaying all bulk submit jobs.
 *
 * @author John Grimes
 */

import { Box, Flex, Heading, Text } from "@radix-ui/themes";
import { BulkSubmitJobCard } from "./BulkSubmitJobCard";
import type { BulkSubmitJob } from "../../types/job";

interface BulkSubmitJobListProps {
  jobs: BulkSubmitJob[];
  onAbort: (id: string) => void;
}

export function BulkSubmitJobList({ jobs, onAbort }: BulkSubmitJobListProps) {
  if (jobs.length === 0) {
    return (
      <Box py="6">
        <Text color="gray" align="center" as="div">
          No submissions yet. Create a new submission or start monitoring an existing one.
        </Text>
      </Box>
    );
  }

  return (
    <Box>
      <Heading size="4" mb="3">
        Submissions
      </Heading>
      <Flex direction="column" gap="3">
        {jobs.map((job) => (
          <BulkSubmitJobCard key={job.id} job={job} onAbort={onAbort} />
        ))}
      </Flex>
    </Box>
  );
}

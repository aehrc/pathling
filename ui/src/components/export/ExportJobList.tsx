/**
 * List component for displaying all export jobs.
 *
 * @author John Grimes
 */

import { Box, Flex, Heading, Text } from "@radix-ui/themes";
import { ExportJobCard } from "./ExportJobCard";
import type { ExportJob } from "../../types/export";

interface ExportJobListProps {
  jobs: ExportJob[];
  onCancel: (id: string) => void;
  onDownload: (url: string, filename: string) => void;
}

export function ExportJobList({ jobs, onCancel, onDownload }: ExportJobListProps) {
  if (jobs.length === 0) {
    return (
      <Box py="6">
        <Text color="gray" align="center" as="div">
          No export jobs yet. Start a new export using the form above.
        </Text>
      </Box>
    );
  }

  return (
    <Box>
      <Heading size="4" mb="3">
        Export Jobs
      </Heading>
      <Flex direction="column" gap="3">
        {jobs.map((job) => (
          <ExportJobCard
            key={job.id}
            job={job}
            onCancel={onCancel}
            onDownload={onDownload}
          />
        ))}
      </Flex>
    </Box>
  );
}

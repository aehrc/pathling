/**
 * Card component displaying a single bulk submit job with progress and actions.
 *
 * @author John Grimes
 */

import { Cross2Icon, ReloadIcon } from "@radix-ui/react-icons";
import { Badge, Box, Button, Card, Flex, Progress, Text } from "@radix-ui/themes";
import { useJobs } from "../../contexts/JobContext";
import { useBulkSubmitJobPolling } from "../../hooks/useBulkSubmitJobPolling";
import type { BulkSubmitJob } from "../../types/job";

interface BulkSubmitJobCardProps {
  job: BulkSubmitJob;
  onAbort: (id: string) => void;
}

const STATUS_COLORS: Record<BulkSubmitJob["status"], "blue" | "green" | "red" | "orange" | "gray"> =
  {
    pending: "blue",
    in_progress: "blue",
    completed: "green",
    failed: "red",
    cancelled: "gray",
  };

const STATUS_LABELS: Record<BulkSubmitJob["status"], string> = {
  pending: "Pending",
  in_progress: "Processing",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Aborted",
};

function formatDate(date: Date): string {
  return new Intl.DateTimeFormat("en-AU", {
    dateStyle: "short",
    timeStyle: "medium",
  }).format(date);
}

export function BulkSubmitJobCard({ job, onAbort }: BulkSubmitJobCardProps) {
  const { updateJobProgress, updateJobManifest, updateJobError, updateJobStatus } = useJobs();

  // Poll job status while active.
  useBulkSubmitJobPolling({
    jobId: job.id,
    pollUrl: job.pollUrl,
    status: job.status,
    onProgress: updateJobProgress,
    onStatusChange: updateJobStatus,
    onComplete: updateJobManifest,
    onError: updateJobError,
  });

  const isActive = job.status === "pending" || job.status === "in_progress";
  const showProgress = isActive && job.progress !== null;

  return (
    <Card>
      <Flex direction="column" gap="3">
        <Flex justify="between" align="start">
          <Box>
            <Flex align="center" gap="2" mb="1">
              <Text weight="medium">Submission</Text>
              <Badge color={STATUS_COLORS[job.status]}>{STATUS_LABELS[job.status]}</Badge>
            </Flex>
            <Text size="1" color="gray" as="div">
              Started: {formatDate(job.createdAt)}
            </Text>
            <Text size="1" color="gray" as="div">
              ID: {job.submissionId}
            </Text>
            <Text size="1" color="gray" as="div">
              Submitter: {job.submitter.system}|{job.submitter.value}
            </Text>
            {job.request.manifestUrl && (
              <Text size="1" color="gray" as="div">
                Manifest: {job.request.manifestUrl}
              </Text>
            )}
          </Box>
          {isActive && (
            <Button size="1" variant="soft" color="red" onClick={() => onAbort(job.id)}>
              <Cross2Icon />
              Abort
            </Button>
          )}
        </Flex>

        {showProgress && (
          <Box>
            <Flex justify="between" mb="1">
              <Text size="1" color="gray">
                Progress
              </Text>
              <Text size="1" color="gray">
                {job.progress}%
              </Text>
            </Flex>
            <Progress value={job.progress ?? 0} />
          </Box>
        )}

        {isActive && !showProgress && (
          <Flex align="center" gap="2">
            <ReloadIcon style={{ animation: "spin 1s linear infinite" }} />
            <Text size="2" color="gray">
              Processing...
            </Text>
          </Flex>
        )}

        {job.status === "failed" && job.error && (
          <Text size="2" color="red">
            Error: {job.error}
          </Text>
        )}

        {job.status === "completed" && job.manifest && (
          <Box>
            <Text size="2" weight="medium" mb="2">
              Output files ({job.manifest.output.length})
            </Text>
            <Flex direction="column" gap="1">
              {job.manifest.output.map((output, index) => (
                <Flex key={index} justify="between">
                  <Text size="2" color="gray">
                    {output.type}
                  </Text>
                  {output.count !== undefined && (
                    <Text size="2" color="gray">
                      {output.count} resources
                    </Text>
                  )}
                </Flex>
              ))}
            </Flex>
            {job.manifest.error && job.manifest.error.length > 0 && (
              <Box mt="2">
                <Text size="2" weight="medium" color="red" mb="1">
                  Errors ({job.manifest.error.length})
                </Text>
                {job.manifest.error.map((err, index) => (
                  <Text key={index} size="1" color="red" as="div">
                    {err.type}: {err.url}
                  </Text>
                ))}
              </Box>
            )}
          </Box>
        )}
      </Flex>
    </Card>
  );
}

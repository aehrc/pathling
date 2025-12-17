/**
 * Card component displaying a single export job with progress and actions.
 *
 * @author John Grimes
 */

import { Cross2Icon, DownloadIcon, ReloadIcon } from "@radix-ui/react-icons";
import { Badge, Box, Button, Card, Flex, Progress, Text } from "@radix-ui/themes";
import { useJobs } from "../../contexts/JobContext";
import { useExportJobPolling } from "../../hooks/useExportJobPolling";
import type { ExportJob } from "../../types/job";

interface ExportJobCardProps {
  job: ExportJob;
  onCancel: (id: string) => void;
  onDownload: (url: string, filename: string) => void;
}

const STATUS_COLORS: Record<ExportJob["status"], "blue" | "green" | "red" | "orange" | "gray"> = {
  pending: "blue",
  in_progress: "blue",
  completed: "green",
  failed: "red",
  cancelled: "gray",
};

const STATUS_LABELS: Record<ExportJob["status"], string> = {
  pending: "Pending",
  in_progress: "In progress",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
};

function formatDate(date: Date): string {
  return new Intl.DateTimeFormat("en-AU", {
    dateStyle: "short",
    timeStyle: "medium",
  }).format(date);
}

function formatLevel(level: ExportJob["request"]["level"]): string {
  switch (level) {
    case "system":
      return "System";
    case "patient-type":
      return "Patient type";
    case "patient-instance":
      return "Patient instance";
    case "group":
      return "Group";
    default:
      return level;
  }
}

export function ExportJobCard({ job, onCancel, onDownload }: ExportJobCardProps) {
  const { updateJobProgress, updateJobManifest, updateJobError, updateJobStatus } = useJobs();

  // Poll job status while active.
  useExportJobPolling({
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
              <Text weight="medium">{formatLevel(job.request.level)} Export</Text>
              <Badge color={STATUS_COLORS[job.status]}>{STATUS_LABELS[job.status]}</Badge>
            </Flex>
            <Text size="1" color="gray">
              Started: {formatDate(job.createdAt)}
            </Text>
            {job.request.resourceTypes && job.request.resourceTypes.length > 0 && (
              <Text size="1" color="gray" as="div">
                Types: {job.request.resourceTypes.join(", ")}
              </Text>
            )}
          </Box>
          {isActive && (
            <Button size="1" variant="soft" color="red" onClick={() => onCancel(job.id)}>
              <Cross2Icon />
              Cancel
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

        {job.status === "completed" && job.manifest?.output && (
          <Box>
            <Text size="2" weight="medium" mb="2">
              Output files ({job.manifest.output.length})
            </Text>
            <Flex direction="column" gap="1">
              {job.manifest.output.map((output, index) => (
                <Flex key={index} justify="between" align="center">
                  <Text size="2">
                    {output.type}.ndjson
                    {output.count !== undefined && (
                      <Text color="gray"> ({output.count} resources)</Text>
                    )}
                  </Text>
                  <Button
                    size="1"
                    variant="soft"
                    onClick={() => onDownload(output.url, `${output.type}.ndjson`)}
                  >
                    <DownloadIcon />
                    Download
                  </Button>
                </Flex>
              ))}
            </Flex>
          </Box>
        )}
      </Flex>
    </Card>
  );
}

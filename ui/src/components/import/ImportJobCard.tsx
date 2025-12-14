/**
 * Card component displaying a single import job with progress and actions.
 *
 * @author John Grimes
 */

import { Cross2Icon, ReloadIcon } from "@radix-ui/react-icons";
import { Badge, Box, Button, Card, Flex, Progress, Text } from "@radix-ui/themes";
import type { ImportJob } from "../../types/job";
import { IMPORT_FORMATS, IMPORT_MODES } from "../../types/import";

interface ImportJobCardProps {
  job: ImportJob;
  onCancel: (id: string) => void;
}

const STATUS_COLORS: Record<ImportJob["status"], "blue" | "green" | "red" | "orange" | "gray"> = {
  pending: "blue",
  in_progress: "blue",
  completed: "green",
  failed: "red",
  cancelled: "gray",
};

const STATUS_LABELS: Record<ImportJob["status"], string> = {
  pending: "Pending",
  in_progress: "In Progress",
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

function getFormatLabel(format: string): string {
  const found = IMPORT_FORMATS.find((f) => f.value === format);
  return found?.label ?? format;
}

function getModeLabel(mode: string): string {
  const found = IMPORT_MODES.find((m) => m.value === mode);
  return found?.label ?? mode;
}

export function ImportJobCard({ job, onCancel }: ImportJobCardProps) {
  const isActive = job.status === "pending" || job.status === "in_progress";
  const showProgress = isActive && job.progress !== null;

  return (
    <Card>
      <Flex direction="column" gap="3">
        <Flex justify="between" align="start">
          <Box>
            <Flex align="center" gap="2" mb="1">
              <Text weight="medium">Import</Text>
              <Badge color={STATUS_COLORS[job.status]}>{STATUS_LABELS[job.status]}</Badge>
            </Flex>
            <Text size="1" color="gray">
              Started: {formatDate(job.createdAt)}
            </Text>
            <Text size="1" color="gray" as="div">
              Mode: {getModeLabel(job.request.mode)} | Format:{" "}
              {getFormatLabel(job.request.inputFormat)}
            </Text>
            <Text size="1" color="gray" as="div">
              Source: {job.request.inputSource}
            </Text>
            <Text size="1" color="gray" as="div">
              Files: {job.request.input.map((i) => i.type).join(", ")}
            </Text>
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
              Imported Files ({job.manifest.output.length})
            </Text>
            <Flex direction="column" gap="1">
              {job.manifest.output.map((output, index) => (
                <Text key={index} size="2" color="gray">
                  {output.inputUrl}
                </Text>
              ))}
            </Flex>
          </Box>
        )}
      </Flex>
    </Card>
  );
}

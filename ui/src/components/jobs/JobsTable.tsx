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
 * Presentational table of the caller's asynchronous jobs, handling the loading, empty, error and
 * populated states. All actions are delegated to the parent via callbacks.
 *
 * @author John Grimes
 */

import { Cross2Icon, InfoCircledIcon, TrashIcon } from "@radix-ui/react-icons";
import {
  Badge,
  Box,
  Button,
  Callout,
  Flex,
  Progress,
  Spinner,
  Table,
  Text,
} from "@radix-ui/themes";

import { formatJobStartTime, isJobInProgress, statusBadge } from "./jobsPresentation";

import type { JobSummary } from "../../api/jobs";

interface JobsTableProps {
  jobs: JobSummary[];
  isLoading: boolean;
  error: Error | null;
  onRetry: () => void;
  onCancelJob: (job: JobSummary) => void;
}

/**
 * Renders a short, monospace form of a job identifier.
 *
 * @param id - The full job identifier.
 * @returns The first segment followed by an ellipsis.
 */
function shortId(id: string): string {
  return id.length > 8 ? `${id.slice(0, 8)}…` : id;
}

/**
 * Displays the caller's jobs and their status, with a per-row cancel or remove action.
 *
 * @param root0 - The component props.
 * @param root0.jobs - The jobs to display.
 * @param root0.isLoading - Whether the initial job list is loading.
 * @param root0.error - Any error from loading the job list.
 * @param root0.onRetry - Callback to retry loading after an error.
 * @param root0.onCancelJob - Callback invoked with the job whose action was activated.
 * @returns The jobs table component.
 */
export function JobsTable({
  jobs,
  isLoading,
  error,
  onRetry,
  onCancelJob,
}: Readonly<JobsTableProps>) {
  if (error) {
    return (
      <Callout.Root color="red" role="alert">
        <Callout.Icon>
          <InfoCircledIcon />
        </Callout.Icon>
        <Flex direction="column" gap="2" align="start">
          <Callout.Text>Could not load jobs: {error.message}</Callout.Text>
          <Button size="1" variant="soft" onClick={onRetry}>
            Retry
          </Button>
        </Flex>
      </Callout.Root>
    );
  }

  if (isLoading) {
    return (
      <Flex align="center" gap="2">
        <Spinner />
        <Text>Loading jobs...</Text>
      </Flex>
    );
  }

  if (jobs.length === 0) {
    return (
      <Box
        p="6"
        style={{
          border: "1px dashed var(--gray-6)",
          borderRadius: "var(--radius-3)",
          textAlign: "center",
        }}
      >
        <Text as="p" weight="medium" mb="1">
          No jobs to show
        </Text>
        <Text as="p" size="2" color="gray">
          Jobs appear here when you start an asynchronous operation such as an export, import or SQL
          on FHIR export. Jobs do not survive a server restart.
        </Text>
      </Box>
    );
  }

  return (
    <Table.Root variant="surface">
      <Table.Header>
        <Table.Row>
          <Table.ColumnHeaderCell>Operation</Table.ColumnHeaderCell>
          <Table.ColumnHeaderCell>Status</Table.ColumnHeaderCell>
          <Table.ColumnHeaderCell>Progress</Table.ColumnHeaderCell>
          <Table.ColumnHeaderCell>Started</Table.ColumnHeaderCell>
          <Table.ColumnHeaderCell>Job ID</Table.ColumnHeaderCell>
          <Table.ColumnHeaderCell>Actions</Table.ColumnHeaderCell>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {jobs.map((job) => {
          const badge = statusBadge(job.status);
          const inProgress = isJobInProgress(job);
          return (
            <Table.Row key={job.id}>
              <Table.Cell>{job.operation}</Table.Cell>
              <Table.Cell>
                <Badge size="1" color={badge.color}>
                  {badge.label}
                </Badge>
              </Table.Cell>
              <Table.Cell>
                {inProgress && job.progress !== undefined ? (
                  <Flex align="center" gap="2">
                    <Box width="120px">
                      <Progress size="1" value={job.progress} />
                    </Box>
                    <Text size="1" color="gray">
                      {job.progress}%
                    </Text>
                  </Flex>
                ) : (
                  <Text color="gray">{"—"}</Text>
                )}
              </Table.Cell>
              <Table.Cell>
                <Text size="2">{formatJobStartTime(job.startTime)}</Text>
              </Table.Cell>
              <Table.Cell>
                <Text size="1" title={job.id} style={{ fontFamily: "var(--font-mono, monospace)" }}>
                  {shortId(job.id)}
                </Text>
              </Table.Cell>
              <Table.Cell>
                <Button
                  size="1"
                  variant="soft"
                  color={inProgress ? "red" : "gray"}
                  onClick={() => onCancelJob(job)}
                >
                  {inProgress ? <Cross2Icon /> : <TrashIcon />}
                  {inProgress ? "Cancel" : "Remove"}
                </Button>
              </Table.Cell>
            </Table.Row>
          );
        })}
      </Table.Body>
    </Table.Root>
  );
}

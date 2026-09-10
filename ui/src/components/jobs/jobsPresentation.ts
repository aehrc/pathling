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
 * Plain presentation logic for the jobs page, kept free of React so it can be unit tested in
 * isolation.
 *
 * @author John Grimes
 */

import { formatDateTime } from "../../utils";

import type { JobSummary, JobSummaryStatus } from "../../api/jobs";

/** Radix badge colours used for job statuses. */
export type JobBadgeColor = "blue" | "green" | "red" | "gray";

/** The visual representation of a job status. */
export interface JobStatusBadge {
  label: string;
  color: JobBadgeColor;
}

/** Poll interval used while any job is in progress, matching the async poll cadence. */
export const IN_PROGRESS_REFETCH_INTERVAL = 3000;

/** Poll interval used when no job is in progress. */
export const IDLE_REFETCH_INTERVAL = 10000;

const STATUS_BADGES: Record<JobSummaryStatus, JobStatusBadge> = {
  "in-progress": { label: "In progress", color: "blue" },
  completed: { label: "Completed", color: "green" },
  failed: { label: "Failed", color: "red" },
  cancelled: { label: "Cancelled", color: "gray" },
};

/**
 * Maps a job status to its badge label and colour.
 *
 * @param status - The job status.
 * @returns The badge label and colour to display.
 */
export function statusBadge(status: JobSummaryStatus): JobStatusBadge {
  return STATUS_BADGES[status];
}

/**
 * Reports whether a job is still running.
 *
 * @param job - The job to test.
 * @returns True if the job is in progress.
 */
export function isJobInProgress(job: JobSummary): boolean {
  return job.status === "in-progress";
}

/**
 * Reports whether cancelling a job should be confirmed first. Only in-progress jobs need
 * confirmation, since cancelling them discards work; finished jobs are simply removed.
 *
 * @param job - The job being acted on.
 * @returns True if a confirmation dialog should be shown before cancelling.
 */
export function requiresCancelConfirmation(job: JobSummary): boolean {
  return isJobInProgress(job);
}

/**
 * Chooses the query refetch interval based on whether any job is still running, so progress stays
 * current while jobs run without polling needlessly when they are all finished.
 *
 * @param jobs - The current list of jobs.
 * @returns The refetch interval in milliseconds.
 */
export function selectRefetchInterval(jobs: JobSummary[]): number {
  return jobs.some(isJobInProgress)
    ? IN_PROGRESS_REFETCH_INTERVAL
    : IDLE_REFETCH_INTERVAL;
}

/**
 * Formats a job's ISO start time for display, falling back to the raw value if it cannot be parsed.
 *
 * @param startTime - The ISO 8601 start time.
 * @returns A human-readable date-time string.
 */
export function formatJobStartTime(startTime: string): string {
  const date = new Date(startTime);
  if (Number.isNaN(date.getTime())) {
    return startTime;
  }
  return formatDateTime(date);
}

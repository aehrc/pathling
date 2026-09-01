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

import { describe, expect, it } from "vitest";

import {
  IDLE_REFETCH_INTERVAL,
  IN_PROGRESS_REFETCH_INTERVAL,
  formatJobStartTime,
  isJobInProgress,
  selectRefetchInterval,
  statusBadge,
} from "../jobsPresentation";

import type { JobSummary } from "../../../api/jobs";

/**
 * Builds a job summary with the given status for testing.
 *
 * @param status - The job status.
 * @param overrides - Optional field overrides.
 * @returns A job summary.
 */
function job(
  status: JobSummary["status"],
  overrides: Partial<JobSummary> = {},
): JobSummary {
  return {
    id: "id",
    operation: "export",
    status,
    startTime: "2026-07-24T00:42:11.000Z",
    url: "https://example.com/fhir/$job?id=id",
    ...overrides,
  };
}

describe("statusBadge", () => {
  it("maps each status to a label and colour", () => {
    expect(statusBadge("in-progress")).toEqual({
      label: "In progress",
      color: "blue",
    });
    expect(statusBadge("completed")).toEqual({
      label: "Completed",
      color: "green",
    });
    expect(statusBadge("failed")).toEqual({ label: "Failed", color: "red" });
    expect(statusBadge("cancelled")).toEqual({
      label: "Cancelled",
      color: "gray",
    });
  });
});

describe("selectRefetchInterval", () => {
  it("polls quickly when any job is in progress", () => {
    const jobs = [job("completed"), job("in-progress"), job("failed")];
    expect(selectRefetchInterval(jobs)).toBe(IN_PROGRESS_REFETCH_INTERVAL);
    expect(IN_PROGRESS_REFETCH_INTERVAL).toBe(3000);
  });

  it("polls slowly when no job is in progress", () => {
    const jobs = [job("completed"), job("failed"), job("cancelled")];
    expect(selectRefetchInterval(jobs)).toBe(IDLE_REFETCH_INTERVAL);
    expect(IDLE_REFETCH_INTERVAL).toBe(10000);
  });

  it("polls slowly when there are no jobs", () => {
    expect(selectRefetchInterval([])).toBe(IDLE_REFETCH_INTERVAL);
  });
});

describe("isJobInProgress", () => {
  it("is true only for in-progress jobs", () => {
    expect(isJobInProgress(job("in-progress"))).toBe(true);
    expect(isJobInProgress(job("completed"))).toBe(false);
    expect(isJobInProgress(job("failed"))).toBe(false);
    expect(isJobInProgress(job("cancelled"))).toBe(false);
  });
});

describe("formatJobStartTime", () => {
  it("formats a valid ISO instant into a readable string", () => {
    const formatted = formatJobStartTime("2026-07-24T00:42:11.000Z");
    expect(formatted).toContain("2026");
    expect(formatted).not.toBe("2026-07-24T00:42:11.000Z");
  });

  it("returns the original value when the timestamp is unparseable", () => {
    expect(formatJobStartTime("not-a-date")).toBe("not-a-date");
  });
});

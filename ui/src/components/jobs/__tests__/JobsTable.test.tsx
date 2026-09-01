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

import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { render, screen, within } from "../../../test/testUtils";
import { JobsTable } from "../JobsTable";

import type { JobSummary } from "../../../api/jobs";

const inProgressJob: JobSummary = {
  id: "7f3a9c1e-2b4d-4b8a-9c0d-1e2f3a4b5c6d",
  operation: "export",
  status: "in-progress",
  progress: 62,
  startTime: "2026-07-24T00:42:11.000Z",
  url: "https://example.com/fhir/$job?id=7f3a9c1e-2b4d-4b8a-9c0d-1e2f3a4b5c6d",
};

const completedJob: JobSummary = {
  id: "4c88f21a-1111-2222-3333-444455556666",
  operation: "import",
  status: "completed",
  startTime: "2026-07-24T09:03:00.000Z",
  url: "https://example.com/fhir/$job?id=4c88f21a-1111-2222-3333-444455556666",
};

/**
 * Renders the table with default no-op handlers, allowing overrides.
 *
 * @param props - Property overrides.
 * @returns The render result.
 */
function renderTable(props: Partial<Parameters<typeof JobsTable>[0]> = {}) {
  return render(
    <JobsTable
      jobs={props.jobs ?? []}
      isLoading={props.isLoading ?? false}
      error={props.error ?? null}
      onRetry={props.onRetry ?? vi.fn()}
      onCancelJob={props.onCancelJob ?? vi.fn()}
    />,
  );
}

describe("JobsTable", () => {
  it("renders a row per job with its operation and status", () => {
    renderTable({ jobs: [inProgressJob, completedJob] });

    expect(screen.getByText("export")).toBeInTheDocument();
    expect(screen.getByText("import")).toBeInTheDocument();
    expect(screen.getByText("In progress")).toBeInTheDocument();
    expect(screen.getByText("Completed")).toBeInTheDocument();
  });

  it("shows a progress bar only for in-progress jobs", () => {
    renderTable({ jobs: [inProgressJob, completedJob] });

    // Exactly one progress bar, for the single in-progress job.
    const progressBars = screen.getAllByRole("progressbar");
    expect(progressBars).toHaveLength(1);
    expect(screen.getByText("62%")).toBeInTheDocument();
  });

  it("renders the empty state when there are no jobs", () => {
    renderTable({ jobs: [] });

    expect(screen.getByText(/no jobs/i)).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("renders an error state with a working retry action", async () => {
    const onRetry = vi.fn();
    renderTable({ error: new Error("network down"), onRetry });

    // The shared error presentation announces the error and keeps the recovery
    // action inside the callout.
    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent("Could not load jobs: network down");

    const retry = within(alert).getByRole("button", { name: /retry/i });
    await userEvent.click(retry);

    expect(onRetry).toHaveBeenCalledTimes(1);
  });

  it("labels the action Cancel for in-progress jobs and Remove for finished jobs", () => {
    renderTable({ jobs: [inProgressJob, completedJob] });

    expect(screen.getByRole("button", { name: /cancel/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /remove/i })).toBeInTheDocument();
  });

  it("invokes onCancelJob when a row action is activated", async () => {
    const onCancelJob = vi.fn();
    renderTable({ jobs: [inProgressJob], onCancelJob });

    await userEvent.click(screen.getByRole("button", { name: /cancel/i }));

    expect(onCancelJob).toHaveBeenCalledWith(inProgressJob);
  });
});

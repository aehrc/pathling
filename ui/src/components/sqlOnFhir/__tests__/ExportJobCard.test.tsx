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
 * Tests for the ExportJobCard component, the presentation shared by both
 * asynchronous export flows.
 *
 * @author John Grimes
 */

import { beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { OperationOutcomeError } from "../../../types/errors";
import { ExportJobCard } from "../ExportJobCard";

import type { ExportJobCardData } from "../ExportJobCard";
import type { OperationOutcome } from "fhir/r4";

vi.mock("../../../utils", () => ({
  formatDateTime: () => "15 Jan 2026, 10:00 AM",
}));

describe("ExportJobCard", () => {
  const onCancel = vi.fn();
  const onDownload = vi.fn();
  const onClose = vi.fn();

  /**
   * Builds the card state for a job, defaulting everything a test does not care
   * about.
   *
   * @param overrides - The job fields to override.
   * @returns The export job card state.
   */
  function createJob(overrides: Partial<ExportJobCardData> = {}): ExportJobCardData {
    return {
      status: "in_progress",
      progress: null,
      error: null,
      format: "csv",
      manifest: null,
      createdAt: new Date("2026-01-15T10:00:00Z"),
      ...overrides,
    };
  }

  /**
   * Renders the card for a given job.
   *
   * @param job - The job state to render.
   * @returns The render result.
   */
  function renderCard(job: ExportJobCardData): ReturnType<typeof render> {
    return render(
      <ExportJobCard
        job={job}
        getOutputs={() => []}
        onCancel={onCancel}
        onDownload={onDownload}
        onClose={onClose}
      />,
    );
  }

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("shows the format and status of a running export", () => {
    renderCard(createJob({ status: "in_progress", progress: 40 }));

    expect(screen.getByText("Export to CSV")).toBeInTheDocument();
    expect(screen.getByText("Exporting")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /cancel/i })).toBeInTheDocument();
  });

  it("offers a close button once the export has finished", () => {
    renderCard(createJob({ status: "completed" }));

    expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /cancel/i })).not.toBeInTheDocument();
  });

  // FR-001, FR-005 and FR-006: a failed export shows its failure in the shared
  // callout, so it looks like every other failure and is announced.
  it("displays a failure in the shared callout, announced as an alert", () => {
    const outcome: OperationOutcome = {
      resourceType: "OperationOutcome",
      issue: [
        { severity: "error", code: "processing", diagnostics: "Export path is not writable" },
      ],
    };
    const error = new OperationOutcomeError(outcome, 400, "View export");

    const { container } = renderCard(createJob({ status: "failed", error }));

    expect(screen.getByRole("alert")).toHaveTextContent("Export path is not writable");
    expect(container.querySelector(".rt-CalloutIcon svg")).toBeInTheDocument();
  });

  // FR-008: each issue of a multi-issue outcome keeps its own severity.
  it("displays each issue of a multi-issue failure with its severity", () => {
    const outcome: OperationOutcome = {
      resourceType: "OperationOutcome",
      issue: [
        { severity: "error", code: "processing", diagnostics: "Export path is not writable" },
        { severity: "warning", code: "informational", diagnostics: "Partial output was retained" },
      ],
    };
    const error = new OperationOutcomeError(outcome, 400, "View export");

    renderCard(createJob({ status: "failed", error }));

    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent("Export path is not writable");
    expect(alert).toHaveTextContent("Partial output was retained");
    expect(screen.getByText("Error")).toBeInTheDocument();
    expect(screen.getByText("Warning")).toBeInTheDocument();
  });

  // FR-010: a failure with nothing to say still produces a callout rather than
  // an empty one.
  it("displays a fallback description when the failure carries no detail", () => {
    renderCard(createJob({ status: "failed", error: new Error("") }));

    expect(screen.getByRole("alert")).toHaveTextContent(/no further detail/i);
  });

  it("shows nothing failure-related while the export is running", () => {
    renderCard(createJob({ status: "in_progress" }));

    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });
});

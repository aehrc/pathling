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

import { render, screen } from "../../../test/testUtils";
import { CancelJobDialog } from "../CancelJobDialog";
import { requiresCancelConfirmation } from "../jobsPresentation";

import type { JobSummary } from "../../../api/jobs";

const inProgressJob: JobSummary = {
  id: "7f3a9c1e",
  operation: "export",
  status: "in-progress",
  progress: 62,
  startTime: "2026-07-24T00:42:11.000Z",
  url: "https://example.com/fhir/$job?id=7f3a9c1e",
};

describe("requiresCancelConfirmation", () => {
  it("requires confirmation only for in-progress jobs", () => {
    expect(requiresCancelConfirmation(inProgressJob)).toBe(true);
    expect(requiresCancelConfirmation({ ...inProgressJob, status: "completed" })).toBe(false);
    expect(requiresCancelConfirmation({ ...inProgressJob, status: "failed" })).toBe(false);
    expect(requiresCancelConfirmation({ ...inProgressJob, status: "cancelled" })).toBe(false);
  });
});

describe("CancelJobDialog", () => {
  it("renders nothing when no job is pending confirmation", () => {
    render(
      <CancelJobDialog
        job={null}
        isCancelling={false}
        onConfirm={vi.fn()}
        onOpenChange={vi.fn()}
      />,
    );

    expect(screen.queryByRole("alertdialog")).not.toBeInTheDocument();
  });

  it("shows a confirmation describing the job when one is pending", () => {
    render(
      <CancelJobDialog
        job={inProgressJob}
        isCancelling={false}
        onConfirm={vi.fn()}
        onOpenChange={vi.fn()}
      />,
    );

    expect(screen.getByRole("alertdialog")).toBeInTheDocument();
    expect(screen.getByText(/cancel job\?/i)).toBeInTheDocument();
    // The description mentions the operation so the user knows what they are stopping.
    expect(screen.getByText(/export/i)).toBeInTheDocument();
  });

  it("invokes onConfirm when the destructive action is activated", async () => {
    const onConfirm = vi.fn();
    render(
      <CancelJobDialog
        job={inProgressJob}
        isCancelling={false}
        onConfirm={onConfirm}
        onOpenChange={vi.fn()}
      />,
    );

    await userEvent.click(screen.getByRole("button", { name: /cancel job/i }));

    expect(onConfirm).toHaveBeenCalledTimes(1);
  });

  it("dismisses without confirming when keep running is chosen", async () => {
    const onConfirm = vi.fn();
    const onOpenChange = vi.fn();
    render(
      <CancelJobDialog
        job={inProgressJob}
        isCancelling={false}
        onConfirm={onConfirm}
        onOpenChange={onOpenChange}
      />,
    );

    await userEvent.click(screen.getByRole("button", { name: /keep running/i }));

    expect(onConfirm).not.toHaveBeenCalled();
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });
});

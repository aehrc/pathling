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
 * Tests for the BulkSubmit page, covering the reporting of monitoring failures.
 *
 * @author John Grimes
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../test/testUtils";
import { BulkSubmit } from "../BulkSubmit";

import type { ReactNode } from "react";

// Mock the toast context, so that the test can prove a displayed failure is not
// also announced.
const mockShowToast = vi.fn();
vi.mock("../../contexts/ToastContext", () => ({
  useToast: () => ({ showToast: mockShowToast }),
}));

// Mock the guard so the page content renders without an access check.
vi.mock("../../components/auth/CapabilityGuard", () => ({
  CapabilityGuard: ({ children }: { children: () => ReactNode }) => children(),
}));

// Mock the form, which plays no part in these assertions.
vi.mock("../../components/bulkSubmit/BulkSubmitMonitorForm", () => ({
  BulkSubmitMonitorForm: () => <div data-testid="bulk-submit-form" />,
}));

// Capture the options the page passes to the monitoring hook, so the test can
// prove no failure callback is wired into it, and control the state it reports.
let capturedOptions: { onError?: (error: Error) => void } | undefined;
let optionsWereCaptured = false;
let mockStatus = "idle";
let mockError: Error | undefined = undefined;
vi.mock("../../hooks", () => ({
  useBulkSubmit: (options?: { onError?: (error: Error) => void }) => {
    capturedOptions = options;
    optionsWereCaptured = true;
    return { status: mockStatus, error: mockError };
  },
}));

describe("BulkSubmit page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    capturedOptions = undefined;
    optionsWereCaptured = false;
    mockStatus = "idle";
    mockError = undefined;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // FR-002 and FR-012: the monitor card displays the failure, so the page wires
  // no failure callback into the hook and nothing is announced.
  it("wires no failure callback into the monitoring hook", () => {
    render(<BulkSubmit />);

    expect(optionsWereCaptured).toBe(true);
    expect(capturedOptions?.onError).toBeUndefined();
    expect(mockShowToast).not.toHaveBeenCalled();
  });

  // FR-001, FR-005 and FR-006: the failure is displayed in the card, through the
  // shared callout, and is announced as an alert.
  it("displays a monitoring failure in the shared callout without a notification", () => {
    mockStatus = "error";
    mockError = new Error("Submission status request failed");

    const { container } = render(<BulkSubmit />);

    expect(screen.getByRole("alert")).toHaveTextContent("Submission status request failed");
    expect(container.querySelector(".rt-CalloutIcon svg")).toBeInTheDocument();
    expect(mockShowToast).not.toHaveBeenCalled();
  });
});

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

import { render } from "../../test/testUtils";
import { BulkSubmit } from "../BulkSubmit";

import type { ReactNode } from "react";

// Mock the toast context, which is where failures must be reported.
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
// drive its error callback directly.
let capturedOnError: ((error: Error) => void) | undefined;
vi.mock("../../hooks", () => ({
  useBulkSubmit: (options: { onError?: (error: Error) => void }) => {
    capturedOnError = options.onError;
    return { status: "idle" };
  },
}));

describe("BulkSubmit page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    capturedOnError = undefined;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // FR-016: a bulk submit failure must produce a visible message.
  it("reports a bulk submit failure to the user", () => {
    render(<BulkSubmit />);

    capturedOnError?.(new Error("Submission status request failed"));

    expect(mockShowToast).toHaveBeenCalledWith(
      "Bulk submit failed",
      "Submission status request failed",
    );
  });
});

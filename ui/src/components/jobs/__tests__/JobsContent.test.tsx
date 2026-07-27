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
 * Tests for the JobsContent component, which holds the job list query and the
 * cancellation behaviour previously kept in the jobs page.
 *
 * @author John Grimes
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, waitFor } from "../../../test/testUtils";
import { JobsContent } from "../JobsContent";

import type { JobSummary } from "../../../api";

const runningJob: JobSummary = {
  id: "job-running",
  operation: "export",
  status: "in-progress",
  startTime: "2026-07-27T00:00:00Z",
  url: "https://fhir.example.org/fhir/$job?id=job-running",
};

const finishedJob: JobSummary = {
  id: "job-finished",
  operation: "import",
  status: "completed",
  startTime: "2026-07-27T00:00:00Z",
  url: "https://fhir.example.org/fhir/$job?id=job-finished",
};

// Mock the toast context, used to report the outcome of a cancellation.
const mockShowToast = vi.fn();
vi.mock("../../../contexts/ToastContext", () => ({
  useToast: () => ({ showToast: mockShowToast }),
}));

vi.mock("../../../contexts/AuthContext", () => ({
  useAuth: () => ({ client: null }),
}));

vi.mock("../../../config", () => ({
  config: { fhirBaseUrl: "https://fhir.example.org/fhir" },
}));

// Mock the job list query, toggled per test.
let mockJobsQuery: { data?: JobSummary[]; isLoading: boolean; error: Error | null } = {
  data: [],
  isLoading: false,
  error: null,
};
const mockRefetch = vi.fn();
vi.mock("../../../hooks", () => ({
  JOBS_QUERY_KEY: ["jobs"],
  useJobsList: () => ({ ...mockJobsQuery, refetch: mockRefetch }),
}));

// Mock the cancellation API so no request is made.
const mockJobCancel = vi.fn();
vi.mock("../../../api", () => ({
  jobCancel: (...args: unknown[]) => mockJobCancel(...args),
}));

/**
 * Renders the component with the query client its cancellation mutation needs.
 *
 * @returns The render result.
 */
function renderJobsContent() {
  const queryClient = new QueryClient({
    defaultOptions: { mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <JobsContent />
    </QueryClientProvider>,
  );
}

describe("JobsContent", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockJobsQuery = { data: [], isLoading: false, error: null };
    mockJobCancel.mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // FR-014: listing continues to behave as it does today.
  it("lists the jobs returned by the query", () => {
    mockJobsQuery = { data: [runningJob, finishedJob], isLoading: false, error: null };

    renderJobsContent();

    expect(screen.getByText("export")).toBeInTheDocument();
    expect(screen.getByText("import")).toBeInTheDocument();
  });

  it("shows the loading state while the first list is loading", () => {
    mockJobsQuery = { data: undefined, isLoading: true, error: null };

    renderJobsContent();

    expect(screen.getByText("Loading jobs...")).toBeInTheDocument();
  });

  it("shows the error state when the list cannot be loaded", () => {
    mockJobsQuery = { data: undefined, isLoading: false, error: new Error("request timed out") };

    renderJobsContent();

    expect(screen.getByRole("alert")).toHaveTextContent("request timed out");
  });

  // FR-014: an in-progress job is confirmed before it is stopped.
  it("asks for confirmation before cancelling an in-progress job", async () => {
    const user = userEvent.setup();
    mockJobsQuery = { data: [runningJob], isLoading: false, error: null };

    renderJobsContent();

    await user.click(screen.getByRole("button", { name: /cancel/i }));

    expect(screen.getByText("Cancel job?")).toBeInTheDocument();
    expect(mockJobCancel).not.toHaveBeenCalled();
  });

  // FR-014: a finished job is removed straight away.
  it("removes a finished job without confirmation", async () => {
    const user = userEvent.setup();
    mockJobsQuery = { data: [finishedJob], isLoading: false, error: null };

    renderJobsContent();

    await user.click(screen.getByRole("button", { name: /remove/i }));

    await waitFor(() => {
      expect(mockJobCancel).toHaveBeenCalledTimes(1);
    });
    expect(screen.queryByText("Cancel job?")).not.toBeInTheDocument();
  });
});

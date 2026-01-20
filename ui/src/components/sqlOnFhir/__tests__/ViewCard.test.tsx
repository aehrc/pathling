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
 * Tests for the ViewCard component.
 *
 * This test suite verifies that the ViewCard component correctly displays
 * view job information, manages the view execution lifecycle, handles
 * different states (running, complete, error), and supports export operations.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, waitFor } from "../../../test/testUtils";
import { ViewCard } from "../ViewCard";

import type { ViewJob } from "../../../types/viewJob";

// Define mock functions at module level.
const mockExecute = vi.fn();
let mockStatus: "idle" | "pending" | "success" | "error" = "idle";
let mockResult: { columns: string[]; rows: Record<string, unknown>[] } | undefined = undefined;
let mockError: Error | undefined = undefined;

// Mock useViewRun hook.
vi.mock("../../../hooks", () => ({
  useViewRun: () => ({
    execute: mockExecute,
    status: mockStatus,
    result: mockResult,
    error: mockError,
  }),
}));

// Mock useAuth hook.
vi.mock("../../../contexts/AuthContext", () => ({
  useAuth: () => ({
    client: { state: { tokenResponse: { access_token: "mock-token" } } },
  }),
}));

// Mock config.
vi.mock("../../../config", () => ({
  config: {
    fhirBaseUrl: "https://fhir.example.org/fhir",
  },
}));

// Mock the API read function.
vi.mock("../../../api", () => ({
  read: vi.fn().mockResolvedValue({ resourceType: "ViewDefinition", name: "test-view" }),
}));

// Mock the ViewExportCardWrapper to simplify testing.
vi.mock("../ViewExportCardWrapper", () => ({
  ViewExportCardWrapper: ({ format }: { format: string }) => (
    <div data-testid="export-card-wrapper">Export: {format}</div>
  ),
}));

// Mock ExportControls component.
vi.mock("../ExportControls", () => ({
  ExportControls: ({
    onExport,
    disabled,
  }: {
    onExport: (format: string) => void;
    disabled: boolean;
  }) => (
    <button data-testid="export-controls" onClick={() => onExport("csv")} disabled={disabled}>
      Export
    </button>
  ),
}));

// Mock formatDateTime utility.
vi.mock("../../../utils", () => ({
  formatDateTime: () => "15 Jan 2024, 10:00 AM",
}));

describe("ViewCard", () => {
  const defaultOnError = vi.fn();
  const defaultOnClose = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus = "idle";
    mockResult = undefined;
    mockError = undefined;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  function createJob(overrides: Partial<ViewJob> = {}): ViewJob {
    return {
      id: "test-job-1",
      mode: "stored",
      viewDefinitionId: "view-def-123",
      limit: 10,
      createdAt: new Date("2024-01-15T10:00:00Z"),
      ...overrides,
    };
  }

  describe("Rendering", () => {
    it("displays stored view mode label", () => {
      const job = createJob({ mode: "stored" });

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByText("Run stored view definition")).toBeInTheDocument();
    });

    it("displays inline view mode label", () => {
      const job = createJob({
        mode: "inline",
        viewDefinitionJson: '{"name": "test"}',
        viewDefinitionId: undefined,
      });

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByText("Run provided view definition")).toBeInTheDocument();
    });

    it("displays job ID", () => {
      const job = createJob({ id: "my-view-job-id" });

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByText("Job ID: my-view-job-id")).toBeInTheDocument();
    });

    it("displays view definition ID when present", () => {
      const job = createJob({ viewDefinitionId: "my-view-def" });

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByText("View definition ID: my-view-def")).toBeInTheDocument();
    });

    it("does not display view definition ID when not present", () => {
      const job = createJob({
        mode: "inline",
        viewDefinitionId: undefined,
        viewDefinitionJson: "{}",
      });

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.queryByText(/View definition ID:/)).not.toBeInTheDocument();
    });

    it("displays formatted creation date", () => {
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByText("15 Jan 2024, 10:00 AM")).toBeInTheDocument();
    });
  });

  describe("Execution lifecycle", () => {
    it("executes view on mount", async () => {
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      await waitFor(() => {
        expect(mockExecute).toHaveBeenCalledWith({
          mode: "stored",
          viewDefinitionId: "view-def-123",
          viewDefinitionJson: undefined,
          limit: 10,
        });
      });
    });

    it("displays loading state when status is pending", () => {
      mockStatus = "pending";
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByText("Executing view definition...")).toBeInTheDocument();
    });

    it("displays error message when execution fails", () => {
      mockStatus = "error";
      mockError = new Error("View execution failed");
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByText("View run failed: View execution failed")).toBeInTheDocument();
    });

    it("reports error to parent when execution fails", async () => {
      mockStatus = "error";
      mockError = new Error("Execution error");
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      await waitFor(() => {
        expect(defaultOnError).toHaveBeenCalledWith("Execution error");
      });
    });

    it("displays no rows message when result is empty", () => {
      mockStatus = "success";
      mockResult = { columns: ["id", "name"], rows: [] };
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByText("No rows returned.")).toBeInTheDocument();
    });
  });

  describe("Results display", () => {
    it("displays results table when execution succeeds with rows", () => {
      mockStatus = "success";
      mockResult = {
        columns: ["id", "name"],
        rows: [
          { id: "1", name: "Test Patient" },
          { id: "2", name: "Another Patient" },
        ],
      };
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByText("2 rows (first 10)")).toBeInTheDocument();
      expect(screen.getByText("id")).toBeInTheDocument();
      expect(screen.getByText("name")).toBeInTheDocument();
      expect(screen.getByText("1")).toBeInTheDocument();
      expect(screen.getByText("Test Patient")).toBeInTheDocument();
    });

    it("formats object values as JSON strings", () => {
      mockStatus = "success";
      mockResult = {
        columns: ["data"],
        rows: [{ data: { nested: "value" } }],
      };
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByText('{"nested":"value"}')).toBeInTheDocument();
    });

    it("displays empty string for null values", () => {
      mockStatus = "success";
      mockResult = {
        columns: ["value"],
        rows: [{ value: null }],
      };
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      // The cell should exist but contain empty content.
      const cells = screen.getAllByRole("cell");
      expect(cells[0]).toHaveTextContent("");
    });
  });

  describe("Close button", () => {
    it("shows Close button when status is success", () => {
      mockStatus = "success";
      mockResult = { columns: [], rows: [] };
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });

    it("shows Close button when status is error", () => {
      mockStatus = "error";
      mockError = new Error("Test error");
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });

    it("does not show Close button when status is pending", () => {
      mockStatus = "pending";
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.queryByRole("button", { name: /close/i })).not.toBeInTheDocument();
    });

    it("does not show Close button when onClose is not provided", () => {
      mockStatus = "success";
      mockResult = { columns: [], rows: [] };
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} />);

      expect(screen.queryByRole("button", { name: /close/i })).not.toBeInTheDocument();
    });

    it("calls onClose when Close button is clicked", async () => {
      const user = userEvent.setup();
      mockStatus = "success";
      mockResult = { columns: [], rows: [] };
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      const closeButton = screen.getByRole("button", { name: /close/i });
      await user.click(closeButton);

      expect(defaultOnClose).toHaveBeenCalledTimes(1);
    });
  });

  describe("Export functionality", () => {
    it("shows export controls when results have rows", () => {
      mockStatus = "success";
      mockResult = {
        columns: ["id"],
        rows: [{ id: "1" }],
      };
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.getByTestId("export-controls")).toBeInTheDocument();
    });

    it("does not show export controls when results are empty", () => {
      mockStatus = "success";
      mockResult = { columns: [], rows: [] };
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      expect(screen.queryByTestId("export-controls")).not.toBeInTheDocument();
    });

    it("creates export card when export is triggered", async () => {
      const user = userEvent.setup();
      mockStatus = "success";
      mockResult = {
        columns: ["id"],
        rows: [{ id: "1" }],
      };
      const job = createJob();

      render(<ViewCard job={job} onError={defaultOnError} onClose={defaultOnClose} />);

      const exportButton = screen.getByTestId("export-controls");
      await user.click(exportButton);

      await waitFor(() => {
        expect(screen.getByTestId("export-card-wrapper")).toBeInTheDocument();
      });
    });
  });
});

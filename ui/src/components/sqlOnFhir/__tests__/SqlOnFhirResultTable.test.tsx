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
 * Tests for the SqlOnFhirResultTable component.
 *
 * This test suite verifies that the SqlOnFhirResultTable correctly displays
 * different states (initial, loading, error, empty, results) and handles
 * export functionality.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { SqlOnFhirResultTable } from "../SqlOnFhirResultTable";

import type { ViewExportJob } from "../../../types/job";

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

// Mock ViewExportCard component.
vi.mock("../ViewExportCard", () => ({
  ViewExportCard: ({ job }: { job: ViewExportJob }) => (
    <div data-testid="view-export-card">Export Job: {job.status}</div>
  ),
}));

describe("SqlOnFhirResultTable", () => {
  const defaultOnExport = vi.fn();
  const defaultOnDownload = vi.fn();
  const defaultOnCancelExport = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("Initial state", () => {
    it("displays prompt to execute when hasExecuted is false", () => {
      render(
        <SqlOnFhirResultTable
          rows={undefined}
          columns={undefined}
          isLoading={false}
          error={null}
          hasExecuted={false}
        />,
      );

      expect(screen.getByText("Results")).toBeInTheDocument();
      expect(screen.getByText("Execute a view definition to view results.")).toBeInTheDocument();
    });
  });

  describe("Loading state", () => {
    it("displays loading message when isLoading is true", () => {
      render(
        <SqlOnFhirResultTable
          rows={undefined}
          columns={undefined}
          isLoading={true}
          error={null}
          hasExecuted={true}
        />,
      );

      expect(screen.getByText("Results")).toBeInTheDocument();
      expect(screen.getByText("Executing view definition...")).toBeInTheDocument();
    });
  });

  describe("Error state", () => {
    it("displays error message when error is present", () => {
      const testError = new Error("View execution failed");

      render(
        <SqlOnFhirResultTable
          rows={undefined}
          columns={undefined}
          isLoading={false}
          error={testError}
          hasExecuted={true}
        />,
      );

      expect(screen.getByText("Results")).toBeInTheDocument();
      expect(screen.getByText("View execution failed")).toBeInTheDocument();
    });
  });

  describe("Empty results", () => {
    it("displays no rows message when rows array is empty", () => {
      render(
        <SqlOnFhirResultTable
          rows={[]}
          columns={["id", "name"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
        />,
      );

      expect(screen.getByText("Results")).toBeInTheDocument();
      expect(screen.getByText("No rows returned.")).toBeInTheDocument();
    });

    it("displays no rows message when rows is undefined after execution", () => {
      render(
        <SqlOnFhirResultTable
          rows={undefined}
          columns={undefined}
          isLoading={false}
          error={null}
          hasExecuted={true}
        />,
      );

      expect(screen.getByText("No rows returned.")).toBeInTheDocument();
    });
  });

  describe("Results display", () => {
    it("displays table with column headers", () => {
      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1", name: "Test" }]}
          columns={["id", "name"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
        />,
      );

      expect(screen.getByText("id")).toBeInTheDocument();
      expect(screen.getByText("name")).toBeInTheDocument();
    });

    it("displays table with row data", () => {
      render(
        <SqlOnFhirResultTable
          rows={[
            { id: "1", name: "First" },
            { id: "2", name: "Second" },
          ]}
          columns={["id", "name"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
        />,
      );

      expect(screen.getByText("1")).toBeInTheDocument();
      expect(screen.getByText("First")).toBeInTheDocument();
      expect(screen.getByText("2")).toBeInTheDocument();
      expect(screen.getByText("Second")).toBeInTheDocument();
    });

    it("displays row count badge", () => {
      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }, { id: "2" }, { id: "3" }]}
          columns={["id"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
        />,
      );

      expect(screen.getByText("3 rows (first 10)")).toBeInTheDocument();
    });

    it("formats object values as JSON strings", () => {
      render(
        <SqlOnFhirResultTable
          rows={[{ data: { nested: "value" } }]}
          columns={["data"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
        />,
      );

      expect(screen.getByText('{"nested":"value"}')).toBeInTheDocument();
    });

    it("displays empty string for null values", () => {
      render(
        <SqlOnFhirResultTable
          rows={[{ value: null }]}
          columns={["value"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
        />,
      );

      // The cell should exist but contain empty content.
      const cells = screen.getAllByRole("cell");
      expect(cells[0]).toHaveTextContent("");
    });

    it("displays empty string for undefined values", () => {
      render(
        <SqlOnFhirResultTable
          rows={[{ other: "value" }]}
          columns={["missing"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
        />,
      );

      const cells = screen.getAllByRole("cell");
      expect(cells[0]).toHaveTextContent("");
    });
  });

  describe("Export functionality", () => {
    it("shows export controls when onExport is provided", () => {
      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }]}
          columns={["id"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
          onExport={defaultOnExport}
        />,
      );

      expect(screen.getByTestId("export-controls")).toBeInTheDocument();
    });

    it("does not show export controls when onExport is not provided", () => {
      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }]}
          columns={["id"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
        />,
      );

      expect(screen.queryByTestId("export-controls")).not.toBeInTheDocument();
    });

    it("enables export controls when no export job is active", () => {
      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }]}
          columns={["id"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
          onExport={defaultOnExport}
          exportJob={null}
          isExporting={false}
        />,
      );

      expect(screen.getByTestId("export-controls")).not.toBeDisabled();
    });

    it("disables export controls when isExporting is true", () => {
      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }]}
          columns={["id"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
          onExport={defaultOnExport}
          isExporting={true}
        />,
      );

      expect(screen.getByTestId("export-controls")).toBeDisabled();
    });

    it("enables export when job is completed", () => {
      const completedJob: ViewExportJob = {
        id: "job-1",
        type: "view-export",
        pollUrl: null,
        status: "completed",
        progress: null,
        error: null,
        request: { format: "csv" },
        manifest: null,
        createdAt: new Date(),
      };

      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }]}
          columns={["id"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
          onExport={defaultOnExport}
          exportJob={completedJob}
          isExporting={false}
        />,
      );

      expect(screen.getByTestId("export-controls")).not.toBeDisabled();
    });

    it("disables export when job is in_progress", () => {
      const inProgressJob: ViewExportJob = {
        id: "job-1",
        type: "view-export",
        pollUrl: null,
        status: "in_progress",
        progress: 50,
        error: null,
        request: { format: "csv" },
        manifest: null,
        createdAt: new Date(),
      };

      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }]}
          columns={["id"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
          onExport={defaultOnExport}
          exportJob={inProgressJob}
          isExporting={false}
        />,
      );

      expect(screen.getByTestId("export-controls")).toBeDisabled();
    });

    it("calls onExport when export button is clicked", async () => {
      const user = userEvent.setup();

      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }]}
          columns={["id"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
          onExport={defaultOnExport}
        />,
      );

      await user.click(screen.getByTestId("export-controls"));

      expect(defaultOnExport).toHaveBeenCalledWith("csv");
    });
  });

  describe("Export job display", () => {
    it("shows ViewExportCard when export job is present with required handlers", () => {
      const exportJob: ViewExportJob = {
        id: "job-1",
        type: "view-export",
        pollUrl: null,
        status: "in_progress",
        progress: 50,
        error: null,
        request: { format: "csv" },
        manifest: null,
        createdAt: new Date(),
      };

      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }]}
          columns={["id"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
          onExport={defaultOnExport}
          exportJob={exportJob}
          onDownload={defaultOnDownload}
          onCancelExport={defaultOnCancelExport}
        />,
      );

      expect(screen.getByTestId("view-export-card")).toBeInTheDocument();
      expect(screen.getByText("Export Job: in_progress")).toBeInTheDocument();
    });

    it("does not show ViewExportCard when exportJob is null", () => {
      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }]}
          columns={["id"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
          onExport={defaultOnExport}
          exportJob={null}
          onDownload={defaultOnDownload}
          onCancelExport={defaultOnCancelExport}
        />,
      );

      expect(screen.queryByTestId("view-export-card")).not.toBeInTheDocument();
    });

    it("does not show ViewExportCard when onDownload is not provided", () => {
      const exportJob: ViewExportJob = {
        id: "job-1",
        type: "view-export",
        pollUrl: null,
        status: "completed",
        progress: null,
        error: null,
        request: { format: "csv" },
        manifest: null,
        createdAt: new Date(),
      };

      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }]}
          columns={["id"]}
          isLoading={false}
          error={null}
          hasExecuted={true}
          onExport={defaultOnExport}
          exportJob={exportJob}
          onCancelExport={defaultOnCancelExport}
        />,
      );

      expect(screen.queryByTestId("view-export-card")).not.toBeInTheDocument();
    });
  });

  describe("State priority", () => {
    // These tests verify that states are rendered in the correct priority order.

    it("shows initial state even when hasExecuted is false with other props set", () => {
      render(
        <SqlOnFhirResultTable
          rows={[{ id: "1" }]}
          columns={["id"]}
          isLoading={true}
          error={new Error("test")}
          hasExecuted={false}
        />,
      );

      expect(screen.getByText("Execute a view definition to view results.")).toBeInTheDocument();
    });

    it("shows loading state before error when both are set", () => {
      render(
        <SqlOnFhirResultTable
          rows={undefined}
          columns={undefined}
          isLoading={true}
          error={new Error("Some error")}
          hasExecuted={true}
        />,
      );

      expect(screen.getByText("Executing view definition...")).toBeInTheDocument();
      expect(screen.queryByText("Some error")).not.toBeInTheDocument();
    });

    it("shows error state before empty results", () => {
      render(
        <SqlOnFhirResultTable
          rows={[]}
          columns={[]}
          isLoading={false}
          error={new Error("Error takes priority")}
          hasExecuted={true}
        />,
      );

      expect(screen.getByText("Error takes priority")).toBeInTheDocument();
      expect(screen.queryByText("No rows returned.")).not.toBeInTheDocument();
    });
  });
});

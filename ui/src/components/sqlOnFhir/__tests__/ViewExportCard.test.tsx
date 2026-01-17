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
 * Tests for the ViewExportCard component's Delete button functionality.
 *
 * @author John Grimes
 */

import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ViewExportCard } from "../ViewExportCard";

import type { ViewExportJob } from "../../../types/job";

describe("ViewExportCard", () => {
  const defaultOnCancel = vi.fn();
  const defaultOnDownload = vi.fn();
  const defaultOnClose = vi.fn();
  const defaultOnDelete = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  function createJob(overrides: Partial<ViewExportJob> = {}): ViewExportJob {
    return {
      id: "test-job-1",
      type: "view-export",
      pollUrl: "http://localhost/fhir/$job?id=test",
      status: "pending",
      progress: null,
      error: null,
      request: { format: "csv" },
      manifest: null,
      createdAt: new Date("2024-01-15T10:00:00Z"),
      ...overrides,
    };
  }

  // Create a valid FHIR Parameters manifest for completed jobs.
  const completedManifest = { resourceType: "Parameters" as const, parameter: [] };

  describe("Title", () => {
    it("displays 'Export to CSV' for CSV format", () => {
      const job = createJob({ request: { format: "csv" } });

      render(
        <ViewExportCard job={job} onCancel={defaultOnCancel} onDownload={defaultOnDownload} />,
      );

      expect(screen.getByText("Export to CSV")).toBeInTheDocument();
    });

    it("displays 'Export to NDJSON' for NDJSON format", () => {
      const job = createJob({ request: { format: "ndjson" } });

      render(
        <ViewExportCard job={job} onCancel={defaultOnCancel} onDownload={defaultOnDownload} />,
      );

      expect(screen.getByText("Export to NDJSON")).toBeInTheDocument();
    });

    it("displays 'Export to Parquet' for Parquet format", () => {
      const job = createJob({ request: { format: "parquet" } });

      render(
        <ViewExportCard job={job} onCancel={defaultOnCancel} onDownload={defaultOnDownload} />,
      );

      expect(screen.getByText("Export to Parquet")).toBeInTheDocument();
    });
  });

  describe("Delete button", () => {
    it("shows both Delete and Close buttons when status is completed", () => {
      const job = createJob({
        status: "completed",
        manifest: completedManifest,
      });

      render(
        <ViewExportCard
          job={job}
          onCancel={defaultOnCancel}
          onDownload={defaultOnDownload}
          onClose={defaultOnClose}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByRole("button", { name: /delete/i })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });

    it("shows only Close button when status is cancelled", () => {
      const job = createJob({ status: "cancelled" });

      render(
        <ViewExportCard
          job={job}
          onCancel={defaultOnCancel}
          onDownload={defaultOnDownload}
          onClose={defaultOnClose}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.queryByRole("button", { name: /delete/i })).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });

    it("shows only Close button when status is failed", () => {
      const job = createJob({
        status: "failed",
        error: new Error("Test error"),
      });

      render(
        <ViewExportCard
          job={job}
          onCancel={defaultOnCancel}
          onDownload={defaultOnDownload}
          onClose={defaultOnClose}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.queryByRole("button", { name: /delete/i })).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });

    it("calls onDelete then onClose when Delete button is clicked", async () => {
      const user = userEvent.setup();
      defaultOnDelete.mockResolvedValue(undefined);

      const job = createJob({
        status: "completed",
        manifest: completedManifest,
      });

      render(
        <ViewExportCard
          job={job}
          onCancel={defaultOnCancel}
          onDownload={defaultOnDownload}
          onClose={defaultOnClose}
          onDelete={defaultOnDelete}
        />,
      );

      const deleteButton = screen.getByRole("button", { name: /delete/i });
      await user.click(deleteButton);

      await waitFor(() => {
        expect(defaultOnDelete).toHaveBeenCalledTimes(1);
      });
      expect(defaultOnClose).toHaveBeenCalledTimes(1);

      // Verify onDelete was called before onClose.
      expect(defaultOnDelete.mock.invocationCallOrder[0]).toBeLessThan(
        defaultOnClose.mock.invocationCallOrder[0],
      );
    });

    it("calls only onClose when Close button is clicked (not onDelete)", async () => {
      const user = userEvent.setup();

      const job = createJob({
        status: "completed",
        manifest: completedManifest,
      });

      render(
        <ViewExportCard
          job={job}
          onCancel={defaultOnCancel}
          onDownload={defaultOnDownload}
          onClose={defaultOnClose}
          onDelete={defaultOnDelete}
        />,
      );

      const closeButton = screen.getByRole("button", { name: /close/i });
      await user.click(closeButton);

      expect(defaultOnClose).toHaveBeenCalledTimes(1);
      expect(defaultOnDelete).not.toHaveBeenCalled();
    });

    it("shows loading state during deletion", async () => {
      const user = userEvent.setup();

      // Create a promise that we can control.
      let resolveDelete: () => void;
      const deletePromise = new Promise<void>((resolve) => {
        resolveDelete = resolve;
      });
      defaultOnDelete.mockImplementation(() => deletePromise);

      const job = createJob({
        status: "completed",
        manifest: completedManifest,
      });

      render(
        <ViewExportCard
          job={job}
          onCancel={defaultOnCancel}
          onDownload={defaultOnDownload}
          onClose={defaultOnClose}
          onDelete={defaultOnDelete}
        />,
      );

      const deleteButton = screen.getByRole("button", { name: /delete/i });
      await user.click(deleteButton);

      // The button should show loading state (be disabled).
      await waitFor(() => {
        expect(deleteButton).toBeDisabled();
      });

      // Resolve the delete.
      resolveDelete!();

      await waitFor(() => {
        expect(defaultOnClose).toHaveBeenCalled();
      });
    });

    it("shows Cancel button when status is pending or in_progress", () => {
      const job = createJob({
        status: "in_progress",
        progress: 50,
      });

      render(
        <ViewExportCard
          job={job}
          onCancel={defaultOnCancel}
          onDownload={defaultOnDownload}
          onClose={defaultOnClose}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByRole("button", { name: /cancel/i })).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: /delete/i })).not.toBeInTheDocument();
      expect(screen.queryByRole("button", { name: /close/i })).not.toBeInTheDocument();
    });

    it("does not show Delete button when onDelete is not provided", () => {
      const job = createJob({
        status: "completed",
        manifest: completedManifest,
      });

      render(
        <ViewExportCard
          job={job}
          onCancel={defaultOnCancel}
          onDownload={defaultOnDownload}
          onClose={defaultOnClose}
        />,
      );

      expect(screen.queryByRole("button", { name: /delete/i })).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });
  });
});

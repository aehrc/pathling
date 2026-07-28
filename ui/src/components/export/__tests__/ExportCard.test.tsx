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
 * Tests for the ExportCard component.
 *
 * @author John Grimes
 */

import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ExportCard } from "../ExportCard";

import type { ExportRequest } from "../../../types/export";

// Define mock functions at module level.
const mockStartWith = vi.fn();
const mockCancel = vi.fn();
const mockDeleteJob = vi.fn();
const mockDownload = vi.fn();
const mockReset = vi.fn();

// Track the mock status state.
let mockStatus: string = "idle";
let mockResult: object | undefined = undefined;
let mockError: Error | undefined = undefined;

// Captures the failure handler the card gives the download hook, so that a
// download failure can be reported to the card as the hook would report it.
const mockDownloadFile = vi.fn();
let reportDownloadFailure: ((error: Error) => void) | undefined = undefined;

// A download failure is the one failure no card can display, so it is the one
// that must still raise a notification.
const mockShowToast = vi.fn();
vi.mock("../../../contexts/ToastContext", () => ({
  useToast: () => ({ showToast: mockShowToast }),
}));

// Mock useBulkExport hook with factory function.
vi.mock("../../../hooks", () => ({
  useBulkExport: () => ({
    startWith: mockStartWith,
    cancel: mockCancel,
    deleteJob: mockDeleteJob,
    download: mockDownload,
    reset: mockReset,
    status: mockStatus,
    result: mockResult,
    error: mockError,
    progress: undefined,
    request: undefined,
  }),
  useDownloadFile: (onError?: (error: Error) => void) => {
    reportDownloadFailure = onError;
    return mockDownloadFile;
  },
}));

/** An export manifest carrying one output file, so a download can be started. */
const manifestWithOutput = {
  resourceType: "Parameters",
  parameter: [
    {
      name: "output",
      part: [
        { name: "type", valueCode: "Patient" },
        { name: "url", valueUri: "https://example.com/result?file=Patient.ndjson" },
      ],
    },
  ],
};

describe("ExportCard", () => {
  const defaultRequest: ExportRequest = {
    level: "system",
    resourceTypes: ["Patient", "Observation"],
    outputFormat: "ndjson",
  };
  const defaultCreatedAt = new Date("2024-01-15T10:00:00Z");
  const defaultOnClose = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus = "idle";
    mockResult = undefined;
    mockError = undefined;
    reportDownloadFailure = undefined;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("Delete button", () => {
    it("shows both Delete and Close buttons when status is completed", () => {
      mockStatus = "complete";
      mockResult = { resourceType: "Parameters", parameter: [] };

      render(
        <ExportCard
          request={defaultRequest}
          createdAt={defaultCreatedAt}
          onClose={defaultOnClose}
        />,
      );

      expect(screen.getByRole("button", { name: /delete/i })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });

    it("shows only Close button when status is cancelled", () => {
      mockStatus = "cancelled";

      render(
        <ExportCard
          request={defaultRequest}
          createdAt={defaultCreatedAt}
          onClose={defaultOnClose}
        />,
      );

      expect(screen.queryByRole("button", { name: /delete/i })).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });

    it("shows only Close button when status is error", () => {
      mockStatus = "error";
      mockError = new Error("Test error");

      render(
        <ExportCard
          request={defaultRequest}
          createdAt={defaultCreatedAt}
          onClose={defaultOnClose}
        />,
      );

      expect(screen.queryByRole("button", { name: /delete/i })).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });

    it("calls deleteJob then onClose when Delete button is clicked", async () => {
      const user = userEvent.setup();
      mockStatus = "complete";
      mockResult = { resourceType: "Parameters", parameter: [] };
      mockDeleteJob.mockResolvedValue(undefined);

      render(
        <ExportCard
          request={defaultRequest}
          createdAt={defaultCreatedAt}
          onClose={defaultOnClose}
        />,
      );

      const deleteButton = screen.getByRole("button", { name: /delete/i });
      await user.click(deleteButton);

      await waitFor(() => {
        expect(mockDeleteJob).toHaveBeenCalledTimes(1);
      });
      expect(defaultOnClose).toHaveBeenCalledTimes(1);

      // Verify deleteJob was called before onClose.
      expect(mockDeleteJob.mock.invocationCallOrder[0]).toBeLessThan(
        defaultOnClose.mock.invocationCallOrder[0],
      );
    });

    it("calls only onClose when Close button is clicked (not deleteJob)", async () => {
      const user = userEvent.setup();
      mockStatus = "complete";
      mockResult = { resourceType: "Parameters", parameter: [] };

      render(
        <ExportCard
          request={defaultRequest}
          createdAt={defaultCreatedAt}
          onClose={defaultOnClose}
        />,
      );

      const closeButton = screen.getByRole("button", { name: /close/i });
      await user.click(closeButton);

      expect(defaultOnClose).toHaveBeenCalledTimes(1);
      expect(mockDeleteJob).not.toHaveBeenCalled();
    });

    it("shows loading state during deletion", async () => {
      const user = userEvent.setup();
      mockStatus = "complete";
      mockResult = { resourceType: "Parameters", parameter: [] };

      // Create a promise that we can control.
      let resolveDelete: () => void;
      const deletePromise = new Promise<void>((resolve) => {
        resolveDelete = resolve;
      });
      mockDeleteJob.mockImplementation(() => deletePromise);

      render(
        <ExportCard
          request={defaultRequest}
          createdAt={defaultCreatedAt}
          onClose={defaultOnClose}
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

    it("shows Cancel button when status is pending or in-progress", () => {
      mockStatus = "in-progress";

      render(
        <ExportCard
          request={defaultRequest}
          createdAt={defaultCreatedAt}
          onClose={defaultOnClose}
        />,
      );

      expect(screen.getByRole("button", { name: /cancel/i })).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: /delete/i })).not.toBeInTheDocument();
      expect(screen.queryByRole("button", { name: /close/i })).not.toBeInTheDocument();
    });
  });

  // A download failure has no card of its own to appear in, because the card is
  // describing a job that succeeded, so it is reported as a notification
  // (FR-003, FR-004).
  describe("download failure", () => {
    it("raises a notification naming the download", () => {
      mockStatus = "complete";
      mockResult = manifestWithOutput;

      render(
        <ExportCard
          request={defaultRequest}
          createdAt={defaultCreatedAt}
          onClose={defaultOnClose}
        />,
      );
      reportDownloadFailure?.(new Error("Download failed: 403 - Forbidden"));

      expect(mockShowToast).toHaveBeenCalledWith(
        "Download failed",
        "Download failed: 403 - Forbidden",
      );
    });

    it("downloads the output file and raises no notification when it succeeds", async () => {
      const user = userEvent.setup();
      mockStatus = "complete";
      mockResult = manifestWithOutput;

      render(
        <ExportCard
          request={defaultRequest}
          createdAt={defaultCreatedAt}
          onClose={defaultOnClose}
        />,
      );

      await user.click(screen.getByRole("button", { name: /download/i }));

      expect(mockDownloadFile).toHaveBeenCalledWith(
        "https://example.com/result?file=Patient.ndjson",
        "Patient.ndjson",
      );
      expect(mockShowToast).not.toHaveBeenCalled();
    });
  });

  // The export job's own failure is displayed in the card, so it must not also
  // be announced as a notification (FR-001, FR-002).
  describe("export job failure", () => {
    it("displays the failure and raises no notification", () => {
      mockStatus = "error";
      mockError = new Error("Export failed: 500 - Internal error");

      render(
        <ExportCard
          request={defaultRequest}
          createdAt={defaultCreatedAt}
          onClose={defaultOnClose}
        />,
      );

      expect(screen.getByText("Export failed: 500 - Internal error")).toBeInTheDocument();
      expect(mockShowToast).not.toHaveBeenCalled();
    });
  });
});

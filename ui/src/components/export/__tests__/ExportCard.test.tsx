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
 * Tests for the ExportCard component's Delete button functionality.
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
  useDownloadFile: () => vi.fn(),
}));

describe("ExportCard", () => {
  const defaultRequest: ExportRequest = {
    level: "system",
    resourceTypes: ["Patient", "Observation"],
    outputFormat: "ndjson",
  };
  const defaultCreatedAt = new Date("2024-01-15T10:00:00Z");
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

  describe("Delete button", () => {
    it("shows both Delete and Close buttons when status is completed", () => {
      mockStatus = "complete";
      mockResult = { resourceType: "Parameters", parameter: [] };

      render(
        <ExportCard
          request={defaultRequest}
          createdAt={defaultCreatedAt}
          onError={defaultOnError}
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
          onError={defaultOnError}
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
          onError={defaultOnError}
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
          onError={defaultOnError}
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
          onError={defaultOnError}
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
          onError={defaultOnError}
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
          onError={defaultOnError}
          onClose={defaultOnClose}
        />,
      );

      expect(screen.getByRole("button", { name: /cancel/i })).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: /delete/i })).not.toBeInTheDocument();
      expect(screen.queryByRole("button", { name: /close/i })).not.toBeInTheDocument();
    });
  });
});

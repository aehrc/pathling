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
 * Tests for the ViewExportCardWrapper component.
 *
 * This test suite verifies that the ViewExportCardWrapper correctly manages
 * the export lifecycle, starts exports on mount, and passes the correct props
 * to the ViewExportCard component.
 *
 * @author John Grimes
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, waitFor } from "../../../test/testUtils";
import { ViewExportCardWrapper } from "../ViewExportCardWrapper";

import type { ViewDefinition } from "../../../api";
import type { ViewExportOutputFormat } from "../../../hooks";

// Define mock functions at module level.
const mockStartWith = vi.fn();
const mockCancel = vi.fn();
const mockDeleteJob = vi.fn();
let mockStatus: string = "idle";
let mockResult: object | null = null;
let mockError: Error | null = null;
let mockProgress: number | undefined = undefined;
let mockRequest: { format: string } | undefined = undefined;

// Captures the failure handler the wrapper gives the download hook, so that a
// download failure can be reported to it as the hook would report it.
let reportDownloadFailure: ((error: Error) => void) | undefined = undefined;

// A download failure is the one failure no card can display, so it is the one
// that must still raise a notification.
const mockShowToast = vi.fn();
vi.mock("../../../contexts/ToastContext", () => ({
  useToast: () => ({ showToast: mockShowToast }),
}));

// Mock useViewExport hook.
vi.mock("../../../hooks", () => ({
  useViewExport: () => ({
    startWith: mockStartWith,
    cancel: mockCancel,
    deleteJob: mockDeleteJob,
    status: mockStatus,
    result: mockResult,
    error: mockError,
    progress: mockProgress,
    request: mockRequest,
  }),
  useDownloadFile: (onError?: (error: Error) => void) => {
    reportDownloadFailure = onError;
    return vi.fn();
  },
}));

// Track props passed to ViewExportCard.
interface ViewExportCardMockProps {
  job: {
    id: string;
    status: string;
    progress: number | null;
    error: Error | null;
    request: { format: string };
  };
  onCancel: () => void;
  onDownload: () => void;
  onClose: () => void;
  onDelete: () => void;
}

let lastViewExportCardProps: ViewExportCardMockProps | null = null;

// Mock ViewExportCard to capture props.
vi.mock("../ViewExportCard", () => ({
  ViewExportCard: (props: ViewExportCardMockProps) => {
    lastViewExportCardProps = props;
    return (
      <div data-testid="view-export-card">
        <span data-testid="export-status">{props.job.status}</span>
        <span data-testid="export-format">{props.job.request.format}</span>
      </div>
    );
  },
}));

describe("ViewExportCardWrapper", () => {
  const defaultOnClose = vi.fn();

  const defaultViewDefinition: ViewDefinition = {
    resourceType: "ViewDefinition",
    name: "test-view",
    resource: "Patient",
    status: "active",
    select: [],
  };

  /**
   * Renders the wrapper with the default props, overriding only what a test
   * cares about.
   *
   * @param overrides - The props to override.
   * @param overrides.id - The export instance identifier.
   * @param overrides.format - The export output format.
   * @returns The render result.
   */
  function renderWrapper(
    overrides: { id?: string; format?: ViewExportOutputFormat } = {},
  ): ReturnType<typeof render> {
    return render(
      <ViewExportCardWrapper
        id={overrides.id ?? "export-1"}
        viewDefinition={defaultViewDefinition}
        format={overrides.format ?? "csv"}
        createdAt={new Date("2024-01-15T10:00:00Z")}
        onClose={defaultOnClose}
      />,
    );
  }

  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus = "idle";
    mockResult = null;
    mockError = null;
    mockProgress = undefined;
    mockRequest = { format: "csv" };
    lastViewExportCardProps = null;
    reportDownloadFailure = undefined;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("Initialisation", () => {
    it("starts export on mount", async () => {
      renderWrapper();

      await waitFor(() => {
        expect(mockStartWith).toHaveBeenCalledWith({
          views: [{ viewDefinition: defaultViewDefinition }],
          format: "csv",
          header: true,
        });
      });
    });

    it("only starts export once even if re-rendered", async () => {
      const { rerender } = renderWrapper();

      // Rerender with same props.
      rerender(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
        />,
      );

      await waitFor(() => {
        // Should only be called once.
        expect(mockStartWith).toHaveBeenCalledTimes(1);
      });
    });

    it("starts export with different formats", async () => {
      renderWrapper({ format: "ndjson" });

      await waitFor(() => {
        expect(mockStartWith).toHaveBeenCalledWith({
          views: [{ viewDefinition: defaultViewDefinition }],
          format: "ndjson",
          header: true,
        });
      });
    });
  });

  describe("Status mapping", () => {
    it.each([
      ["pending", "in_progress"],
      ["in-progress", "in_progress"],
      ["complete", "completed"],
      ["error", "failed"],
      ["cancelled", "cancelled"],
    ])("maps %s status to %s", (status, expected) => {
      mockStatus = status;
      if (status === "complete") {
        mockResult = { parameter: [] };
      }
      if (status === "error") {
        mockError = new Error("Export failed");
      }

      renderWrapper();

      expect(screen.getByTestId("export-status")).toHaveTextContent(expected);
    });
  });

  describe("Props passed to ViewExportCard", () => {
    it("passes correct id to ViewExportCard", () => {
      renderWrapper({ id: "my-unique-export-id" });

      expect(lastViewExportCardProps?.job.id).toBe("my-unique-export-id");
    });

    it("passes progress to ViewExportCard", () => {
      mockStatus = "in-progress";
      mockProgress = 45;

      renderWrapper();

      expect(lastViewExportCardProps?.job.progress).toBe(45);
    });

    it("passes format from request to ViewExportCard", () => {
      mockRequest = { format: "parquet" };

      renderWrapper({ format: "parquet" });

      expect(screen.getByTestId("export-format")).toHaveTextContent("parquet");
    });

    it("passes cancel function to ViewExportCard", () => {
      renderWrapper();

      expect(lastViewExportCardProps?.onCancel).toBe(mockCancel);
    });

    it("passes delete function to ViewExportCard", () => {
      renderWrapper();

      expect(lastViewExportCardProps?.onDelete).toBe(mockDeleteJob);
    });

    it("passes onClose to ViewExportCard", () => {
      renderWrapper();

      expect(lastViewExportCardProps?.onClose).toBe(defaultOnClose);
    });
  });

  describe("Rendering", () => {
    it("renders ViewExportCard", () => {
      renderWrapper();

      expect(screen.getByTestId("view-export-card")).toBeInTheDocument();
    });
  });

  // A download failure has no card of its own to appear in, because the card is
  // describing a job that succeeded, so it is notified (FR-003, FR-004).
  describe("Download failure", () => {
    it("raises a notification naming the download", () => {
      mockStatus = "complete";
      mockResult = { parameter: [] };

      renderWrapper();
      reportDownloadFailure?.(new Error("Download failed: 403 - Forbidden"));

      expect(mockShowToast).toHaveBeenCalledWith(
        "Download failed",
        "Download failed: 403 - Forbidden",
      );
    });
  });
});

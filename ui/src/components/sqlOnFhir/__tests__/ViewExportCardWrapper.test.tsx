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

// Define mock functions at module level.
const mockStartWith = vi.fn();
const mockCancel = vi.fn();
const mockDeleteJob = vi.fn();
let mockStatus: string = "idle";
let mockResult: object | null = null;
let mockError: Error | null = null;
let mockProgress: number | undefined = undefined;
let mockRequest: { format: string } | undefined = undefined;

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
  useDownloadFile: () => () => {
    // Simulate download.
    return vi.fn();
  },
}));

// Track props passed to ViewExportCard.
interface ViewExportCardMockProps {
  job: {
    id: string;
    status: string;
    progress: number | null;
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
  const defaultOnError = vi.fn();

  const defaultViewDefinition: ViewDefinition = {
    resourceType: "ViewDefinition",
    name: "test-view",
    resource: "Patient",
    status: "active",
    select: [],
  };

  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus = "idle";
    mockResult = null;
    mockError = null;
    mockProgress = undefined;
    mockRequest = { format: "csv" };
    lastViewExportCardProps = null;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("Initialisation", () => {
    it("starts export on mount", async () => {
      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      await waitFor(() => {
        expect(mockStartWith).toHaveBeenCalledWith({
          views: [{ viewDefinition: defaultViewDefinition }],
          format: "csv",
          header: true,
        });
      });
    });

    it("only starts export once even if re-rendered", async () => {
      const { rerender } = render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      // Rerender with same props.
      rerender(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      await waitFor(() => {
        // Should only be called once.
        expect(mockStartWith).toHaveBeenCalledTimes(1);
      });
    });

    it("starts export with different formats", async () => {
      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="ndjson"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

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
    it("maps pending status to in_progress", () => {
      mockStatus = "pending";

      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(screen.getByTestId("export-status")).toHaveTextContent("in_progress");
    });

    it("maps in-progress status to in_progress", () => {
      mockStatus = "in-progress";

      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(screen.getByTestId("export-status")).toHaveTextContent("in_progress");
    });

    it("maps complete status to completed", () => {
      mockStatus = "complete";
      mockResult = { parameter: [] };

      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(screen.getByTestId("export-status")).toHaveTextContent("completed");
    });

    it("maps error status to failed", () => {
      mockStatus = "error";
      mockError = new Error("Export failed");

      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(screen.getByTestId("export-status")).toHaveTextContent("failed");
    });

    it("maps cancelled status to cancelled", () => {
      mockStatus = "cancelled";

      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(screen.getByTestId("export-status")).toHaveTextContent("cancelled");
    });
  });

  describe("Props passed to ViewExportCard", () => {
    it("passes correct id to ViewExportCard", () => {
      render(
        <ViewExportCardWrapper
          id="my-unique-export-id"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(lastViewExportCardProps?.job.id).toBe("my-unique-export-id");
    });

    it("passes progress to ViewExportCard", () => {
      mockStatus = "in-progress";
      mockProgress = 45;

      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(lastViewExportCardProps?.job.progress).toBe(45);
    });

    it("passes format from request to ViewExportCard", () => {
      mockRequest = { format: "parquet" };

      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="parquet"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(screen.getByTestId("export-format")).toHaveTextContent("parquet");
    });

    it("passes cancel function to ViewExportCard", () => {
      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(lastViewExportCardProps?.onCancel).toBe(mockCancel);
    });

    it("passes delete function to ViewExportCard", () => {
      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(lastViewExportCardProps?.onDelete).toBe(mockDeleteJob);
    });

    it("passes onClose to ViewExportCard", () => {
      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(lastViewExportCardProps?.onClose).toBe(defaultOnClose);
    });
  });

  describe("Rendering", () => {
    it("renders ViewExportCard", () => {
      render(
        <ViewExportCardWrapper
          id="export-1"
          viewDefinition={defaultViewDefinition}
          format="csv"
          createdAt={new Date("2024-01-15T10:00:00Z")}
          onClose={defaultOnClose}
          onError={defaultOnError}
        />,
      );

      expect(screen.getByTestId("view-export-card")).toBeInTheDocument();
    });
  });
});

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
 * Tests for the SqlExportCardWrapper component, which manages one
 * `$sql-export` job and reports its outcome.
 *
 * @author John Grimes
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, waitFor } from "../../../test/testUtils";
import { SqlExportCardWrapper } from "../SqlExportCardWrapper";

import type { SqlExportEntry } from "../../../api";

// The state of the export job the wrapper is managing.
const mockStartWith = vi.fn();
const mockCancel = vi.fn();
let mockStatus: string = "idle";
let mockResult: object | null = null;
let mockError: Error | null = null;
let mockProgress: number | undefined = undefined;

// Captures the failure handler the wrapper gives the download hook, so that a
// download failure can be reported to it as the hook would report it.
let reportDownloadFailure: ((error: Error) => void) | undefined = undefined;

// A download failure is the one failure no card can display, so it is the one
// that must still raise a notification.
const mockShowToast = vi.fn();
vi.mock("../../../contexts/ToastContext", () => ({
  useToast: () => ({ showToast: mockShowToast }),
}));

// Captures the options the wrapper passes to the export hook, so a test can
// prove no failure callback is wired into it.
let capturedExportOptions: { onError?: (error: Error) => void } | undefined = undefined;

vi.mock("../../../hooks", () => ({
  useSqlExport: (options?: { onError?: (error: Error) => void }) => {
    capturedExportOptions = options;
    return {
      startWith: mockStartWith,
      cancel: mockCancel,
      status: mockStatus,
      result: mockResult,
      error: mockError,
      progress: mockProgress,
    };
  },
  useDownloadFile: (onError?: (error: Error) => void) => {
    reportDownloadFailure = onError;
    return vi.fn();
  },
}));

vi.mock("../../../api", () => ({
  parseSqlExportManifest: () => [],
}));

interface ExportJobCardMockProps {
  job: {
    status: string;
    progress: number | null;
    error: Error | null;
    format: string;
  };
  onCancel: () => void;
  onDownload: (url: string, filename: string) => void;
  onClose: () => void;
}

let lastExportJobCardProps: ExportJobCardMockProps | null = null;

// The presentational card is mocked so that its props can be inspected.
vi.mock("../ExportJobCard", () => ({
  ExportJobCard: (props: ExportJobCardMockProps) => {
    lastExportJobCardProps = props;
    return (
      <div data-testid="export-job-card">
        <span data-testid="export-status">{props.job.status}</span>
        <span data-testid="export-format">{props.job.format}</span>
      </div>
    );
  },
}));

describe("SqlExportCardWrapper", () => {
  const defaultOnClose = vi.fn();
  const subjects: SqlExportEntry[] = [
    { name: "demographics", subject: { kind: "reference", reference: "ViewDefinition/v" } },
    { name: "bp", subject: { kind: "reference", reference: "Library/bp" } },
  ];

  /**
   * Renders the wrapper with the default props, overriding only what a test
   * cares about.
   *
   * @param overrides - The props to override.
   * @param overrides.subjects - The subjects to export.
   * @param overrides.patientIds - The patient filter.
   * @returns The render result.
   */
  function renderWrapper(
    overrides: { subjects?: SqlExportEntry[]; patientIds?: string[] } = {},
  ): ReturnType<typeof render> {
    return render(
      <SqlExportCardWrapper
        subjects={overrides.subjects ?? subjects}
        format="csv"
        patientIds={overrides.patientIds}
        createdAt={new Date("2026-01-15T10:00:00Z")}
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
    lastExportJobCardProps = null;
    reportDownloadFailure = undefined;
    capturedExportOptions = undefined;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("Initialisation", () => {
    // One job carries every subject, so a mixed set kicks off exactly once.
    it("starts one job carrying every subject on mount", async () => {
      renderWrapper({ patientIds: ["p1"] });

      await waitFor(() => {
        expect(mockStartWith).toHaveBeenCalledWith({
          subjects,
          format: "csv",
          header: true,
          patientIds: ["p1"],
          groupIds: undefined,
          since: undefined,
        });
      });
    });

    it("starts the export only once when re-rendered", async () => {
      const { rerender } = renderWrapper();

      rerender(
        <SqlExportCardWrapper
          subjects={subjects}
          format="csv"
          createdAt={new Date("2026-01-15T10:00:00Z")}
          onClose={defaultOnClose}
        />,
      );

      await waitFor(() => {
        expect(mockStartWith).toHaveBeenCalledTimes(1);
      });
    });
  });

  describe("Status mapping", () => {
    it.each([
      ["idle", "pending"],
      ["pending", "in_progress"],
      ["in-progress", "in_progress"],
      ["complete", "completed"],
      ["error", "failed"],
      ["cancelled", "cancelled"],
    ])("maps %s status to %s", (status, expected) => {
      mockStatus = status;

      renderWrapper();

      expect(screen.getByTestId("export-status")).toHaveTextContent(expected);
    });
  });

  describe("Rendering", () => {
    it("renders the shared export job card with the chosen format", () => {
      renderWrapper();

      expect(screen.getByTestId("export-job-card")).toBeInTheDocument();
      expect(screen.getByTestId("export-format")).toHaveTextContent("csv");
    });

    it("passes the cancel and close callbacks to the card", () => {
      renderWrapper();

      expect(lastExportJobCardProps?.onCancel).toBe(mockCancel);
      expect(lastExportJobCardProps?.onClose).toBe(defaultOnClose);
    });
  });

  // A download failure has no card of its own to appear in, because the card is
  // describing a job that succeeded, so it is notified.
  describe("Download failure", () => {
    it("raises a notification naming the download", () => {
      mockStatus = "complete";

      renderWrapper();
      reportDownloadFailure?.(new Error("Download failed: 403 - Forbidden"));

      expect(mockShowToast).toHaveBeenCalledWith(
        "Download failed",
        "Download failed: 403 - Forbidden",
      );
    });
  });

  // The export job's own failure is displayed by the card it is given to, so it
  // must not also be announced as a notification.
  describe("Export job failure", () => {
    it("hands the failure to the card and raises no notification", () => {
      mockStatus = "error";
      mockError = new Error("SQL export failed: 500 - Internal error");

      renderWrapper();
      // Driving whatever callback the wrapper wired in, so that a reinstated
      // notification fails this test rather than passing unnoticed.
      capturedExportOptions?.onError?.(mockError);

      expect(capturedExportOptions?.onError).toBeUndefined();
      expect(lastExportJobCardProps?.job.error).toBe(mockError);
      expect(mockShowToast).not.toHaveBeenCalled();
    });
  });
});

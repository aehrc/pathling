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
 * Tests for the ImportCard component.
 *
 * This test suite verifies that the ImportCard component correctly displays
 * import job information, manages the import lifecycle, handles different
 * states (running, complete, error, cancelled), and shows appropriate controls.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, waitFor } from "../../../test/testUtils";
import { OperationOutcomeError } from "../../../types/errors";
import { ImportCard } from "../ImportCard";

import type { ImportJob, ImportRequest } from "../../../types/import";
import type { ImportPnpRequest } from "../../../types/importPnp";
import type { OperationOutcome } from "fhir/r4";

// A failure the card displays must not also be announced, so the toast is
// mocked to prove it is never called.
const mockShowToast = vi.fn();
vi.mock("../../../contexts/ToastContext", () => ({
  useToast: () => ({ showToast: mockShowToast }),
}));

// Define mock functions for standard import.
const mockStandardStartWith = vi.fn();
const mockStandardCancel = vi.fn();
let mockStandardStatus: string = "idle";
let mockStandardError: Error | undefined = undefined;
let mockStandardProgress: number | undefined = undefined;

// Define mock functions for PnP import.
const mockPnpStartWith = vi.fn();
const mockPnpCancel = vi.fn();
let mockPnpStatus: string = "idle";
let mockPnpError: Error | undefined = undefined;
let mockPnpProgress: number | undefined = undefined;

// Mock useImport hook.
vi.mock("../../../hooks", () => ({
  useImport: () => ({
    startWith: mockStandardStartWith,
    cancel: mockStandardCancel,
    status: mockStandardStatus,
    error: mockStandardError,
    progress: mockStandardProgress,
  }),
  useImportPnp: () => ({
    startWith: mockPnpStartWith,
    cancel: mockPnpCancel,
    status: mockPnpStatus,
    error: mockPnpError,
    progress: mockPnpProgress,
  }),
}));

// Mock formatDateTime utility.
vi.mock("../../../utils", () => ({
  formatDateTime: () => "15 Jan 2024, 10:00 AM",
}));

describe("ImportCard", () => {
  const defaultOnClose = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockStandardStatus = "idle";
    mockStandardError = undefined;
    mockStandardProgress = undefined;
    mockPnpStatus = "idle";
    mockPnpError = undefined;
    mockPnpProgress = undefined;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  function createStandardJob(overrides: Partial<ImportJob> = {}): ImportJob {
    return {
      id: "import-job-1",
      type: "standard",
      request: {
        input: [
          { url: "https://example.org/data.ndjson", type: "Patient" },
          { url: "https://example.org/obs.ndjson", type: "Observation" },
        ],
        saveMode: "merge",
        inputFormat: "application/fhir+ndjson",
      } as ImportRequest,
      createdAt: new Date("2024-01-15T10:00:00Z"),
      ...overrides,
    };
  }

  function createPnpJob(overrides: Partial<ImportJob> = {}): ImportJob {
    return {
      id: "import-pnp-job-1",
      type: "pnp",
      request: {
        exportUrl: "https://source-server.example.org/fhir",
        saveMode: "overwrite",
        inputFormat: "application/fhir+ndjson",
      } as ImportPnpRequest,
      createdAt: new Date("2024-01-15T10:00:00Z"),
      ...overrides,
    };
  }

  describe("Rendering standard import", () => {
    it("displays 'Import from URLs' label for standard import", () => {
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByText("Import from URLs")).toBeInTheDocument();
    });

    it("displays source count for standard import", () => {
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByText("Importing 2 source(s)")).toBeInTheDocument();
    });

    it("displays formatted creation date", () => {
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByText("15 Jan 2024, 10:00 AM")).toBeInTheDocument();
    });
  });

  describe("Rendering PnP import", () => {
    it("displays 'Import from FHIR server' label for PnP import", () => {
      const job = createPnpJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByText("Import from FHIR server")).toBeInTheDocument();
    });

    it("displays source URL for PnP import", () => {
      const job = createPnpJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(
        screen.getByText("Importing from https://source-server.example.org/fhir"),
      ).toBeInTheDocument();
    });
  });

  describe("Import lifecycle", () => {
    it("starts standard import on mount", async () => {
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      await waitFor(() => {
        expect(mockStandardStartWith).toHaveBeenCalledWith({
          sources: ["https://example.org/data.ndjson", "https://example.org/obs.ndjson"],
          resourceTypes: ["Patient", "Observation"],
          saveMode: "merge",
          inputFormat: "application/fhir+ndjson",
        });
      });
    });

    it("starts PnP import on mount", async () => {
      const job = createPnpJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      await waitFor(() => {
        expect(mockPnpStartWith).toHaveBeenCalledWith({
          exportUrl: "https://source-server.example.org/fhir",
          saveMode: "overwrite",
          inputFormat: "application/fhir+ndjson",
          types: undefined,
          since: undefined,
          until: undefined,
          elements: undefined,
          typeFilters: undefined,
          includeAssociatedData: undefined,
        });
      });
    });

    it("only starts import once even if re-rendered", async () => {
      const job = createStandardJob();

      const { rerender } = render(<ImportCard job={job} onClose={defaultOnClose} />);

      rerender(<ImportCard job={job} onClose={defaultOnClose} />);

      await waitFor(() => {
        expect(mockStandardStartWith).toHaveBeenCalledTimes(1);
      });
    });
  });

  describe("Running state", () => {
    it("shows Cancel button when import is pending", () => {
      mockStandardStatus = "pending";
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByRole("button", { name: /cancel/i })).toBeInTheDocument();
    });

    it("shows Cancel button when import is in-progress", () => {
      mockStandardStatus = "in-progress";
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByRole("button", { name: /cancel/i })).toBeInTheDocument();
    });

    it("calls cancel when Cancel button is clicked", async () => {
      const user = userEvent.setup();
      mockStandardStatus = "in-progress";
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      const cancelButton = screen.getByRole("button", { name: /cancel/i });
      await user.click(cancelButton);

      expect(mockStandardCancel).toHaveBeenCalledTimes(1);
    });

    it("shows progress bar when progress is defined", () => {
      mockStandardStatus = "in-progress";
      mockStandardProgress = 75;
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByText("Progress")).toBeInTheDocument();
      expect(screen.getByText("75%")).toBeInTheDocument();
    });

    it("shows processing message when progress is undefined", () => {
      mockStandardStatus = "in-progress";
      mockStandardProgress = undefined;
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByText("Processing...")).toBeInTheDocument();
    });
  });

  describe("Completed state", () => {
    it("shows success message when import completes", () => {
      mockStandardStatus = "complete";
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByText("Import completed successfully")).toBeInTheDocument();
    });

    it("shows Close button when import completes", () => {
      mockStandardStatus = "complete";
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });

    it("calls onClose when Close button is clicked", async () => {
      const user = userEvent.setup();
      mockStandardStatus = "complete";
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      const closeButton = screen.getByRole("button", { name: /close/i });
      await user.click(closeButton);

      expect(defaultOnClose).toHaveBeenCalledTimes(1);
    });

    it("does not show Close button when onClose is not provided", () => {
      mockStandardStatus = "complete";
      const job = createStandardJob();

      render(<ImportCard job={job} />);

      expect(screen.queryByRole("button", { name: /close/i })).not.toBeInTheDocument();
    });
  });

  describe("Error state", () => {
    it("displays error message when import fails", () => {
      mockStandardStatus = "error";
      mockStandardError = new Error("Import failed");
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      // The callout shows the severity badge and the text separately.
      expect(screen.getByText("Error")).toBeInTheDocument();
      expect(screen.getByText("Import failed")).toBeInTheDocument();
    });

    // FR-001 and FR-002: the card displays the failure, so nothing else
    // announces it.
    it("displays the failure without raising a notification", () => {
      mockStandardStatus = "error";
      mockStandardError = new Error("Connection timeout");
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByText("Connection timeout")).toBeInTheDocument();
      expect(mockShowToast).not.toHaveBeenCalled();
    });

    // FR-005 and FR-006: the failure is rendered by the shared callout, so it
    // looks like every other failure and is announced to assistive technology.
    it("displays the failure in the shared callout, announced as an alert", () => {
      mockStandardStatus = "error";
      const outcome: OperationOutcome = {
        resourceType: "OperationOutcome",
        issue: [
          { severity: "error", code: "processing", diagnostics: "Cannot write to existing path" },
        ],
      };
      mockStandardError = new OperationOutcomeError(outcome, 400, "Import kick-off");
      const job = createStandardJob();

      const { container } = render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByRole("alert")).toHaveTextContent("Cannot write to existing path");
      expect(container.querySelector(".rt-CalloutIcon svg")).toBeInTheDocument();
    });

    it("shows Close button when import has error", () => {
      mockStandardStatus = "error";
      mockStandardError = new Error("Error");
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });
  });

  describe("Cancelled state", () => {
    it("shows cancelled message when import is cancelled", () => {
      mockStandardStatus = "cancelled";
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByText("Cancelled")).toBeInTheDocument();
    });

    it("shows Close button when import is cancelled", () => {
      mockStandardStatus = "cancelled";
      const job = createStandardJob();

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument();
    });
  });

  describe("PnP import with optional parameters", () => {
    it("passes optional parameters to PnP import", async () => {
      const job = createPnpJob({
        request: {
          exportUrl: "https://source.example.org/fhir",
          exportType: "dynamic",
          saveMode: "merge",
          inputFormat: "application/fhir+ndjson",
          types: ["Patient", "Observation"],
          since: "2024-01-01",
          until: "2024-12-31",
          elements: "id,name",
          typeFilters: ["Patient?active=true"],
          includeAssociatedData: ["LatestProvenanceResources"],
        } as ImportPnpRequest,
      });

      render(<ImportCard job={job} onClose={defaultOnClose} />);

      await waitFor(() => {
        expect(mockPnpStartWith).toHaveBeenCalledWith({
          exportUrl: "https://source.example.org/fhir",
          saveMode: "merge",
          inputFormat: "application/fhir+ndjson",
          types: ["Patient", "Observation"],
          since: "2024-01-01",
          until: "2024-12-31",
          elements: "id,name",
          typeFilters: ["Patient?active=true"],
          includeAssociatedData: ["LatestProvenanceResources"],
        });
      });
    });
  });
});

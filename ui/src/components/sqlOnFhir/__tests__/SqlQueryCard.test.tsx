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
 * Tests for the SqlQueryCard component.
 *
 * Verifies the format-aware result branching: tabular formats render a
 * preview table (capped at 10 rows), parquet renders an export-pending
 * notice and errors surface in a Callout with the submitted SQL above.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { OperationOutcomeError } from "../../../types/errors";
import { SqlQueryCard } from "../SqlQueryCard";

import type { SqlQueryJob, SqlQueryResult } from "../../../types/sqlQuery";
import type { OperationOutcome } from "fhir/r4";

// A failure the card displays must not also be announced, so the toast is
// mocked to prove it is never called.
const mockShowToast = vi.fn();
vi.mock("../../../contexts/ToastContext", () => ({
  useToast: () => ({ showToast: mockShowToast }),
}));

const mockExecute = vi.fn();
let mockStatus: "idle" | "pending" | "success" | "error" = "idle";
let mockResult: SqlQueryResult | undefined;
let mockError: Error | undefined;

// Captures the options the card passes to its data hook, so a test can prove no
// failure callback is wired into it.
type HookOptions = { onError?: (error: Error) => void } | undefined;
let capturedOptions: HookOptions = undefined;

vi.mock("../../../hooks", () => ({
  useSqlRun: (options?: { onError?: (error: Error) => void }) => {
    capturedOptions = options;
    return {
      execute: mockExecute,
      status: mockStatus,
      result: mockResult,
      error: mockError,
    };
  },
  // The submitted-SQL preview composes useClipboard; the card itself does not
  // exercise it, so a no-op stub suffices here.
  useClipboard: () => vi.fn(),
}));

vi.mock("../../../utils", () => ({
  formatDateTime: () => "15 Jan 2026, 10:00 AM",
}));

const TABULAR_RESULT: SqlQueryResult = {
  kind: "tabular",
  format: "csv",
  columns: ["patient_id", "given_name"],
  rows: [
    { patient_id: "pat-1", given_name: "Alice" },
    { patient_id: "pat-2", given_name: "Bob" },
  ],
  rawBody: new Blob(["patient_id,given_name\npat-1,Alice\npat-2,Bob"], {
    type: "text/csv",
  }),
};

const BINARY_RESULT: SqlQueryResult = {
  kind: "binary",
  format: "parquet",
  blob: new Blob([new Uint8Array([1, 2, 3, 4])], {
    type: "application/vnd.apache.parquet",
  }),
};

function createJob(overrides: Partial<SqlQueryJob> = {}): SqlQueryJob {
  return {
    id: "job-1",
    mode: "inline",
    request: {
      mode: "inline",
      library: {
        resourceType: "Library",
        status: "active",
        type: {
          coding: [
            {
              system: "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
              code: "sql-query",
            },
          ],
        },
        content: [
          {
            contentType: "application/sql",
            data: "U0VMRUNUIDE=",
          },
        ],
      },
      format: "csv",
    },
    sql: "SELECT 1",
    createdAt: new Date(),
    ...overrides,
  };
}

describe("SqlQueryCard", () => {
  const onClose = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus = "idle";
    mockResult = undefined;
    mockError = undefined;
    capturedOptions = undefined;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // The pending state is communicated via a spinner and a status message
  // so the user knows the request is in flight.
  it("shows a spinner and pending message while the request is in flight", () => {
    mockStatus = "pending";
    render(<SqlQueryCard job={createJob()} onClose={onClose} />);
    expect(screen.getByText(/executing sql query/i)).toBeInTheDocument();
  });

  // A successful tabular result renders a Table with one column per CSV
  // column and a row count badge.
  it("renders a table for a successful tabular result", () => {
    mockStatus = "success";
    mockResult = TABULAR_RESULT;
    render(<SqlQueryCard job={createJob()} onClose={onClose} />);
    expect(screen.getByText(/2 rows/i)).toBeInTheDocument();
    expect(screen.getByText("patient_id")).toBeInTheDocument();
    expect(screen.getByText("given_name")).toBeInTheDocument();
    expect(screen.getByText("pat-1")).toBeInTheDocument();
    expect(screen.getByText("Alice")).toBeInTheDocument();
    expect(screen.queryByLabelText(/download/i)).not.toBeInTheDocument();
  });

  // The submitted SQL is echoed beneath a successful result in the shared,
  // height-bounded preview area.
  it("echoes the submitted SQL beneath a successful result", () => {
    mockStatus = "success";
    mockResult = TABULAR_RESULT;
    render(<SqlQueryCard job={createJob({ sql: "SELECT * FROM patients" })} onClose={onClose} />);
    expect(screen.getByRole("textbox", { name: /submitted sql/i })).toHaveValue(
      "SELECT * FROM patients",
    );
  });

  // The card is a preview only and clamps the rendered rows to 10, with
  // full-result downloads deferred to a future SQL query export operation.
  it("clamps the rendered rows to 10 even if the result has more", () => {
    mockStatus = "success";
    mockResult = {
      ...TABULAR_RESULT,
      rows: Array.from({ length: 25 }, (_, i) => ({
        patient_id: `pat-${i + 1}`,
        given_name: `Name ${i + 1}`,
      })),
    };
    render(<SqlQueryCard job={createJob()} onClose={onClose} />);
    expect(screen.getByText(/10 rows/i)).toBeInTheDocument();
    expect(screen.getByText("pat-10")).toBeInTheDocument();
    expect(screen.queryByText("pat-11")).not.toBeInTheDocument();
  });

  // Once a run returns rows, the result card offers an export affordance: a
  // format picker and an Export button to start an asynchronous export.
  it("renders export controls once the run returns rows", () => {
    mockStatus = "success";
    mockResult = TABULAR_RESULT;
    render(<SqlQueryCard job={createJob()} onClose={onClose} />);
    expect(screen.getByText(/export full result set/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^export$/i })).toBeInTheDocument();
  });

  // Empty tabular results show "No rows returned" instead of an empty
  // table.
  it("shows a no-rows message when the tabular result is empty", () => {
    mockStatus = "success";
    mockResult = {
      ...TABULAR_RESULT,
      rows: [],
    };
    render(<SqlQueryCard job={createJob()} onClose={onClose} />);
    expect(screen.getByText(/no rows returned/i)).toBeInTheDocument();
  });

  // Binary (parquet) results cannot be previewed in the card; the body points
  // the operator to the Export control to download the full result set.
  it("offers export for non-previewable parquet results", () => {
    mockStatus = "success";
    mockResult = BINARY_RESULT;
    render(
      <SqlQueryCard
        job={createJob({
          request: { ...createJob().request, format: "parquet" },
        })}
        onClose={onClose}
      />,
    );
    expect(screen.getByText(/parquet results cannot be previewed/i)).toBeInTheDocument();
    // No preview table for a binary result, but the export affordance is present.
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^export$/i })).toBeInTheDocument();
  });

  // OperationOutcome errors are shown in a callout with the submitted SQL
  // displayed alongside.
  it("renders an OperationOutcome error in a callout above the submitted SQL", () => {
    mockStatus = "error";
    mockError = new OperationOutcomeError(
      {
        resourceType: "OperationOutcome",
        issue: [
          {
            severity: "error",
            code: "invalid",
            diagnostics: "SQL contains a disallowed operation",
          },
        ],
      },
      400,
      "SQL query run",
    );
    render(<SqlQueryCard job={createJob({ sql: "DROP TABLE conditions" })} onClose={onClose} />);
    // The shared error presentation announces every error as an alert.
    expect(screen.getByRole("alert")).toHaveTextContent(/sql contains a disallowed operation/i);
    // The submitted SQL is echoed in the read-only preview area.
    expect(screen.getByRole("textbox", { name: /submitted sql/i })).toHaveValue(
      "DROP TABLE conditions",
    );
  });

  // Generic errors fall back to the message text.
  it("renders a generic error message when the error is not an OperationOutcome", () => {
    mockStatus = "error";
    mockError = new Error("Network error");
    render(<SqlQueryCard job={createJob()} onClose={onClose} />);
    expect(screen.getByRole("alert")).toHaveTextContent(/network error/i);
  });

  // FR-001 and FR-002: the card displays the failure, so nothing else announces
  // it.
  it("displays the failure without raising a notification", () => {
    mockStatus = "error";
    mockError = new Error("Network error");
    render(<SqlQueryCard job={createJob()} onClose={onClose} />);
    // Driving whatever callback the card wired in, so that a reinstated
    // notification fails this test rather than passing unnoticed.
    capturedOptions?.onError?.(mockError);
    expect(screen.getByRole("alert")).toHaveTextContent(/network error/i);
    expect(capturedOptions?.onError).toBeUndefined();
    expect(mockShowToast).not.toHaveBeenCalled();
  });

  // FR-009: the outcome diagnostics are still preferred now that the shared
  // derivation has replaced the card's own extraction helper.
  it("prefers the outcome diagnostics over the flattened message", () => {
    mockStatus = "error";
    const outcome: OperationOutcome = {
      resourceType: "OperationOutcome",
      issue: [{ severity: "error", code: "processing", diagnostics: "Unknown column: bad_column" }],
    };
    mockError = new OperationOutcomeError(outcome, 400, "SQL query");
    render(<SqlQueryCard job={createJob()} onClose={onClose} />);
    expect(screen.getByRole("alert")).toHaveTextContent("Unknown column: bad_column");
    expect(screen.getByRole("alert")).not.toHaveTextContent("SQL query failed");
  });

  // FR-008: an outcome carrying several issues shows each one, with its own
  // severity, rather than only the first.
  it("shows every issue of a multi-issue outcome with its severity", () => {
    mockStatus = "error";
    const outcome: OperationOutcome = {
      resourceType: "OperationOutcome",
      issue: [
        { severity: "error", code: "processing", diagnostics: "Unknown column: bad_column" },
        { severity: "warning", code: "informational", diagnostics: "Result set was truncated" },
      ],
    };
    mockError = new OperationOutcomeError(outcome, 400, "SQL query");
    render(<SqlQueryCard job={createJob()} onClose={onClose} />);
    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent("Unknown column: bad_column");
    expect(alert).toHaveTextContent("Result set was truncated");
    expect(screen.getByText("Error")).toBeInTheDocument();
    expect(screen.getByText("Warning")).toBeInTheDocument();
  });

  // The close button only appears once the request has terminated.
  it("hides the close button while the request is pending", () => {
    mockStatus = "pending";
    render(<SqlQueryCard job={createJob()} onClose={onClose} />);
    expect(screen.queryByRole("button", { name: /close result/i })).not.toBeInTheDocument();
  });

  // The close button surfaces after success and triggers the onClose
  // callback when clicked.
  it("calls onClose when the close button is clicked after success", async () => {
    mockStatus = "success";
    mockResult = TABULAR_RESULT;
    const user = userEvent.setup();
    render(<SqlQueryCard job={createJob()} onClose={onClose} />);
    await user.click(screen.getByRole("button", { name: /close result/i }));
    expect(onClose).toHaveBeenCalled();
  });
});

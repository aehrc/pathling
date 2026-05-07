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
 * table with a download button, parquet renders a download-only body and
 * errors surface in a Callout with the submitted SQL above.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { OperationOutcomeError } from "../../../types/errors";
import { SqlQueryCard } from "../SqlQueryCard";

import type { SqlQueryJob, SqlQueryResult } from "../../../types/sqlQuery";

const mockExecute = vi.fn();
let mockStatus: "idle" | "pending" | "success" | "error" = "idle";
let mockResult: SqlQueryResult | undefined;
let mockError: Error | undefined;

vi.mock("../../../hooks", () => ({
  useSqlQueryRun: () => ({
    execute: mockExecute,
    status: mockStatus,
    result: mockResult,
    error: mockError,
  }),
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
  const onError = vi.fn();
  const onClose = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus = "idle";
    mockResult = undefined;
    mockError = undefined;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // The pending state is communicated via a spinner and a status message
  // so the user knows the request is in flight.
  it("shows a spinner and pending message while the request is in flight", () => {
    mockStatus = "pending";
    render(<SqlQueryCard job={createJob()} onError={onError} onClose={onClose} />);
    expect(screen.getByText(/executing sql query/i)).toBeInTheDocument();
  });

  // A successful tabular result renders a Table with one column per CSV
  // column and a row count badge.
  it("renders a table for a successful tabular result", () => {
    mockStatus = "success";
    mockResult = TABULAR_RESULT;
    render(<SqlQueryCard job={createJob()} onError={onError} onClose={onClose} />);
    expect(screen.getByText(/2 rows/i)).toBeInTheDocument();
    expect(screen.getByText("patient_id")).toBeInTheDocument();
    expect(screen.getByText("given_name")).toBeInTheDocument();
    expect(screen.getByText("pat-1")).toBeInTheDocument();
    expect(screen.getByText("Alice")).toBeInTheDocument();
    expect(screen.getByLabelText(/download \.csv/i)).toBeInTheDocument();
  });

  // Empty tabular results show "No rows returned" instead of an empty
  // table.
  it("shows a no-rows message when the tabular result is empty", () => {
    mockStatus = "success";
    mockResult = {
      ...TABULAR_RESULT,
      rows: [],
    };
    render(<SqlQueryCard job={createJob()} onError={onError} onClose={onClose} />);
    expect(screen.getByText(/no rows returned/i)).toBeInTheDocument();
  });

  // The parquet branch renders only a download button and no table.
  it("renders a download-only body for parquet results", () => {
    mockStatus = "success";
    mockResult = BINARY_RESULT;
    render(
      <SqlQueryCard
        job={createJob({
          request: { ...createJob().request, format: "parquet" },
        })}
        onError={onError}
        onClose={onClose}
      />,
    );
    expect(screen.getByRole("button", { name: /download \.parquet/i })).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
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
    render(
      <SqlQueryCard
        job={createJob({ sql: "DROP TABLE conditions" })}
        onError={onError}
        onClose={onClose}
      />,
    );
    expect(screen.getByText(/sql contains a disallowed operation/i)).toBeInTheDocument();
    expect(screen.getByText(/drop table conditions/i)).toBeInTheDocument();
  });

  // Generic errors fall back to the message text.
  it("renders a generic error message when the error is not an OperationOutcome", () => {
    mockStatus = "error";
    mockError = new Error("Network error");
    render(<SqlQueryCard job={createJob()} onError={onError} onClose={onClose} />);
    expect(screen.getByText(/network error/i)).toBeInTheDocument();
  });

  // The close button only appears once the request has terminated.
  it("hides the close button while the request is pending", () => {
    mockStatus = "pending";
    render(<SqlQueryCard job={createJob()} onError={onError} onClose={onClose} />);
    expect(screen.queryByRole("button", { name: /close result/i })).not.toBeInTheDocument();
  });

  // The close button surfaces after success and triggers the onClose
  // callback when clicked.
  it("calls onClose when the close button is clicked after success", async () => {
    mockStatus = "success";
    mockResult = TABULAR_RESULT;
    const user = userEvent.setup();
    render(<SqlQueryCard job={createJob()} onError={onError} onClose={onClose} />);
    await user.click(screen.getByRole("button", { name: /close result/i }));
    expect(onClose).toHaveBeenCalled();
  });
});

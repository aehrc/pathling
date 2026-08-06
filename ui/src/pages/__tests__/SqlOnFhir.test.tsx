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
 * Tests for the SQL on FHIR page: the reporting of job failures, and the
 * export set the page composes and hands to one export job.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../test/testUtils";
import { SqlOnFhir } from "../SqlOnFhir";

import type { SqlQueryRequest } from "../../types/sqlQuery";
import type { ViewRunRequest } from "../../types/viewJob";
import type { ReactNode } from "react";

// Mock the toast context, which is where failures must be reported.
const mockShowToast = vi.fn();
vi.mock("../../contexts/ToastContext", () => ({
  useToast: () => ({ showToast: mockShowToast }),
}));

// Mock the guard so the page content renders without an access check.
vi.mock("../../components/auth/CapabilityGuard", () => ({
  CapabilityGuard: ({ children }: { children: () => ReactNode }) => children(),
}));

vi.mock("../../hooks", () => ({
  useSaveViewDefinition: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useSaveSqlQueryLibrary: () => ({ mutateAsync: vi.fn(), isPending: false }),
}));

vi.mock("../../components/sqlOnFhir/sqlQueryFormHelpers", () => ({
  extractRequestSql: () => "SELECT 1",
}));

// Mock the form so single clicks start each kind of job, and add each kind of
// subject to the export set.
vi.mock("../../components/sqlOnFhir/SqlOnFhirForm", () => ({
  SqlOnFhirForm: ({
    onExecuteViewDefinition,
    onExecuteSqlQuery,
    onAddViewToExportSet,
    onAddQueryToExportSet,
  }: {
    onExecuteViewDefinition: (request: ViewRunRequest) => void;
    onExecuteSqlQuery: (request: SqlQueryRequest) => void;
    onAddViewToExportSet: (request: ViewRunRequest) => void;
    onAddQueryToExportSet: (request: SqlQueryRequest) => void;
  }) => (
    <>
      <button onClick={() => onExecuteViewDefinition({ mode: "inline" } as ViewRunRequest)}>
        Run view
      </button>
      <button onClick={() => onExecuteSqlQuery({ mode: "inline" } as SqlQueryRequest)}>
        Run query
      </button>
      <button
        onClick={() => onAddViewToExportSet({ mode: "stored", viewDefinitionId: "demographics" })}
      >
        Add view
      </button>
      <button onClick={() => onAddQueryToExportSet({ mode: "stored", libraryId: "bp-summary" })}>
        Add query
      </button>
    </>
  ),
}));

// The export card is mocked so the subjects the page composed can be read back.
let lastExportSubjects: Array<{ name?: string }> | undefined;
vi.mock("../../components/sqlOnFhir/SqlExportCardWrapper", () => ({
  SqlExportCardWrapper: ({ subjects }: { subjects: Array<{ name?: string }> }) => {
    lastExportSubjects = subjects;
    return <div>Export job card</div>;
  },
}));

// Mock the cards, which now display their own failures.
vi.mock("../../components/sqlOnFhir/ViewCard", () => ({
  ViewCard: () => <div>View card</div>,
}));
vi.mock("../../components/sqlOnFhir/SqlQueryCard", () => ({
  SqlQueryCard: () => <div>SQL query card</div>,
}));

describe("SqlOnFhir page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    lastExportSubjects = undefined;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // FR-012: the page neither displays nor originates these failures, so it no
  // longer carries a failure-reporting callback for either card.
  it("runs a view without wiring a failure callback into the card", async () => {
    const user = userEvent.setup();

    render(<SqlOnFhir />);

    await user.click(screen.getByRole("button", { name: "Run view" }));

    expect(screen.getByText("View card")).toBeInTheDocument();
    expect(mockShowToast).not.toHaveBeenCalled();
  });

  it("runs a SQL query without wiring a failure callback into the card", async () => {
    const user = userEvent.setup();

    render(<SqlOnFhir />);

    await user.click(screen.getByRole("button", { name: "Run query" }));

    expect(screen.getByText("SQL query card")).toBeInTheDocument();
    expect(mockShowToast).not.toHaveBeenCalled();
  });

  // The set is hidden until something is in it, then carries one entry per
  // captured subject.
  it("shows the export set once a subject is captured", async () => {
    const user = userEvent.setup();

    render(<SqlOnFhir />);
    expect(screen.queryByText(/export set \(/i)).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Add view" }));
    await user.click(screen.getByRole("button", { name: "Add query" }));

    expect(screen.getByText("Export set (2)")).toBeInTheDocument();
  });

  // Exporting the set starts one job carrying every entry, named as the panel
  // shows them.
  it("exports the whole set as one job", async () => {
    const user = userEvent.setup();

    render(<SqlOnFhir />);
    await user.click(screen.getByRole("button", { name: "Add view" }));
    await user.click(screen.getByRole("button", { name: "Add query" }));
    await user.click(screen.getByRole("button", { name: /^Export set$/ }));

    expect(screen.getByText("Export job card")).toBeInTheDocument();
    expect(lastExportSubjects?.map((subject) => subject.name)).toEqual([
      "demographics",
      "bp-summary",
    ]);
  });

  it("clears the set", async () => {
    const user = userEvent.setup();

    render(<SqlOnFhir />);
    await user.click(screen.getByRole("button", { name: "Add view" }));
    await user.click(screen.getByRole("button", { name: /clear all/i }));

    expect(screen.queryByText(/export set \(/i)).not.toBeInTheDocument();
  });
});

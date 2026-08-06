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
 * Tests for the SQL on FHIR page, covering the reporting of job failures.
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

// Mock the form so single clicks start each kind of job.
vi.mock("../../components/sqlOnFhir/SqlOnFhirForm", () => ({
  SqlOnFhirForm: ({
    onExecuteViewDefinition,
    onExecuteSqlQuery,
  }: {
    onExecuteViewDefinition: (request: ViewRunRequest) => void;
    onExecuteSqlQuery: (request: SqlQueryRequest) => void;
  }) => (
    <>
      <button onClick={() => onExecuteViewDefinition({ mode: "inline" } as ViewRunRequest)}>
        Run view
      </button>
      <button onClick={() => onExecuteSqlQuery({ mode: "inline" } as SqlQueryRequest)}>
        Run query
      </button>
    </>
  ),
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
});

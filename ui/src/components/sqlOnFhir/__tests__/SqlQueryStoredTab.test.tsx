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
 * Tests for the SqlQueryStoredTab component.
 *
 * Verifies the grouped picker (SQL queries and SQL views), omission of an
 * empty group, the renamed "Views" dependency heading, the SQL preview on
 * selection, and the combined empty-state message.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { SqlQueryStoredTab } from "../SqlQueryStoredTab";

import type { SqlQueryLibrarySummary } from "../../../types/sqlQuery";

// The hooks barrel transitively imports main.tsx (via AuthContext), which
// runs createRoot at load. Mock it; the tab only needs useClipboard.
vi.mock("../../../hooks", () => ({
  useClipboard: () => vi.fn(),
}));

/**
 * Builds a minimal stored-Library summary for the picker.
 *
 * @param overrides - Fields to override on the base summary.
 * @returns A summary suitable for the SqlQueryStoredTab props.
 */
function makeSummary(overrides: Partial<SqlQueryLibrarySummary>): SqlQueryLibrarySummary {
  return {
    id: "id",
    title: "Title",
    sql: "SELECT 1",
    relatedArtifacts: [],
    parameters: [],
    resource: {
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
      content: [{ contentType: "application/sql", data: "U0VMRUNUIDE=" }],
    },
    ...overrides,
  };
}

const QUERY = makeSummary({
  id: "patients-by-condition",
  title: "Patients by condition",
  sql: "SELECT * FROM patients",
  parameters: [{ name: "patient_id", type: "string" }],
});

const VIEW = makeSummary({
  id: "active-patients",
  title: "Active patients",
  sql: "SELECT patient_id FROM patients WHERE active = true",
  relatedArtifacts: [{ label: "patients", reference: "ViewDefinition/patient-demographics" }],
});

describe("SqlQueryStoredTab", () => {
  const onSelect = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // Both groups appear when each list has members.
  it("renders a SQL queries group and a SQL views group", async () => {
    const user = userEvent.setup();
    render(
      <SqlQueryStoredTab
        queries={[QUERY]}
        views={[VIEW]}
        isLoading={false}
        selectedId=""
        onSelect={onSelect}
      />,
    );

    await user.click(screen.getByRole("combobox"));

    expect(screen.getByText("SQL queries")).toBeInTheDocument();
    expect(screen.getByText("SQL views")).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Patients by condition" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Active patients" })).toBeInTheDocument();
  });

  // An empty group is omitted entirely so no orphan heading shows.
  it("omits the SQL views group when there are no views", async () => {
    const user = userEvent.setup();
    render(
      <SqlQueryStoredTab
        queries={[QUERY]}
        views={[]}
        isLoading={false}
        selectedId=""
        onSelect={onSelect}
      />,
    );

    await user.click(screen.getByRole("combobox"));

    expect(screen.getByText("SQL queries")).toBeInTheDocument();
    expect(screen.queryByText("SQL views")).not.toBeInTheDocument();
  });

  // Selecting an option emits its logical id to the parent.
  it("emits the selected logical id", async () => {
    const user = userEvent.setup();
    render(
      <SqlQueryStoredTab
        queries={[QUERY]}
        views={[VIEW]}
        isLoading={false}
        selectedId=""
        onSelect={onSelect}
      />,
    );

    await user.click(screen.getByRole("combobox"));
    await user.click(screen.getByRole("option", { name: "Active patients" }));

    expect(onSelect).toHaveBeenCalledWith("active-patients");
  });

  // A selected SQLView (located across both arrays) previews its decoded SQL.
  it("shows the SQL preview for a selected SQLView", () => {
    render(
      <SqlQueryStoredTab
        queries={[QUERY]}
        views={[VIEW]}
        isLoading={false}
        selectedId="active-patients"
        onSelect={onSelect}
      />,
    );

    const preview = screen.getByRole("textbox", {
      name: /decoded sql preview/i,
    });
    expect(preview).toHaveValue("SELECT patient_id FROM patients WHERE active = true");
  });

  // The dependency heading is renamed from "Tables" to "Views".
  it("labels the dependency heading 'Views'", () => {
    render(
      <SqlQueryStoredTab
        queries={[QUERY]}
        views={[VIEW]}
        isLoading={false}
        selectedId="active-patients"
        onSelect={onSelect}
      />,
    );

    expect(screen.getByText("Views")).toBeInTheDocument();
    expect(screen.queryByText("Tables")).not.toBeInTheDocument();
    expect(screen.getByText("ViewDefinition/patient-demographics")).toBeInTheDocument();
  });

  // With neither stored queries nor views, the empty-state copy reflects both.
  it("shows a combined empty state when both lists are empty", () => {
    render(
      <SqlQueryStoredTab
        queries={[]}
        views={[]}
        isLoading={false}
        selectedId=""
        onSelect={onSelect}
      />,
    );

    expect(screen.getByText(/no stored sql queries or sql views/i)).toBeInTheDocument();
  });

  // The loading state is shown while either list is still in flight.
  it("shows a loading state", () => {
    render(
      <SqlQueryStoredTab
        queries={undefined}
        views={undefined}
        isLoading
        selectedId=""
        onSelect={onSelect}
      />,
    );

    expect(screen.getByText(/loading/i)).toBeInTheDocument();
  });
});

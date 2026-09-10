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
 * selection, and the combined empty-state message. Also verifies that the
 * selected query's parameters are presented once, inside the tab, as
 * name/type read-only beside a value input, with no read-only badge list.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { SqlQueryStoredTab } from "../SqlQueryStoredTab";

import type { SqlQueryLibrarySummary, SqlQueryRuntimeBindings } from "../../../types/sqlQuery";

// Mock the hooks barrel; the tab only needs useClipboard.
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

const PLAIN_QUERY = makeSummary({
  id: "all-patients",
  title: "All patients",
  sql: "SELECT * FROM patients",
  parameters: [],
});

const VIEW = makeSummary({
  id: "active-patients",
  title: "Active patients",
  sql: "SELECT patient_id FROM patients WHERE active = true",
  relatedArtifacts: [{ label: "patients", reference: "ViewDefinition/patient-demographics" }],
});

/**
 * Renders the stored tab with sensible defaults and inert callbacks.
 *
 * @param overrides - Props to override on the defaults.
 * @param overrides.queries - Stored SQLQuery summaries.
 * @param overrides.views - Stored SQLView summaries.
 * @param overrides.isLoading - Whether either stored list is loading.
 * @param overrides.selectedId - The currently selected logical ID.
 * @param overrides.bindings - Current runtime bindings, keyed by parameter name.
 * @param overrides.disabled - Whether the controls are disabled.
 * @returns The userEvent instance and the callback spies.
 */
function renderTab(
  overrides: {
    queries?: SqlQueryLibrarySummary[] | undefined;
    views?: SqlQueryLibrarySummary[] | undefined;
    isLoading?: boolean;
    selectedId?: string;
    bindings?: SqlQueryRuntimeBindings;
    disabled?: boolean;
  } = {},
) {
  const user = userEvent.setup();
  const onSelect = vi.fn();
  const onBindingChange = vi.fn();
  render(
    <SqlQueryStoredTab
      queries={"queries" in overrides ? overrides.queries : [QUERY, PLAIN_QUERY]}
      views={"views" in overrides ? overrides.views : [VIEW]}
      isLoading={overrides.isLoading ?? false}
      selectedId={overrides.selectedId ?? ""}
      onSelect={onSelect}
      bindings={overrides.bindings ?? {}}
      onBindingChange={onBindingChange}
      disabled={overrides.disabled ?? false}
    />,
  );
  return { user, onSelect, onBindingChange };
}

describe("SqlQueryStoredTab", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // Both groups appear when each list has members.
  it("renders a SQL queries group and a SQL views group", async () => {
    const { user } = renderTab();

    await user.click(screen.getByRole("combobox"));

    expect(screen.getByText("SQL queries")).toBeInTheDocument();
    expect(screen.getByText("SQL views")).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Patients by condition" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Active patients" })).toBeInTheDocument();
  });

  // An empty group is omitted entirely so no orphan heading shows.
  it("omits the SQL views group when there are no views", async () => {
    const { user } = renderTab({ views: [] });

    await user.click(screen.getByRole("combobox"));

    expect(screen.getByText("SQL queries")).toBeInTheDocument();
    expect(screen.queryByText("SQL views")).not.toBeInTheDocument();
  });

  // Selecting an option emits its logical id to the parent.
  it("emits the selected logical id", async () => {
    const { user, onSelect } = renderTab();

    await user.click(screen.getByRole("combobox"));
    await user.click(screen.getByRole("option", { name: "Active patients" }));

    expect(onSelect).toHaveBeenCalledWith("active-patients");
  });

  // A selected SQLView (located across both arrays) previews its decoded SQL.
  it("shows the SQL preview for a selected SQLView", () => {
    renderTab({ selectedId: "active-patients" });

    const preview = screen.getByRole("textbox", {
      name: /decoded sql preview/i,
    });
    expect(preview).toHaveValue("SELECT patient_id FROM patients WHERE active = true");
  });

  // The dependency heading is renamed from "Tables" to "Views".
  it("labels the dependency heading 'Views'", () => {
    renderTab({ selectedId: "active-patients" });

    expect(screen.getByText("Views")).toBeInTheDocument();
    expect(screen.queryByText("Tables")).not.toBeInTheDocument();
    expect(screen.getByText("ViewDefinition/patient-demographics")).toBeInTheDocument();
  });

  // With neither stored queries nor views, the empty-state copy reflects both.
  it("shows a combined empty state when both lists are empty", () => {
    renderTab({ queries: [], views: [] });

    expect(screen.getByText(/no stored sql queries or sql views/i)).toBeInTheDocument();
  });

  // The loading state is shown while either list is still in flight.
  it("shows a loading state", () => {
    renderTab({ queries: undefined, views: undefined, isLoading: true });

    expect(screen.getByText(/loading/i)).toBeInTheDocument();
  });
});

describe("SqlQueryStoredTab parameters section", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // US2 scenario 1: one parameters section, inside the tab, pairing the
  // read-only declaration with the value bound for this run. The read-only
  // badge list it replaces is gone.
  it("renders exactly one parameters section pairing each declaration with a value input", () => {
    renderTab({
      selectedId: "patients-by-condition",
      bindings: { patient_id: "pat-1" },
    });

    expect(screen.queryAllByText("Parameters")).toHaveLength(1);
    expect(screen.queryByText("Declared parameters")).not.toBeInTheDocument();
    // The declaration is shown read-only beside the value input.
    expect(screen.getByText("patient_id")).toBeInTheDocument();
    expect(screen.getByText("string")).toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: "Runtime value for patient_id" })).toHaveValue(
      "pat-1",
    );
  });

  // The value typed in the section is reported to the parent against the
  // parameter's name, which is how it reaches the request.
  it("emits value changes against the parameter name", async () => {
    const { user, onBindingChange } = renderTab({ selectedId: "patients-by-condition" });

    await user.type(screen.getByRole("textbox", { name: "Runtime value for patient_id" }), "x");

    expect(onBindingChange).toHaveBeenCalledWith("patient_id", "x");
  });

  // Execution disables the whole tab, values included.
  it("disables the value input when the tab is disabled", () => {
    renderTab({ selectedId: "patients-by-condition", disabled: true });

    expect(screen.getByRole("textbox", { name: "Runtime value for patient_id" })).toBeDisabled();
  });

  // US2 scenario 2: a stored query declaring nothing says so, rather than
  // leaving an unexplained empty section.
  it("states that a selected query declaring no parameters has none", () => {
    renderTab({ selectedId: "all-patients" });

    expect(screen.getByText(/declares no runtime parameters/i)).toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: /^Runtime value for/ })).not.toBeInTheDocument();
  });

  // US2 scenario 2: a SQLView never declares parameters, so it takes the same
  // guidance.
  it("states that a selected SQLView has no parameters", () => {
    renderTab({ selectedId: "active-patients" });

    expect(screen.getByText(/declares no runtime parameters/i)).toBeInTheDocument();
  });

  // US2 scenario 3: with nothing selected there is nothing to bind, so the
  // section does not render at all.
  it("renders no parameters section when nothing is selected", () => {
    renderTab({ selectedId: "" });

    expect(screen.queryByText("Parameters")).not.toBeInTheDocument();
    expect(screen.queryByText(/declares no runtime parameters/i)).not.toBeInTheDocument();
  });
});

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
 * Tests for the SqlQueryInlineTab component.
 *
 * Verifies the "Views" editor, the grouped source selector binding each source
 * by its canonical URL, the row update on selection, the disabled state for
 * URL-less sources, and the "source not found" surfacing of an unmatched stored
 * reference. Also verifies that a parameter row describes its parameter
 * completely - name, type, value and remove - with no default-value field, and
 * that invalid values and duplicate names are marked on the row.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { SqlQueryInlineTab } from "../SqlQueryInlineTab";

import type {
  SourceOption,
  SqlQueryParameterDeclaration,
  SqlQueryRelatedArtifact,
} from "../../../types/sqlQuery";

const PATIENT_DEMOGRAPHICS_URL = "https://example.org/ViewDefinition/patient_demographics";
const ACTIVE_PATIENTS_URL = "https://example.org/Library/ActivePatients";

const VIEW_DEFINITIONS: SourceOption[] = [
  { id: "patient-demographics", name: "Patient Demographics", url: PATIENT_DEMOGRAPHICS_URL },
  // A source with no canonical URL cannot be referenced.
  { id: "draft-obs", name: "Draft lab observations", url: undefined },
];
const SQL_VIEWS: SourceOption[] = [
  { id: "active-patients", name: "Active patients", url: ACTIVE_PATIENTS_URL },
];

/**
 * Renders the inline tab with sensible defaults and inert callbacks.
 *
 * @param overrides - Props to override on the defaults.
 * @param overrides.tables - The view rows to render.
 * @param overrides.parameters - The parameter rows to render.
 * @param overrides.duplicateNames - Parameter names declared by more than one row.
 * @param overrides.viewDefinitions - Available ViewDefinition options.
 * @param overrides.sqlViews - Available SQLView options.
 * @returns The userEvent instance and the change spies.
 */
function renderTab(
  overrides: {
    tables?: SqlQueryRelatedArtifact[];
    parameters?: SqlQueryParameterDeclaration[];
    duplicateNames?: ReadonlySet<string>;
    viewDefinitions?: SourceOption[];
    sqlViews?: SourceOption[];
  } = {},
) {
  const user = userEvent.setup();
  const onTablesChange = vi.fn();
  const onParametersChange = vi.fn();
  render(
    <SqlQueryInlineTab
      title=""
      onTitleChange={vi.fn()}
      sql=""
      onSqlChange={vi.fn()}
      tables={overrides.tables ?? []}
      onTablesChange={onTablesChange}
      parameters={overrides.parameters ?? []}
      onParametersChange={onParametersChange}
      duplicateNames={overrides.duplicateNames ?? new Set()}
      viewDefinitions={overrides.viewDefinitions ?? VIEW_DEFINITIONS}
      sqlViews={overrides.sqlViews ?? SQL_VIEWS}
    />,
  );
  return { user, onTablesChange, onParametersChange };
}

/**
 * Builds a parameter row.
 *
 * @param overrides - Fields to override on the base row.
 * @returns A parameter row for the inline tab.
 */
function makeParam(
  overrides: Partial<SqlQueryParameterDeclaration> = {},
): SqlQueryParameterDeclaration {
  return { rowId: "p1", name: "", type: "string", value: "", ...overrides };
}

/** A single empty view row. */
const EMPTY_ROW: SqlQueryRelatedArtifact = {
  rowId: "r1",
  label: "patients",
  referenceUrl: "",
};

describe("SqlQueryInlineTab", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // The editor is titled "Views" rather than "Tables".
  it("titles the section 'Views'", () => {
    renderTab();
    expect(screen.getByText("Views")).toBeInTheDocument();
    expect(screen.queryByText("Tables")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /add view/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /add table/i })).toBeNull();
  });

  // The per-row source selector groups ViewDefinitions and SQLViews.
  it("groups ViewDefinitions and SQLViews in the source selector", async () => {
    const { user } = renderTab({ tables: [EMPTY_ROW] });

    await user.click(screen.getByRole("combobox", { name: /source for view 1/i }));

    expect(screen.getByText("View definitions")).toBeInTheDocument();
    expect(screen.getByText("SQL views")).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Patient Demographics" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Active patients" })).toBeInTheDocument();
  });

  // Selecting a ViewDefinition stamps the row with the source's canonical URL.
  it("updates the row with the chosen ViewDefinition's url", async () => {
    const { user, onTablesChange } = renderTab({ tables: [EMPTY_ROW] });

    await user.click(screen.getByRole("combobox", { name: /source for view 1/i }));
    await user.click(screen.getByRole("option", { name: "Patient Demographics" }));

    expect(onTablesChange).toHaveBeenCalledWith([
      expect.objectContaining({
        rowId: "r1",
        referenceUrl: PATIENT_DEMOGRAPHICS_URL,
      }),
    ]);
  });

  // Selecting a SQLView stamps the row with the SQLView's canonical URL.
  it("updates the row with the chosen SQLView's url", async () => {
    const { user, onTablesChange } = renderTab({ tables: [EMPTY_ROW] });

    await user.click(screen.getByRole("combobox", { name: /source for view 1/i }));
    await user.click(screen.getByRole("option", { name: "Active patients" }));

    expect(onTablesChange).toHaveBeenCalledWith([
      expect.objectContaining({
        rowId: "r1",
        referenceUrl: ACTIVE_PATIENTS_URL,
      }),
    ]);
  });

  // A source with no canonical URL is rendered disabled with an explanation and
  // cannot be selected, since it could never satisfy a canonical reference.
  it("disables a URL-less source with an explanation and prevents selecting it", async () => {
    const { user, onTablesChange } = renderTab({ tables: [EMPTY_ROW] });

    await user.click(screen.getByRole("combobox", { name: /source for view 1/i }));

    const draftOption = screen.getByRole("option", { name: /Draft lab observations/i });
    expect(draftOption).toHaveAttribute("aria-disabled", "true");
    expect(screen.getByText(/No canonical URL/i)).toBeInTheDocument();

    await user.click(draftOption);
    expect(onTablesChange).not.toHaveBeenCalled();
  });

  // When editing a stored query, a saved URL that matches no known source is
  // surfaced verbatim with a "source not found" note.
  it("surfaces an unmatched stored reference verbatim", () => {
    const unmatchedRow: SqlQueryRelatedArtifact = {
      rowId: "r1",
      label: "patients",
      referenceUrl: "https://example.org/ViewDefinition/Gone",
    };
    renderTab({ tables: [unmatchedRow] });

    expect(screen.getByText(/source not found/i)).toBeInTheDocument();
    expect(screen.getByText("https://example.org/ViewDefinition/Gone")).toBeInTheDocument();
  });

  // With neither ViewDefinitions nor SQLViews, the selector is disabled and
  // shows a "nothing to reference" placeholder.
  it("disables the selector when there is nothing to reference", () => {
    renderTab({ tables: [EMPTY_ROW], viewDefinitions: [], sqlViews: [] });

    const combobox = screen.getByRole("combobox", { name: /source for view 1/i });
    expect(combobox).toBeDisabled();
    expect(combobox).toHaveTextContent(/nothing to reference/i);
  });
});

describe("SqlQueryInlineTab parameter rows", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // A row describes its parameter completely: the name and type that are saved,
  // plus the value bound on this run.
  it("renders a name, type, value and remove control for a parameter row", () => {
    renderTab({ parameters: [makeParam({ name: "patient_id" })] });

    expect(screen.getByRole("textbox", { name: "Name for parameter 1" })).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: "Type for parameter 1" })).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "Value for parameter 1" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Remove parameter 1" })).toBeInTheDocument();
    expect(screen.getByText("Value")).toBeInTheDocument();
  });

  // The dead "Default (optional)" field is gone: a row carries the value bound
  // on this run, not a default that was never used.
  it("offers no default-value field", () => {
    renderTab({ parameters: [makeParam({ name: "patient_id" })] });

    expect(screen.queryByText("Default (optional)")).toBeNull();
    expect(screen.queryByRole("textbox", { name: /default value for parameter/i })).toBeNull();
  });

  // Editing the value updates the row, which is where the inline binding lives.
  it("reports the typed value against the row", async () => {
    const { user, onParametersChange } = renderTab({
      parameters: [makeParam({ name: "period_end", type: "date" })],
    });

    await user.type(screen.getByRole("textbox", { name: "Value for parameter 1" }), "2");

    expect(onParametersChange).toHaveBeenCalledWith([
      expect.objectContaining({ rowId: "p1", value: "2" }),
    ]);
  });

  // A boolean parameter has two states and no unbound state, so its value
  // control is a switch rather than a text field.
  it("renders a switch for a boolean parameter", () => {
    renderTab({ parameters: [makeParam({ name: "active", type: "boolean" })] });

    expect(screen.getByRole("switch", { name: "Value for parameter 1" })).toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: "Value for parameter 1" })).toBeNull();
  });

  // A value that does not parse as its declared type is marked with a message
  // naming the expected form.
  it("marks a value that does not parse as its declared type", () => {
    renderTab({
      parameters: [makeParam({ name: "period_end", type: "date", value: "not-a-date" })],
    });

    expect(screen.getByText("Expected a ISO 8601 date (YYYY-MM-DD) value.")).toBeInTheDocument();
  });

  // A named row with no value cannot be submitted, so the empty input is marked
  // as required.
  it("marks an empty value on a named row as required", () => {
    renderTab({ parameters: [makeParam({ name: "patient_id" })] });

    expect(screen.getByRole("textbox", { name: "Value for parameter 1" })).toBeRequired();
  });

  // An unnamed row declares nothing, so its empty value is not required.
  it("does not require a value on an unnamed row", () => {
    renderTab({ parameters: [makeParam({ name: "" })] });

    expect(screen.getByRole("textbox", { name: "Value for parameter 1" })).not.toBeRequired();
  });

  // Two rows declaring the same name are ambiguous, since one name can only
  // bind one value, so both rows are marked with the offending name.
  it("marks both rows sharing a name as duplicates", () => {
    renderTab({
      parameters: [
        makeParam({ rowId: "p1", name: "period_end" }),
        makeParam({ rowId: "p2", name: "period_end" }),
      ],
      duplicateNames: new Set(["period_end"]),
    });

    expect(screen.getAllByText("Duplicate parameter name: period_end.")).toHaveLength(2);
  });

  // A row whose name is not among the duplicates is left unmarked.
  it("does not mark a uniquely named row", () => {
    renderTab({
      parameters: [makeParam({ name: "period_start" })],
      duplicateNames: new Set(["period_end"]),
    });

    expect(screen.queryByText(/duplicate parameter name/i)).toBeNull();
  });
});

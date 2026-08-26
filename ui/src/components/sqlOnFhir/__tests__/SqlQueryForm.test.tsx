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
 * Tests for the SqlQueryForm component, focused on where parameter values are
 * presented and how they reach the request.
 *
 * Each tab owns its parameter presentation, so no shared "Runtime parameter
 * values" section is rendered below the tabs in either mode. Executing an
 * inline query binds the values typed on its rows, and values bound on the
 * stored tab are retained by parameter name across query selections. Saving an
 * inline query switches to the stored tab with the saved query selected and
 * the values typed inline carried across by name. Execute and Add to export
 * set are gated on every declared parameter carrying a valid value, while Save
 * is gated only on duplicate names, since values are never persisted.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, within } from "../../../test/testUtils";
import { SqlQueryForm } from "../SqlQueryForm";

import type {
  SaveSqlQueryLibraryResult,
  SqlQueryLibrary,
  SqlQueryLibrarySummary,
  SqlQueryParameterType,
  SqlQueryRequest,
} from "../../../types/sqlQuery";
import type { UserEvent } from "@testing-library/user-event";

/**
 * Builds a minimal stored-Library summary for the picker.
 *
 * @param overrides - Fields to override on the base summary.
 * @returns A summary suitable for the form's stored lists.
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

// The SQL authored inline before a save, distinctive enough to identify the
// selected query in the stored tab's preview afterwards.
const SAVED_QUERY_SQL = "SELECT * FROM patients WHERE date <= :period_end";

const PARAM_QUERY = makeSummary({
  id: "param-query",
  title: "Parameterised query",
  parameters: [{ name: "patient_id", type: "string" }],
});

const PLAIN_QUERY = makeSummary({
  id: "plain-query",
  title: "Plain query",
  parameters: [],
});

// Two queries declaring the same parameter, used to show that a bound value
// is retained by name across selections.
const PERIOD_QUERY = makeSummary({
  id: "period-query",
  title: "Period query",
  parameters: [{ name: "period_end", type: "date" }],
});

const OTHER_PERIOD_QUERY = makeSummary({
  id: "other-period-query",
  title: "Other period query",
  parameters: [{ name: "period_end", type: "date" }],
});

// The query an inline save resolves to. Its distinctive SQL identifies it in
// the stored tab's preview, and it declares the parameter authored inline.
const SAVED_QUERY = makeSummary({
  id: "saved-query",
  title: "Saved period query",
  sql: SAVED_QUERY_SQL,
  parameters: [{ name: "period_end", type: "date" }],
});

const SQL_VIEW = makeSummary({
  id: "sql-view",
  title: "Active patients view",
  url: "http://example.org/Library/active-patients-view",
  parameters: [],
});

// Mock the hooks barrel with the stored lists the form and its picker
// consume.
vi.mock("../../../hooks", () => ({
  useSqlQueryLibraries: () => ({
    data: [PARAM_QUERY, PLAIN_QUERY, PERIOD_QUERY, OTHER_PERIOD_QUERY, SAVED_QUERY],
    isLoading: false,
  }),
  useSqlViews: () => ({ data: [SQL_VIEW], isLoading: false }),
  useViewDefinitions: () => ({ data: [] }),
  useClipboard: () => vi.fn(),
}));

const RUNTIME_SECTION = "Runtime parameter values";

/**
 * Renders the form with inert callbacks, overridden as each test requires.
 *
 * @param overrides - Props to override on the inert defaults.
 * @returns The userEvent instance for driving interactions.
 */
function renderForm(
  overrides: Readonly<{
    onExecute?: (request: SqlQueryRequest) => void;
    onAddToExportSet?: (request: SqlQueryRequest) => void;
    onSaveToServer?: (library: SqlQueryLibrary) => Promise<SaveSqlQueryLibraryResult>;
  }> = {},
) {
  const user = userEvent.setup();
  render(
    <SqlQueryForm
      onExecute={vi.fn()}
      onSaveToServer={vi.fn()}
      isExecuting={false}
      isSaving={false}
      {...overrides}
    />,
  );
  return { user };
}

/**
 * Authors a query on the "Provide SQL" tab: some SQL, one resolved view, and
 * optionally a title and a single declared parameter.
 *
 * @param user - The userEvent instance.
 * @param options - What to author beyond the view.
 * @param options.title - Library title, required before a save is allowed.
 * @param options.sql - The SQL to type, defaulting to a trivial statement.
 * @param options.parameter - The single parameter row to declare, with the
 *   value to type against it (omitted for an empty value).
 */
async function authorInlineQuery(
  user: UserEvent,
  options: Readonly<{
    title?: string;
    sql?: string;
    parameter?: { name: string; type: SqlQueryParameterType; value?: string };
  }> = {},
) {
  await user.click(screen.getByRole("tab", { name: /provide sql/i }));
  if (options.title !== undefined) {
    await user.type(screen.getByRole("textbox", { name: /library title/i }), options.title);
  }
  await user.type(screen.getByRole("textbox", { name: /^sql$/i }), options.sql ?? "SELECT 1");
  await user.click(screen.getByRole("button", { name: /add view/i }));
  await user.type(screen.getByRole("textbox", { name: /label for view 1/i }), "patients");
  await user.click(screen.getByRole("combobox", { name: /source for view 1/i }));
  await user.click(screen.getByRole("option", { name: "Active patients view" }));
  const parameter = options.parameter;
  if (parameter) {
    await declareParameter(user, 1, parameter);
  }
}

/**
 * Declares one inline parameter row: adds a row and fills the row at the given
 * one-based position.
 *
 * @param user - The userEvent instance.
 * @param index - The one-based position of the row to fill.
 * @param parameter - The name and type to declare, and the value to type
 *   against the row. A value is only typed when supplied, and a boolean row's
 *   control is a switch rather than a text field, so it is never typed into.
 */
async function declareParameter(
  user: UserEvent,
  index: number,
  parameter: Readonly<{ name: string; type: SqlQueryParameterType; value?: string }>,
) {
  await user.click(screen.getByRole("button", { name: /add parameter/i }));
  await user.type(
    screen.getByRole("textbox", { name: `Name for parameter ${index}` }),
    parameter.name,
  );
  await user.click(screen.getByRole("combobox", { name: `Type for parameter ${index}` }));
  await user.click(screen.getByRole("option", { name: parameter.type }));
  if (parameter.value !== undefined) {
    await user.type(
      screen.getByRole("textbox", { name: `Value for parameter ${index}` }),
      parameter.value,
    );
  }
}

/**
 * Opens the "Select query" picker and chooses the option with the given name.
 *
 * @param user - The userEvent instance.
 * @param optionName - The visible option label to select.
 */
async function selectSource(user: UserEvent, optionName: string) {
  await user.click(screen.getByRole("combobox", { name: /sql query source/i }));
  await user.click(screen.getByRole("option", { name: optionName }));
}

describe("SqlQueryForm parameter value placement", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // US2 scenario 1: the stored tab owns its parameters, so the shared section
  // that used to sit below the tabs is gone and the value input lives inside
  // the tab panel.
  it("renders the stored query's value inputs inside the tab, not below it", async () => {
    const { user } = renderForm();

    await selectSource(user, "Parameterised query");

    expect(screen.queryByText(RUNTIME_SECTION)).not.toBeInTheDocument();
    const panel = screen.getByRole("tabpanel");
    expect(
      within(panel).queryByRole("textbox", { name: "Runtime value for patient_id" }),
    ).toBeInTheDocument();
  });

  // On the "Provide SQL" tab each row carries its own value, so no separate
  // section is rendered below the tabs.
  it("shows no runtime params section on the Provide SQL tab", async () => {
    const { user } = renderForm();
    await user.click(screen.getByRole("tab", { name: /provide sql/i }));
    expect(screen.queryByText(RUNTIME_SECTION)).not.toBeInTheDocument();
  });

  // US2 scenario 4: values are keyed by parameter name, so binding a
  // reporting period once carries it to every query declaring that name.
  it("retains a bound value when switching to a query declaring the same parameter", async () => {
    const { user } = renderForm();

    await selectSource(user, "Period query");
    await user.type(
      screen.getByRole("textbox", { name: "Runtime value for period_end" }),
      "2025-06-30",
    );

    await selectSource(user, "Other period query");

    const panel = screen.getByRole("tabpanel");
    const valueInput = within(panel).queryByRole("textbox", {
      name: "Runtime value for period_end",
    });
    expect(valueInput).toHaveValue("2025-06-30");
  });
});

describe("SqlQueryForm stored execution", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // Executing a stored query attaches the resolved SQL to the request so the
  // result card can show what ran, even though only the reference is sent to
  // the server.
  it("forwards the selected query's SQL on the request", async () => {
    const onExecute = vi.fn();
    const { user } = renderForm({ onExecute });

    await selectSource(user, "Plain query");
    await user.click(screen.getByRole("button", { name: /execute/i }));

    expect(onExecute).toHaveBeenCalledTimes(1);
    expect(onExecute).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: "stored",
        libraryId: "plain-query",
        sql: "SELECT 1",
      }),
    );
  });

  // A failed save is reported through the shared error presentation, which
  // announces it as an alert.
  it("announces a failed save as an alert", async () => {
    const { user } = renderForm({
      onSaveToServer: vi.fn().mockRejectedValue(new Error("Save rejected by the server")),
    });

    // Saving requires a title, some SQL, and at least one resolved view.
    await authorInlineQuery(user, { title: "My query" });

    await user.click(screen.getByRole("button", { name: /save to server/i }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Save rejected by the server");
  });
});

describe("SqlQueryForm inline execution", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // The value typed on the row is the value bound on this run, so it must reach
  // the request along with the type it was declared as.
  it("binds the inline row's value on the request", async () => {
    const onExecute = vi.fn();
    const { user } = renderForm({ onExecute });

    // Executing an inline query requires some SQL and at least one resolved
    // view, and period_end is declared as a date with the value to bind.
    await authorInlineQuery(user, {
      sql: SAVED_QUERY_SQL,
      parameter: { name: "period_end", type: "date", value: "2025-06-30" },
    });

    await user.click(screen.getByRole("button", { name: /execute/i }));

    expect(onExecute).toHaveBeenCalledTimes(1);
    expect(onExecute).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: "inline",
        bindings: { period_end: "2025-06-30" },
        parameterTypes: { period_end: "date" },
      }),
    );
  });
});

describe("SqlQueryForm save seeding", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  /**
   * Renders the form with a save resolving to the stored "Saved period query",
   * so the returned id matches a query in the mocked list and the stored tab
   * can render that query's declarations afterwards.
   *
   * @returns The userEvent instance for driving interactions.
   */
  function renderSavingForm() {
    return renderForm({
      onSaveToServer: vi.fn().mockResolvedValue({
        id: "saved-query",
        title: "Saved period query",
      }),
    });
  }

  /**
   * Authors an inline query declaring period_end and saves it to the server.
   *
   * @param user - The userEvent instance.
   * @param value - The value typed against the row; omitted leaves it empty.
   */
  async function saveInlineQuery(user: UserEvent, value?: string) {
    await authorInlineQuery(user, {
      title: "Saved period query",
      sql: SAVED_QUERY_SQL,
      parameter: { name: "period_end", type: "date", value },
    });
    await user.click(screen.getByRole("button", { name: /save to server/i }));
  }

  // US3 scenario 2, first half (FR-009): a successful save leaves the user on
  // the stored tab with the query just saved selected, identified by its SQL
  // in the tab's preview.
  it("switches to the stored tab with the saved query selected", async () => {
    const { user } = renderSavingForm();

    await saveInlineQuery(user, "2025-06-30");

    const preview = await screen.findByRole("textbox", { name: "Decoded SQL preview" });
    expect(preview).toHaveValue(SAVED_QUERY_SQL);
    expect(screen.getByRole("tab", { name: /select query/i })).toHaveAttribute(
      "aria-selected",
      "true",
    );
  });

  // US3 scenario 2: the value typed against the inline row prefills the saved
  // query's value input, so the query can be executed without retyping.
  it("prefills the saved query's value input with the value typed inline", async () => {
    const { user } = renderSavingForm();

    await saveInlineQuery(user, "2025-06-30");

    const valueInput = await screen.findByRole("textbox", {
      name: "Runtime value for period_end",
    });
    expect(valueInput).toHaveValue("2025-06-30");
    // The prefilled input is the stored tab's own, not a leftover section.
    expect(
      within(screen.getByRole("tabpanel")).getByRole("textbox", {
        name: "Runtime value for period_end",
      }),
    ).toBe(valueInput);
  });

  // The user has just typed the inline values, so they win over a value
  // retained under the same name from an earlier stored-query binding.
  it("overwrites a retained binding with the value typed inline", async () => {
    const { user } = renderSavingForm();

    await selectSource(user, "Period query");
    await user.type(
      screen.getByRole("textbox", { name: "Runtime value for period_end" }),
      "2020-01-01",
    );

    await saveInlineQuery(user, "2025-06-30");

    const valueInput = await screen.findByRole("textbox", {
      name: "Runtime value for period_end",
    });
    expect(valueInput).toHaveValue("2025-06-30");
  });

  // A row left empty contributes no binding, so a value retained under the
  // same name survives the save rather than being cleared.
  it("leaves a retained binding untouched when the inline row has no value", async () => {
    const { user } = renderSavingForm();

    await selectSource(user, "Period query");
    await user.type(
      screen.getByRole("textbox", { name: "Runtime value for period_end" }),
      "2020-01-01",
    );

    await saveInlineQuery(user);

    const valueInput = await screen.findByRole("textbox", {
      name: "Runtime value for period_end",
    });
    expect(valueInput).toHaveValue("2020-01-01");
  });
});

describe("SqlQueryForm parameter gating", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  /**
   * Renders the form with an export-set handler, so the "Add to export set"
   * button is present and its gating can be observed.
   *
   * @param overrides - Props to override beyond the export-set handler.
   * @returns The userEvent instance for driving interactions.
   */
  function renderExportableForm(
    overrides: Readonly<{
      onExecute?: (request: SqlQueryRequest) => void;
      onAddToExportSet?: (request: SqlQueryRequest) => void;
    }> = {},
  ) {
    return renderForm({ onAddToExportSet: vi.fn(), ...overrides });
  }

  // US4 scenario 1: an unbound parameter can only produce a server failure, so
  // neither submission is offered and the input says which value is missing.
  it("disables Execute and Add to export set while a stored parameter is unbound", async () => {
    const onExecute = vi.fn();
    const onAddToExportSet = vi.fn();
    const { user } = renderExportableForm({ onExecute, onAddToExportSet });

    await selectSource(user, "Parameterised query");

    const execute = screen.getByRole("button", { name: /execute/i });
    const addToExportSet = screen.getByRole("button", { name: /add to export set/i });
    expect(execute).toBeDisabled();
    expect(addToExportSet).toBeDisabled();
    expect(screen.getByRole("textbox", { name: "Runtime value for patient_id" })).toBeRequired();

    // SC-004: the buttons are the only way to submit, so with both refusing
    // the click no request can reach the server unbound.
    await user.click(execute);
    await user.click(addToExportSet);
    expect(onExecute).not.toHaveBeenCalled();
    expect(onAddToExportSet).not.toHaveBeenCalled();
  });

  // US4 scenario 2: with every declared parameter valued, both submissions are
  // available and the input is no longer marked.
  it("enables Execute and Add to export set once the stored parameter is valued", async () => {
    const { user } = renderExportableForm();

    await selectSource(user, "Parameterised query");
    const valueInput = screen.getByRole("textbox", { name: "Runtime value for patient_id" });
    await user.type(valueInput, "patient-1");

    expect(screen.getByRole("button", { name: /execute/i })).toBeEnabled();
    expect(screen.getByRole("button", { name: /add to export set/i })).toBeEnabled();
    expect(valueInput).not.toBeRequired();
  });

  // FR-007: an empty value blocks execution, but nothing about a value is
  // persisted, so it must not block saving the declarations.
  it("disables Execute but not Save while an inline row has no value", async () => {
    const { user } = renderExportableForm();

    await authorInlineQuery(user, {
      title: "Saved period query",
      sql: SAVED_QUERY_SQL,
      parameter: { name: "period_end", type: "date" },
    });

    expect(screen.getByRole("button", { name: /execute/i })).toBeDisabled();
    expect(screen.getByRole("button", { name: /add to export set/i })).toBeDisabled();
    expect(screen.getByRole("button", { name: /save to server/i })).toBeEnabled();
  });

  // FR-007: one name can only bind one value, so an ambiguous declaration
  // blocks both running and saving until it is resolved.
  it("disables Execute and Save while two inline rows declare the same name", async () => {
    const { user } = renderExportableForm();

    await authorInlineQuery(user, {
      title: "Saved period query",
      sql: SAVED_QUERY_SQL,
      parameter: { name: "period_end", type: "date", value: "2025-06-30" },
    });
    await declareParameter(user, 2, { name: "period_end", type: "date", value: "2025-01-01" });

    expect(screen.getByRole("button", { name: /execute/i })).toBeDisabled();
    expect(screen.getByRole("button", { name: /add to export set/i })).toBeDisabled();
    expect(screen.getByRole("button", { name: /save to server/i })).toBeDisabled();
  });

  // A boolean switch has no unbound state, so an untouched row is complete and
  // binds what the switch displays.
  it("executes an untouched boolean parameter, binding false", async () => {
    const onExecute = vi.fn();
    const { user } = renderExportableForm({ onExecute });

    await authorInlineQuery(user, {
      sql: "SELECT * FROM patients WHERE active = :active",
      parameter: { name: "active", type: "boolean" },
    });

    expect(screen.getByRole("button", { name: /execute/i })).toBeEnabled();
    await user.click(screen.getByRole("button", { name: /execute/i }));

    expect(onExecute).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: "inline",
        bindings: { active: "false" },
        parameterTypes: { active: "boolean" },
      }),
    );
  });

  // US4 scenario 3: with nothing declared there is nothing to bind, so the
  // gating rests on the existing non-parameter conditions alone.
  it("leaves Execute enabled on an inline query with no parameter rows", async () => {
    const { user } = renderExportableForm();

    await authorInlineQuery(user, { sql: "SELECT 1" });

    expect(screen.getByRole("button", { name: /execute/i })).toBeEnabled();
    expect(screen.getByRole("button", { name: /add to export set/i })).toBeEnabled();
  });
});

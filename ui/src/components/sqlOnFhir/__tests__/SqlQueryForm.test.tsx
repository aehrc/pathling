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
 * stored tab are retained by parameter name across query selections.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, within } from "../../../test/testUtils";
import { SqlQueryForm } from "../SqlQueryForm";

import type { SqlQueryLibrarySummary } from "../../../types/sqlQuery";

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
    data: [PARAM_QUERY, PLAIN_QUERY, PERIOD_QUERY, OTHER_PERIOD_QUERY],
    isLoading: false,
  }),
  useSqlViews: () => ({ data: [SQL_VIEW], isLoading: false }),
  useViewDefinitions: () => ({ data: [] }),
  useClipboard: () => vi.fn(),
}));

const RUNTIME_SECTION = "Runtime parameter values";

/**
 * Renders the form with inert callbacks.
 *
 * @returns The userEvent instance for driving interactions.
 */
function renderForm() {
  const user = userEvent.setup();
  render(
    <SqlQueryForm
      onExecute={vi.fn()}
      onSaveToServer={vi.fn()}
      isExecuting={false}
      isSaving={false}
    />,
  );
  return user;
}

/**
 * Opens the "Select query" picker and chooses the option with the given name.
 *
 * @param user - The userEvent instance.
 * @param optionName - The visible option label to select.
 */
async function selectSource(user: ReturnType<typeof userEvent.setup>, optionName: string) {
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
    const user = renderForm();

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
    const user = renderForm();
    await user.click(screen.getByRole("tab", { name: /provide sql/i }));
    expect(screen.queryByText(RUNTIME_SECTION)).not.toBeInTheDocument();
  });

  // US2 scenario 4: values are keyed by parameter name, so binding a
  // reporting period once carries it to every query declaring that name.
  it("retains a bound value when switching to a query declaring the same parameter", async () => {
    const user = renderForm();

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
    const user = userEvent.setup();
    const onExecute = vi.fn();
    render(
      <SqlQueryForm
        onExecute={onExecute}
        onSaveToServer={vi.fn()}
        isExecuting={false}
        isSaving={false}
      />,
    );

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
    const user = userEvent.setup();
    render(
      <SqlQueryForm
        onExecute={vi.fn()}
        onSaveToServer={vi.fn().mockRejectedValue(new Error("Save rejected by the server"))}
        isExecuting={false}
        isSaving={false}
      />,
    );

    // Saving requires a title, some SQL, and at least one resolved view.
    await user.click(screen.getByRole("tab", { name: /provide sql/i }));
    await user.type(screen.getByRole("textbox", { name: /library title/i }), "My query");
    await user.type(screen.getByRole("textbox", { name: /^sql$/i }), "SELECT 1");
    await user.click(screen.getByRole("button", { name: /add view/i }));
    await user.type(screen.getByRole("textbox", { name: /label for view 1/i }), "patients");
    await user.click(screen.getByRole("combobox", { name: /source for view 1/i }));
    await user.click(screen.getByRole("option", { name: "Active patients view" }));

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
    const user = userEvent.setup();
    const onExecute = vi.fn();
    render(
      <SqlQueryForm
        onExecute={onExecute}
        onSaveToServer={vi.fn()}
        isExecuting={false}
        isSaving={false}
      />,
    );

    // Executing an inline query requires some SQL and at least one resolved
    // view.
    await user.click(screen.getByRole("tab", { name: /provide sql/i }));
    await user.type(
      screen.getByRole("textbox", { name: /^sql$/i }),
      "SELECT * FROM patients WHERE date <= :period_end",
    );
    await user.click(screen.getByRole("button", { name: /add view/i }));
    await user.type(screen.getByRole("textbox", { name: /label for view 1/i }), "patients");
    await user.click(screen.getByRole("combobox", { name: /source for view 1/i }));
    await user.click(screen.getByRole("option", { name: "Active patients view" }));

    // Declare period_end as a date and give it the value to bind.
    await user.click(screen.getByRole("button", { name: /add parameter/i }));
    await user.type(screen.getByRole("textbox", { name: "Name for parameter 1" }), "period_end");
    await user.click(screen.getByRole("combobox", { name: "Type for parameter 1" }));
    await user.click(screen.getByRole("option", { name: "date" }));
    await user.type(screen.getByRole("textbox", { name: "Value for parameter 1" }), "2025-06-30");

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

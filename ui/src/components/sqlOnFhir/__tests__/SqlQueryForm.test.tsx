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
 * Tests for the SqlQueryForm component, focused on the conditional
 * visibility of the "Runtime parameter values" section.
 *
 * On the "Select query" tab the section appears only when the selected
 * source declares parameters; on the "Provide SQL" tab it is always shown.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
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

const SQL_VIEW = makeSummary({
  id: "sql-view",
  title: "Active patients view",
  parameters: [],
});

// The hooks barrel transitively imports main.tsx (via AuthContext). Mock it
// with the stored lists the form and its picker consume.
vi.mock("../../../hooks", () => ({
  useSqlQueryLibraries: () => ({
    data: [PARAM_QUERY, PLAIN_QUERY],
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

describe("SqlQueryForm runtime parameter visibility", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // With nothing selected on the stored tab, there is nothing to bind.
  it("hides the runtime params section when no source is selected", () => {
    renderForm();
    expect(screen.queryByText(RUNTIME_SECTION)).not.toBeInTheDocument();
  });

  // A selected parameterised query exposes its bindings.
  it("shows the runtime params section for a parameterised query", async () => {
    const user = renderForm();
    await selectSource(user, "Parameterised query");
    expect(screen.getByText(RUNTIME_SECTION)).toBeInTheDocument();
  });

  // A selected param-less query has nothing to bind, so the section is hidden.
  it("hides the runtime params section for a param-less query", async () => {
    const user = renderForm();
    await selectSource(user, "Plain query");
    expect(screen.queryByText(RUNTIME_SECTION)).not.toBeInTheDocument();
  });

  // A SQLView never declares parameters, so the section is hidden.
  it("hides the runtime params section for a SQLView", async () => {
    const user = renderForm();
    await selectSource(user, "Active patients view");
    expect(screen.queryByText(RUNTIME_SECTION)).not.toBeInTheDocument();
  });

  // On the "Provide SQL" tab the section is always shown, preserving the
  // inline-authoring behaviour.
  it("always shows the runtime params section on the Provide SQL tab", async () => {
    const user = renderForm();
    await user.click(screen.getByRole("tab", { name: /provide sql/i }));
    expect(screen.getByText(RUNTIME_SECTION)).toBeInTheDocument();
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
});

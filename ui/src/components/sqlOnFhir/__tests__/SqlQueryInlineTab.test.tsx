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
 * Verifies the "Views" editor (renamed from "Tables"), the grouped source
 * selector offering ViewDefinitions and SQLViews, the discriminated row
 * update on selection, and the disabled "nothing to reference" state.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { SqlQueryInlineTab } from "../SqlQueryInlineTab";

import type { SqlQueryRelatedArtifact } from "../../../types/sqlQuery";

const VIEW_DEFINITIONS = [{ id: "patient-demographics", name: "Patient Demographics" }];
const SQL_VIEWS = [{ id: "active-patients", name: "Active patients" }];

/**
 * Renders the inline tab with sensible defaults and inert callbacks.
 *
 * @param overrides - Props to override on the defaults.
 * @param overrides.tables - The view rows to render.
 * @param overrides.viewDefinitions - Available ViewDefinition options.
 * @param overrides.sqlViews - Available SQLView options.
 * @returns The userEvent instance and the onTablesChange spy.
 */
function renderTab(
  overrides: {
    tables?: SqlQueryRelatedArtifact[];
    viewDefinitions?: Array<{ id: string; name: string }>;
    sqlViews?: Array<{ id: string; name: string }>;
  } = {},
) {
  const user = userEvent.setup();
  const onTablesChange = vi.fn();
  render(
    <SqlQueryInlineTab
      title=""
      onTitleChange={vi.fn()}
      sql=""
      onSqlChange={vi.fn()}
      tables={overrides.tables ?? []}
      onTablesChange={onTablesChange}
      parameters={[]}
      onParametersChange={vi.fn()}
      viewDefinitions={overrides.viewDefinitions ?? VIEW_DEFINITIONS}
      sqlViews={overrides.sqlViews ?? SQL_VIEWS}
    />,
  );
  return { user, onTablesChange };
}

/** A single empty view row. */
const EMPTY_ROW: SqlQueryRelatedArtifact = {
  rowId: "r1",
  label: "patients",
  referenceId: "",
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

  // Selecting a ViewDefinition stamps the row with the view-definition kind.
  it("updates the row with a view-definition reference", async () => {
    const { user, onTablesChange } = renderTab({ tables: [EMPTY_ROW] });

    await user.click(screen.getByRole("combobox", { name: /source for view 1/i }));
    await user.click(screen.getByRole("option", { name: "Patient Demographics" }));

    expect(onTablesChange).toHaveBeenCalledWith([
      expect.objectContaining({
        rowId: "r1",
        referenceType: "view-definition",
        referenceId: "patient-demographics",
      }),
    ]);
  });

  // Selecting a SQLView stamps the row with the sql-view kind.
  it("updates the row with a sql-view reference", async () => {
    const { user, onTablesChange } = renderTab({ tables: [EMPTY_ROW] });

    await user.click(screen.getByRole("combobox", { name: /source for view 1/i }));
    await user.click(screen.getByRole("option", { name: "Active patients" }));

    expect(onTablesChange).toHaveBeenCalledWith([
      expect.objectContaining({
        rowId: "r1",
        referenceType: "sql-view",
        referenceId: "active-patients",
      }),
    ]);
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

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
 * Tests for the type filters section of the ExportForm component.
 *
 * These tests verify that the form correctly renders the type filter UI,
 * handles adding and removing type filter entries, populates search parameter
 * dropdowns from the CapabilityStatement, resets parameters when the resource
 * type changes, and serialises type filters into the export request.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { ExportForm } from "../ExportForm";

import type { SearchParamCapability } from "../../../hooks/useServerCapabilities";
import type { ExportRequest } from "../../../types/export";

/**
 * Helper to get comboboxes by index. The ExportForm always renders:
 * - index 0: export level selector
 * - index 1: output format selector (from ExportOptions)
 * - index 2+: type filter resource type selectors (one per added entry)
 * - after resource type selection: search param name selectors follow
 *
 * @param index - The zero-based index of the combobox to retrieve.
 * @returns The combobox element at the specified index.
 */
function getCombobox(index: number): HTMLElement {
  return screen.getAllByRole("combobox")[index];
}

describe("ExportForm type filters", () => {
  const defaultResourceTypes = ["Patient", "Observation", "Condition"];
  const defaultSearchParams: Record<string, SearchParamCapability[]> = {
    Patient: [
      { name: "gender", type: "token" },
      { name: "birthdate", type: "date" },
      { name: "name", type: "string" },
    ],
    Observation: [
      { name: "status", type: "token" },
      { name: "code", type: "token" },
    ],
    Condition: [{ name: "clinical-status", type: "token" }],
  };
  const mockOnSubmit = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("initial render", () => {
    it("renders the type filters section with add button", () => {
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      // The label "Type filters" is rendered as a heading-like label.
      expect(screen.getByText("Type filters")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /add type filter/i })).toBeInTheDocument();
    });

    it("shows empty state text when no type filters are added", () => {
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      expect(screen.getByText(/no type filters configured/i)).toBeInTheDocument();
    });
  });

  describe("adding and removing type filters", () => {
    it("adds a type filter entry when the add button is clicked", async () => {
      const user = userEvent.setup();
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      await user.click(screen.getByRole("button", { name: /add type filter/i }));

      // Should now have 3 comboboxes: export level, output format, resource type.
      expect(screen.getAllByRole("combobox")).toHaveLength(3);
      // Empty state text should be gone.
      expect(screen.queryByText(/no type filters configured/i)).not.toBeInTheDocument();
    });

    it("adds multiple type filter entries", async () => {
      const user = userEvent.setup();
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      await user.click(screen.getByRole("button", { name: /add type filter/i }));
      await user.click(screen.getByRole("button", { name: /add type filter/i }));

      // Should have 4 comboboxes: export level, output format, 2x resource type.
      expect(screen.getAllByRole("combobox")).toHaveLength(4);
    });

    it("removes a type filter entry when the remove button is clicked", async () => {
      const user = userEvent.setup();
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      // Add two entries.
      await user.click(screen.getByRole("button", { name: /add type filter/i }));
      await user.click(screen.getByRole("button", { name: /add type filter/i }));
      expect(screen.getAllByRole("combobox")).toHaveLength(4);

      // Find the remove buttons (red icon buttons within type filter cards).
      const removeButtons = screen
        .getAllByRole("button")
        .filter(
          (btn) =>
            btn.querySelector("svg") !== null &&
            !btn.textContent &&
            btn.closest("[data-accent-color='red']") !== null,
        );
      await user.click(removeButtons[0]);

      // Should only have 3 comboboxes now: export level, output format, 1x resource type.
      expect(screen.getAllByRole("combobox")).toHaveLength(3);
    });
  });

  describe("resource type selection", () => {
    it("shows search params input after selecting a resource type", async () => {
      const user = userEvent.setup();
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      // Add a type filter entry.
      await user.click(screen.getByRole("button", { name: /add type filter/i }));

      // Click the resource type combobox (index 2).
      await user.click(getCombobox(2));
      await user.click(screen.getByRole("option", { name: "Patient" }));

      // The search params input should now be visible (exact match to avoid matching help text).
      expect(screen.getByText("Search parameters")).toBeInTheDocument();
    });

    it("resets search param rows when resource type changes", async () => {
      const user = userEvent.setup();
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      // Add a type filter entry and select Patient.
      await user.click(screen.getByRole("button", { name: /add type filter/i }));
      await user.click(getCombobox(2));
      await user.click(screen.getByRole("option", { name: "Patient" }));

      // Add a second search param row.
      await user.click(screen.getByRole("button", { name: /add parameter/i }));
      // There should be two value inputs now.
      expect(screen.getAllByPlaceholderText(/e\.g\., male/i)).toHaveLength(2);

      // Change resource type to Observation (the resource type combobox is still at index 2).
      await user.click(getCombobox(2));
      await user.click(screen.getByRole("option", { name: "Observation" }));

      // Rows should have been reset to a single empty row.
      expect(screen.getAllByPlaceholderText(/e\.g\., male/i)).toHaveLength(1);
    });
  });

  describe("form submission with type filters", () => {
    it("submits without type filters when none are configured", async () => {
      const user = userEvent.setup();
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      await user.click(screen.getByRole("button", { name: /start export/i }));

      const request = mockOnSubmit.mock.calls[0][0] as ExportRequest;
      expect(request.typeFilters).toBeUndefined();
    });

    it("includes type filters with resource type and params in the request", async () => {
      const user = userEvent.setup();
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      // Add a type filter entry and select Patient.
      await user.click(screen.getByRole("button", { name: /add type filter/i }));
      await user.click(getCombobox(2));
      await user.click(screen.getByRole("option", { name: "Patient" }));

      // Select a search parameter (combobox at index 3 is the param name selector).
      await user.click(getCombobox(3));
      await user.click(screen.getByRole("option", { name: /gender/i }));

      const valueInput = screen.getByPlaceholderText(/e\.g\., male/i);
      await user.type(valueInput, "female");

      await user.click(screen.getByRole("button", { name: /start export/i }));

      const request = mockOnSubmit.mock.calls[0][0] as ExportRequest;
      expect(request.typeFilters).toEqual([
        {
          resourceType: "Patient",
          params: { gender: ["female"] },
        },
      ]);
    });

    it("includes type filter entry with no params when resource type is selected but no search params filled", async () => {
      const user = userEvent.setup();
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      // Add a type filter entry and select Patient without filling params.
      await user.click(screen.getByRole("button", { name: /add type filter/i }));
      await user.click(getCombobox(2));
      await user.click(screen.getByRole("option", { name: "Patient" }));

      await user.click(screen.getByRole("button", { name: /start export/i }));

      const request = mockOnSubmit.mock.calls[0][0] as ExportRequest;
      // Entry with resource type but empty params is included.
      expect(request.typeFilters).toEqual([{ resourceType: "Patient", params: {} }]);
    });

    it("excludes type filter entries with no resource type selected", async () => {
      const user = userEvent.setup();
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      // Add a type filter entry but do not select a resource type.
      await user.click(screen.getByRole("button", { name: /add type filter/i }));

      await user.click(screen.getByRole("button", { name: /start export/i }));

      const request = mockOnSubmit.mock.calls[0][0] as ExportRequest;
      // Entry without resource type should be excluded.
      expect(request.typeFilters).toBeUndefined();
    });

    it("handles multiple type filter entries in the request", async () => {
      const user = userEvent.setup();
      render(
        <ExportForm
          onSubmit={mockOnSubmit}
          resourceTypes={defaultResourceTypes}
          searchParams={defaultSearchParams}
        />,
      );

      // Add first type filter: Patient with gender=female.
      await user.click(screen.getByRole("button", { name: /add type filter/i }));
      await user.click(getCombobox(2));
      await user.click(screen.getByRole("option", { name: "Patient" }));

      // Select search param (index 3 = param name for first entry).
      await user.click(getCombobox(3));
      await user.click(screen.getByRole("option", { name: /gender/i }));

      const valueInputs = screen.getAllByPlaceholderText(/e\.g\., male/i);
      await user.type(valueInputs[0], "female");

      // Add second type filter: Observation with status=final.
      await user.click(screen.getByRole("button", { name: /add type filter/i }));
      // Resource type combobox for second entry is now at index 4
      // (export level, output format, Patient resource type, Patient param name, new resource type).
      await user.click(getCombobox(4));
      await user.click(screen.getByRole("option", { name: "Observation" }));

      // Param name selector for second entry is now at index 5.
      await user.click(getCombobox(5));
      await user.click(screen.getByRole("option", { name: /status/i }));

      const allValueInputs = screen.getAllByPlaceholderText(/e\.g\., male/i);
      // The second type filter's value input.
      await user.type(allValueInputs[1], "final");

      await user.click(screen.getByRole("button", { name: /start export/i }));

      const request = mockOnSubmit.mock.calls[0][0] as ExportRequest;
      expect(request.typeFilters).toEqual([
        { resourceType: "Patient", params: { gender: ["female"] } },
        { resourceType: "Observation", params: { status: ["final"] } },
      ]);
    });
  });
});

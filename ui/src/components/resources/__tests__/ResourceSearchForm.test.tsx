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
 * Tests for the ResourceSearchForm component which handles FHIR resource
 * search configuration with FHIRPath filters.
 *
 * These tests verify that the form correctly renders resource type selection,
 * manages multiple filter inputs, validates form data before submission,
 * handles keyboard shortcuts for submission, and handles disabled and loading
 * states appropriately.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, within } from "../../../test/testUtils";
import { ResourceSearchForm } from "../ResourceSearchForm";

import type { SearchParamCapability } from "../../../hooks/useServerCapabilities";
import type { SearchRequest } from "../../../types/search";

describe("ResourceSearchForm", () => {
  const defaultResourceTypes = ["Patient", "Observation", "Condition"];
  const mockOnSubmit = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("initial render", () => {
    it("renders the form heading", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByRole("heading", { name: /search resources/i })).toBeInTheDocument();
    });

    it("renders the resource type selector", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByText(/resource type/i)).toBeInTheDocument();
    });

    it("renders the FHIRPath filters section", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByText(/fhirpath filters/i)).toBeInTheDocument();
    });

    it("renders the search button", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByRole("button", { name: /^search$/i })).toBeInTheDocument();
    });

    it("renders the add filter button", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByRole("button", { name: /add filter/i })).toBeInTheDocument();
    });

    it("renders one filter row by default", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const filterInputs = screen.getAllByPlaceholderText(/gender = 'female'/i);
      expect(filterInputs).toHaveLength(1);
    });

    it("defaults to Patient resource type", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // The combobox should show "Patient" as selected.
      expect(screen.getByRole("combobox")).toHaveTextContent("Patient");
    });
  });

  describe("filter management", () => {
    it("adds a new filter row when add filter button is clicked", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const addButton = screen.getByRole("button", { name: /add filter/i });
      await user.click(addButton);

      const filterInputs = screen.getAllByPlaceholderText(/gender = 'female'/i);
      expect(filterInputs).toHaveLength(2);
    });

    it("removes a filter row when remove button is clicked", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Add a second row first.
      const addButton = screen.getByRole("button", { name: /add filter/i });
      await user.click(addButton);

      // Now we should have two rows.
      let filterInputs = screen.getAllByPlaceholderText(/gender = 'female'/i);
      expect(filterInputs).toHaveLength(2);

      // Find and click a remove button.
      const allButtons = screen.getAllByRole("button");
      const removeButtons = allButtons.filter(
        (btn) => btn.querySelector("svg") !== null && !btn.textContent,
      );
      const enabledRemoveButton = removeButtons.find((btn) => !btn.hasAttribute("disabled"));
      if (enabledRemoveButton) {
        await user.click(enabledRemoveButton);
      }

      filterInputs = screen.getAllByPlaceholderText(/gender = 'female'/i);
      expect(filterInputs).toHaveLength(1);
    });

    it("disables the remove button when there is only one filter row", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const allButtons = screen.getAllByRole("button");
      const removeButton = allButtons.find(
        (btn) => btn.querySelector("svg") !== null && !btn.textContent,
      );

      expect(removeButton).toBeDisabled();
    });

    it("allows updating filter expression", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const filterInput = screen.getByPlaceholderText(/gender = 'female'/i);
      await user.type(filterInput, "active = true");

      expect(filterInput).toHaveValue("active = true");
    });
  });

  describe("resource type selection", () => {
    it("allows changing the resource type", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);

      const observationOption = screen.getByRole("option", { name: "Observation" });
      await user.click(observationOption);

      // Submit and verify the resource type was changed.
      await user.click(screen.getByRole("button", { name: /^search$/i }));

      const request = mockOnSubmit.mock.calls[0][0] as SearchRequest;
      expect(request.resourceType).toBe("Observation");
    });
  });

  describe("form submission", () => {
    it("submits the form with correct data", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      await user.click(screen.getByRole("button", { name: /^search$/i }));

      expect(mockOnSubmit).toHaveBeenCalledTimes(1);
      const request = mockOnSubmit.mock.calls[0][0] as SearchRequest;
      expect(request.resourceType).toBe("Patient");
      expect(request.filters).toEqual([]);
    });

    it("includes non-empty filter expressions in the request", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const filterInput = screen.getByPlaceholderText(/gender = 'female'/i);
      await user.type(filterInput, "gender = 'male'");

      await user.click(screen.getByRole("button", { name: /^search$/i }));

      const request = mockOnSubmit.mock.calls[0][0] as SearchRequest;
      expect(request.filters).toEqual(["gender = 'male'"]);
    });

    it("filters out empty filter expressions on submission", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Add a second filter row.
      await user.click(screen.getByRole("button", { name: /add filter/i }));

      // Only fill in the first filter.
      const filterInputs = screen.getAllByPlaceholderText(/gender = 'female'/i);
      await user.type(filterInputs[0], "active = true");

      await user.click(screen.getByRole("button", { name: /^search$/i }));

      const request = mockOnSubmit.mock.calls[0][0] as SearchRequest;
      expect(request.filters).toEqual(["active = true"]);
    });

    it("submits multiple filter expressions", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Add a second filter row.
      await user.click(screen.getByRole("button", { name: /add filter/i }));

      // Fill in both filters.
      const filterInputs = screen.getAllByPlaceholderText(/gender = 'female'/i);
      await user.type(filterInputs[0], "active = true");
      await user.type(filterInputs[1], "gender = 'female'");

      await user.click(screen.getByRole("button", { name: /^search$/i }));

      const request = mockOnSubmit.mock.calls[0][0] as SearchRequest;
      expect(request.filters).toContain("active = true");
      expect(request.filters).toContain("gender = 'female'");
    });
  });

  describe("keyboard shortcuts", () => {
    it("submits the form when Enter is pressed in a filter input", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const filterInput = screen.getByPlaceholderText(/gender = 'female'/i);
      await user.type(filterInput, "active = true{Enter}");

      expect(mockOnSubmit).toHaveBeenCalledTimes(1);
    });

    it("does not submit when Enter is pressed and form is disabled", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={true}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const filterInput = screen.getByPlaceholderText(/gender = 'female'/i);
      await user.type(filterInput, "active = true{Enter}");

      expect(mockOnSubmit).not.toHaveBeenCalled();
    });

    it("does not submit when Enter is pressed and form is loading", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={true}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const filterInput = screen.getByPlaceholderText(/gender = 'female'/i);
      await user.type(filterInput, "active = true{Enter}");

      expect(mockOnSubmit).not.toHaveBeenCalled();
    });
  });

  describe("button disabled states", () => {
    it("disables the search button when disabled is true", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={true}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const searchButton = screen.getByRole("button", { name: /^search$/i });
      expect(searchButton).toBeDisabled();
    });

    it("disables the search button when isLoading is true", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={true}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const searchButton = screen.getByRole("button", { name: /searching/i });
      expect(searchButton).toBeDisabled();
    });

    it("shows searching text when isLoading is true", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={true}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByRole("button", { name: /searching/i })).toBeInTheDocument();
    });

    it("enables the search button when not disabled and not loading", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const searchButton = screen.getByRole("button", { name: /^search$/i });
      expect(searchButton).not.toBeDisabled();
    });
  });

  describe("help text", () => {
    it("displays filter help text", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(
        screen.getByText(/filter expressions are combined with and logic/i),
      ).toBeInTheDocument();
    });

    it("displays empty filter hint", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByText(/leave empty to return all resources/i)).toBeInTheDocument();
    });
  });

  describe("search parameters", () => {
    const mockSearchParams: Record<string, SearchParamCapability[]> = {
      Patient: [
        { name: "gender", type: "token" },
        { name: "birthdate", type: "date" },
        { name: "name", type: "string" },
      ],
      Observation: [
        { name: "code", type: "token" },
        { name: "date", type: "date" },
      ],
      Condition: [],
    };

    it("renders search parameters section with heading", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Use exact text match to distinguish heading from help text.
      expect(screen.getByText("Search parameters")).toBeInTheDocument();
    });

    it("renders one empty parameter row by default", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Should have one value input for search parameters.
      const valueInputs = screen.getAllByPlaceholderText(/e\.g\., male/i);
      expect(valueInputs).toHaveLength(1);
    });

    it("renders add parameter button", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      expect(screen.getByRole("button", { name: /add parameter/i })).toBeInTheDocument();
    });

    it("adds a new parameter row when add parameter button is clicked", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      await user.click(screen.getByRole("button", { name: /add parameter/i }));

      const valueInputs = screen.getAllByPlaceholderText(/e\.g\., male/i);
      expect(valueInputs).toHaveLength(2);
    });

    it("removes a parameter row when remove button is clicked", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Add a second row.
      await user.click(screen.getByRole("button", { name: /add parameter/i }));
      let valueInputs = screen.getAllByPlaceholderText(/e\.g\., male/i);
      expect(valueInputs).toHaveLength(2);

      // Find a remove button near a parameter value input and click it.
      const firstParamInput = valueInputs[0];
      const paramRow = firstParamInput.closest("[style]")!.parentElement!;
      const removeButton = within(paramRow)
        .getAllByRole("button")
        .find(
          (btn) =>
            btn.querySelector("svg") !== null && !btn.textContent && !btn.hasAttribute("disabled"),
        );
      if (removeButton) {
        await user.click(removeButton);
      }

      valueInputs = screen.getAllByPlaceholderText(/e\.g\., male/i);
      expect(valueInputs).toHaveLength(1);
    });

    it("disables the remove button when there is only one parameter row", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Find the remove button adjacent to the parameter value input.
      const paramInput = screen.getByPlaceholderText(/e\.g\., male/i);
      const paramRow = paramInput.closest("[style]")!.parentElement!;
      const removeButtons = within(paramRow)
        .getAllByRole("button")
        .filter((btn) => btn.querySelector("svg") !== null && !btn.textContent);

      expect(removeButtons).toHaveLength(1);
      expect(removeButtons[0]).toBeDisabled();
    });

    it("populates parameter dropdown with options for selected resource type", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Default resource type is Patient. Open the parameter dropdown.
      // The parameter dropdown is the second combobox (first is resource type).
      const comboboxes = screen.getAllByRole("combobox");
      const paramDropdown = comboboxes[1];
      await user.click(paramDropdown);

      // Verify Patient search params are available.
      expect(screen.getByRole("option", { name: /gender/i })).toBeInTheDocument();
      expect(screen.getByRole("option", { name: /birthdate/i })).toBeInTheDocument();
      expect(screen.getByRole("option", { name: /name/i })).toBeInTheDocument();
    });

    it("updates parameter options when resource type changes", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Change resource type to Observation.
      const resourceTypeCombobox = screen.getAllByRole("combobox")[0];
      await user.click(resourceTypeCombobox);
      await user.click(screen.getByRole("option", { name: "Observation" }));

      // Open the parameter dropdown.
      const paramDropdown = screen.getAllByRole("combobox")[1];
      await user.click(paramDropdown);

      // Verify Observation search params are available.
      expect(screen.getByRole("option", { name: /code/i })).toBeInTheDocument();
      expect(screen.getByRole("option", { name: /date/i })).toBeInTheDocument();

      // Patient-specific params should not be present.
      expect(screen.queryByRole("option", { name: /gender/i })).not.toBeInTheDocument();
    });

    it("resets parameter rows to a single empty row when resource type changes", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Select "gender" parameter for Patient and enter a value.
      const paramDropdown = screen.getAllByRole("combobox")[1];
      await user.click(paramDropdown);
      await user.click(screen.getByRole("option", { name: /gender/i }));
      const valueInput = screen.getByPlaceholderText(/e\.g\., male/i);
      await user.type(valueInput, "male");

      // Add a second parameter row.
      await user.click(screen.getByRole("button", { name: /add parameter/i }));
      expect(screen.getAllByPlaceholderText(/e\.g\., male/i)).toHaveLength(2);

      // Change resource type to Observation.
      const resourceTypeCombobox = screen.getAllByRole("combobox")[0];
      await user.click(resourceTypeCombobox);
      await user.click(screen.getByRole("option", { name: "Observation" }));

      // Should be back to a single empty parameter row.
      const valueInputs = screen.getAllByPlaceholderText(/e\.g\., male/i);
      expect(valueInputs).toHaveLength(1);
      expect(valueInputs[0]).toHaveValue("");

      // The parameter dropdown should be cleared (no selection).
      const updatedParamDropdown = screen.getAllByRole("combobox")[1];
      expect(updatedParamDropdown).toHaveTextContent("Select parameter...");
    });

    it("resets FHIRPath filter rows to a single empty row when resource type changes", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Enter a FHIRPath filter expression.
      const filterInput = screen.getByPlaceholderText(/gender = 'female'/i);
      await user.type(filterInput, "active = true");

      // Add a second filter row and fill it in.
      await user.click(screen.getByRole("button", { name: /add filter/i }));
      const filterInputs = screen.getAllByPlaceholderText(/gender = 'female'/i);
      expect(filterInputs).toHaveLength(2);
      await user.type(filterInputs[1], "gender = 'male'");

      // Change resource type to Observation.
      const resourceTypeCombobox = screen.getAllByRole("combobox")[0];
      await user.click(resourceTypeCombobox);
      await user.click(screen.getByRole("option", { name: "Observation" }));

      // Should be back to a single empty filter row.
      const updatedFilterInputs = screen.getAllByPlaceholderText(/gender = 'female'/i);
      expect(updatedFilterInputs).toHaveLength(1);
      expect(updatedFilterInputs[0]).toHaveValue("");
    });

    it("displays help text for search parameters section", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      expect(screen.getByText(/standard fhir search syntax/i)).toBeInTheDocument();
    });

    it("includes search params in submitted request", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Select "gender" parameter and enter a value.
      const paramDropdown = screen.getAllByRole("combobox")[1];
      await user.click(paramDropdown);
      await user.click(screen.getByRole("option", { name: /gender/i }));

      const valueInput = screen.getByPlaceholderText(/e\.g\., male/i);
      await user.type(valueInput, "male");

      await user.click(screen.getByRole("button", { name: /^search$/i }));

      const request = mockOnSubmit.mock.calls[0][0] as SearchRequest;
      expect(request.params).toEqual({ gender: ["male"] });
    });

    it("excludes empty parameter rows from submission", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Add a second parameter row but leave both empty.
      await user.click(screen.getByRole("button", { name: /add parameter/i }));

      // Only fill in the first row.
      const paramDropdown = screen.getAllByRole("combobox")[1];
      await user.click(paramDropdown);
      await user.click(screen.getByRole("option", { name: /gender/i }));

      const valueInputs = screen.getAllByPlaceholderText(/e\.g\., male/i);
      await user.type(valueInputs[0], "female");

      await user.click(screen.getByRole("button", { name: /^search$/i }));

      const request = mockOnSubmit.mock.calls[0][0] as SearchRequest;
      // Only the filled row should be included.
      expect(request.params).toEqual({ gender: ["female"] });
    });

    it("sends multiple values for the same parameter as repeated entries", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Select "birthdate" in the first row.
      const firstParamDropdown = screen.getAllByRole("combobox")[1];
      await user.click(firstParamDropdown);
      await user.click(screen.getByRole("option", { name: /birthdate/i }));

      const firstValueInput = screen.getAllByPlaceholderText(/e\.g\., male/i)[0];
      await user.type(firstValueInput, "gt2000-01-01");

      // Add a second row with the same parameter.
      await user.click(screen.getByRole("button", { name: /add parameter/i }));

      const secondParamDropdown = screen.getAllByRole("combobox")[2];
      await user.click(secondParamDropdown);
      await user.click(screen.getByRole("option", { name: /birthdate/i }));

      const secondValueInput = screen.getAllByPlaceholderText(/e\.g\., male/i)[1];
      await user.type(secondValueInput, "lt2025-01-01");

      await user.click(screen.getByRole("button", { name: /^search$/i }));

      const request = mockOnSubmit.mock.calls[0][0] as SearchRequest;
      expect(request.params).toEqual({
        birthdate: ["gt2000-01-01", "lt2025-01-01"],
      });
    });

    it("combines search params with FHIRPath filters in submission", async () => {
      const user = userEvent.setup();
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
          searchParams={mockSearchParams}
        />,
      );

      // Set a search parameter.
      const paramDropdown = screen.getAllByRole("combobox")[1];
      await user.click(paramDropdown);
      await user.click(screen.getByRole("option", { name: /gender/i }));

      const valueInput = screen.getByPlaceholderText(/e\.g\., male/i);
      await user.type(valueInput, "male");

      // Set a FHIRPath filter.
      const filterInput = screen.getByPlaceholderText(/gender = 'female'/i);
      await user.type(filterInput, "active = true");

      await user.click(screen.getByRole("button", { name: /^search$/i }));

      const request = mockOnSubmit.mock.calls[0][0] as SearchRequest;
      expect(request.params).toEqual({ gender: ["male"] });
      expect(request.filters).toEqual(["active = true"]);
    });

    it("does not render search parameters section when searchParams is not provided", () => {
      render(
        <ResourceSearchForm
          onSubmit={mockOnSubmit}
          isLoading={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.queryByText("Search parameters")).not.toBeInTheDocument();
    });
  });
});

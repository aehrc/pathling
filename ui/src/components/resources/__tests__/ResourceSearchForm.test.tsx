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

import { render, screen } from "../../../test/testUtils";
import { ResourceSearchForm } from "../ResourceSearchForm";

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
});

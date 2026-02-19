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
 * Tests for the SearchParamsInput component which provides a reusable search
 * parameter rows interface with dropdown selection and value inputs.
 *
 * These tests verify row rendering, add/remove behaviour, parameter name and
 * value changes, and the disabled state of the remove button when only one row
 * remains.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../test/testUtils";
import { SearchParamsInput } from "../SearchParamsInput";

import type { SearchParamCapability } from "../../hooks/useServerCapabilities";
import type { SearchParamRowData } from "../SearchParamsInput";

describe("SearchParamsInput", () => {
  const mockAvailableParams: SearchParamCapability[] = [
    { name: "gender", type: "token" },
    { name: "birthdate", type: "date" },
    { name: "name", type: "string" },
  ];

  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("row rendering", () => {
    it("renders the correct number of rows from props", () => {
      const rows: SearchParamRowData[] = [
        { id: 1, paramName: "", value: "" },
        { id: 2, paramName: "", value: "" },
      ];

      render(
        <SearchParamsInput
          availableParams={mockAvailableParams}
          rows={rows}
          onChange={mockOnChange}
        />,
      );

      // Each row has a value input with the placeholder.
      const valueInputs = screen.getAllByPlaceholderText(/e\.g\., male/i);
      expect(valueInputs).toHaveLength(2);
    });

    it("renders the heading and add parameter button", () => {
      const rows: SearchParamRowData[] = [{ id: 1, paramName: "", value: "" }];

      render(
        <SearchParamsInput
          availableParams={mockAvailableParams}
          rows={rows}
          onChange={mockOnChange}
        />,
      );

      expect(screen.getByText("Search parameters")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /add parameter/i })).toBeInTheDocument();
    });

    it("renders help text about FHIR search syntax", () => {
      const rows: SearchParamRowData[] = [{ id: 1, paramName: "", value: "" }];

      render(
        <SearchParamsInput
          availableParams={mockAvailableParams}
          rows={rows}
          onChange={mockOnChange}
        />,
      );

      expect(screen.getByText(/standard fhir search syntax/i)).toBeInTheDocument();
    });

    it("renders parameter dropdown with available options", async () => {
      const user = userEvent.setup();
      const rows: SearchParamRowData[] = [{ id: 1, paramName: "", value: "" }];

      render(
        <SearchParamsInput
          availableParams={mockAvailableParams}
          rows={rows}
          onChange={mockOnChange}
        />,
      );

      // Open the parameter dropdown.
      const dropdown = screen.getByRole("combobox");
      await user.click(dropdown);

      // All three parameters should be available.
      expect(screen.getByRole("option", { name: /gender/i })).toBeInTheDocument();
      expect(screen.getByRole("option", { name: /birthdate/i })).toBeInTheDocument();
      expect(screen.getByRole("option", { name: /name/i })).toBeInTheDocument();
    });
  });

  describe("add row", () => {
    it("calls onChange with a new empty row appended when add button is clicked", async () => {
      const user = userEvent.setup();
      const rows: SearchParamRowData[] = [{ id: 1, paramName: "gender", value: "male" }];

      render(
        <SearchParamsInput
          availableParams={mockAvailableParams}
          rows={rows}
          onChange={mockOnChange}
        />,
      );

      await user.click(screen.getByRole("button", { name: /add parameter/i }));

      expect(mockOnChange).toHaveBeenCalledTimes(1);
      const newRows = mockOnChange.mock.calls[0][0] as SearchParamRowData[];
      expect(newRows).toHaveLength(2);
      // First row should be unchanged.
      expect(newRows[0]).toEqual({ id: 1, paramName: "gender", value: "male" });
      // Second row should be empty with a new ID.
      expect(newRows[1].paramName).toBe("");
      expect(newRows[1].value).toBe("");
    });
  });

  describe("remove row", () => {
    it("calls onChange with the row removed when remove button is clicked", async () => {
      const user = userEvent.setup();
      const rows: SearchParamRowData[] = [
        { id: 1, paramName: "gender", value: "male" },
        { id: 2, paramName: "birthdate", value: "2000-01-01" },
      ];

      render(
        <SearchParamsInput
          availableParams={mockAvailableParams}
          rows={rows}
          onChange={mockOnChange}
        />,
      );

      // Find the icon-only buttons (remove buttons are icon buttons with no text).
      const allButtons = screen.getAllByRole("button");
      const enabledRemoveButtons = allButtons.filter(
        (btn) =>
          btn.querySelector("svg") !== null && !btn.textContent && !btn.hasAttribute("disabled"),
      );
      // Click the first enabled remove button to remove the first row.
      expect(enabledRemoveButtons.length).toBeGreaterThan(0);
      await user.click(enabledRemoveButtons[0]);

      expect(mockOnChange).toHaveBeenCalledTimes(1);
      const newRows = mockOnChange.mock.calls[0][0] as SearchParamRowData[];
      expect(newRows).toHaveLength(1);
      expect(newRows[0].id).toBe(2);
    });

    it("disables the remove button when there is only one row", () => {
      const rows: SearchParamRowData[] = [{ id: 1, paramName: "", value: "" }];

      render(
        <SearchParamsInput
          availableParams={mockAvailableParams}
          rows={rows}
          onChange={mockOnChange}
        />,
      );

      // Find icon-only buttons (remove buttons).
      const allButtons = screen.getAllByRole("button");
      const removeButtons = allButtons.filter(
        (btn) => btn.querySelector("svg") !== null && !btn.textContent,
      );

      expect(removeButtons).toHaveLength(1);
      expect(removeButtons[0]).toBeDisabled();
    });
  });

  describe("parameter name change", () => {
    it("calls onChange with the updated parameter name when a selection is made", async () => {
      const user = userEvent.setup();
      const rows: SearchParamRowData[] = [{ id: 1, paramName: "", value: "" }];

      render(
        <SearchParamsInput
          availableParams={mockAvailableParams}
          rows={rows}
          onChange={mockOnChange}
        />,
      );

      const dropdown = screen.getByRole("combobox");
      await user.click(dropdown);
      await user.click(screen.getByRole("option", { name: /gender/i }));

      expect(mockOnChange).toHaveBeenCalledTimes(1);
      const newRows = mockOnChange.mock.calls[0][0] as SearchParamRowData[];
      expect(newRows[0].paramName).toBe("gender");
      expect(newRows[0].value).toBe("");
    });
  });

  describe("value change", () => {
    it("calls onChange with the updated value when the user types", async () => {
      const user = userEvent.setup();
      const rows: SearchParamRowData[] = [{ id: 1, paramName: "gender", value: "" }];

      render(
        <SearchParamsInput
          availableParams={mockAvailableParams}
          rows={rows}
          onChange={mockOnChange}
        />,
      );

      const valueInput = screen.getByPlaceholderText(/e\.g\., male/i);
      await user.type(valueInput, "m");

      // onChange should have been called for the first character typed.
      expect(mockOnChange).toHaveBeenCalled();
      const firstCall = mockOnChange.mock.calls[0][0] as SearchParamRowData[];
      expect(firstCall[0].value).toBe("m");
    });
  });

  describe("onKeyDown", () => {
    it("forwards keyboard events from value inputs to the onKeyDown handler", async () => {
      const user = userEvent.setup();
      const mockOnKeyDown = vi.fn();
      const rows: SearchParamRowData[] = [{ id: 1, paramName: "", value: "" }];

      render(
        <SearchParamsInput
          availableParams={mockAvailableParams}
          rows={rows}
          onChange={mockOnChange}
          onKeyDown={mockOnKeyDown}
        />,
      );

      const valueInput = screen.getByPlaceholderText(/e\.g\., male/i);
      await user.type(valueInput, "{Enter}");

      expect(mockOnKeyDown).toHaveBeenCalled();
    });
  });
});

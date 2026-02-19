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
 * Tests for the ExportOptions component which provides reusable form fields
 * for configuring bulk export parameters.
 *
 * These tests verify that the component correctly renders all option fields,
 * handles user input, and notifies parent components of changes through the
 * onChange callback.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { fireEvent, render, screen } from "../../../test/testUtils";
import { DEFAULT_EXPORT_OPTIONS } from "../../../types/exportOptions";
import { ExportOptions } from "../ExportOptions";

import type { ExportOptionsValues } from "../../../types/exportOptions";

describe("ExportOptions", () => {
  const defaultResourceTypes = ["Patient", "Observation", "Condition"];
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("initial render", () => {
    it("renders the resource types picker", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      expect(screen.getByText(/resource types/i)).toBeInTheDocument();
    });

    it("renders since and until date fields", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      expect(screen.getByText(/^since$/i)).toBeInTheDocument();
      expect(screen.getByText(/^until$/i)).toBeInTheDocument();
    });

    it("renders the elements field", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      expect(screen.getByPlaceholderText(/id,meta,name/i)).toBeInTheDocument();
    });

    it("renders the output format field by default", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      // There may be multiple elements with "output format" text (label and help text).
      const elements = screen.getAllByText(/output format/i);
      expect(elements.length).toBeGreaterThan(0);
    });

    it("hides the output format field when hideOutputFormat is true", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
          hideOutputFormat
        />,
      );

      expect(screen.queryByText(/output format/i)).not.toBeInTheDocument();
    });

    it("renders the type filters section with add button", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      expect(screen.getByText("Type filters")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /add type filter/i })).toBeInTheDocument();
    });

    it("shows empty state text when no type filters are configured", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      expect(screen.getByText(/no type filters configured/i)).toBeInTheDocument();
    });

    it("does not render include associated data field", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      expect(screen.queryByText(/include associated data/i)).not.toBeInTheDocument();
    });
  });

  describe("user interactions", () => {
    it("calls onChange when elements value changes", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      const elementsInput = screen.getByPlaceholderText(/id,meta,name/i);
      fireEvent.change(elementsInput, { target: { value: "id,name" } });

      expect(mockOnChange).toHaveBeenCalledTimes(1);
      expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ elements: "id,name" }));
    });

    it("calls onChange when output format is selected", async () => {
      const user = userEvent.setup();
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      // Find and click the output format select trigger.
      const formatTrigger = screen.getByRole("combobox", { name: "" });
      await user.click(formatTrigger);

      // Select NDJSON format.
      const ndjsonOption = screen.getByRole("option", { name: /ndjson/i });
      await user.click(ndjsonOption);

      expect(mockOnChange).toHaveBeenCalled();
      const lastCall = mockOnChange.mock.calls[mockOnChange.mock.calls.length - 1][0];
      expect(lastCall.outputFormat).toBe("application/fhir+ndjson");
    });
  });

  describe("value display", () => {
    it("displays current elements value", () => {
      const values: ExportOptionsValues = {
        ...DEFAULT_EXPORT_OPTIONS,
        elements: "id,name,birthDate",
      };

      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={values}
          onChange={mockOnChange}
        />,
      );

      const elementsInput = screen.getByPlaceholderText(/id,meta,name/i) as HTMLInputElement;
      expect(elementsInput.value).toBe("id,name,birthDate");
    });
  });

  describe("help text", () => {
    it("displays since field help text", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      expect(screen.getByText(/only resources updated after this time/i)).toBeInTheDocument();
    });

    it("displays until field help text", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      expect(screen.getByText(/only resources updated before this time/i)).toBeInTheDocument();
    });

    it("displays elements field help text", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      expect(screen.getByText(/comma-separated list of element names/i)).toBeInTheDocument();
    });

    it("displays output format help text", () => {
      render(
        <ExportOptions
          resourceTypes={defaultResourceTypes}
          values={DEFAULT_EXPORT_OPTIONS}
          onChange={mockOnChange}
        />,
      );

      expect(screen.getByText(/output format for the export data/i)).toBeInTheDocument();
    });
  });
});

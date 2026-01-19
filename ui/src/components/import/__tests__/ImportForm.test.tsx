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
 * Tests for the ImportForm component which handles bulk import configuration.
 *
 * These tests verify that the form correctly renders input format options,
 * manages multiple input file entries, validates form data before submission,
 * and handles disabled and submitting states appropriately.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";

import type { ImportRequest } from "../../../types/import";
import { ImportForm } from "../ImportForm";

describe("ImportForm", () => {
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
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByRole("heading", { name: /new import/i })).toBeInTheDocument();
    });

    it("renders the input format selector", () => {
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByText(/input format/i)).toBeInTheDocument();
    });

    it("renders the input files section", () => {
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByText(/input files/i)).toBeInTheDocument();
    });

    it("renders the save mode field", () => {
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByText(/save mode/i)).toBeInTheDocument();
    });

    it("renders the start import button", () => {
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByRole("button", { name: /start import/i })).toBeInTheDocument();
    });

    it("renders the add input button", () => {
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByRole("button", { name: /add input/i })).toBeInTheDocument();
    });

    it("renders one input row by default", () => {
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Should have one URL input.
      const urlInputs = screen.getAllByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      expect(urlInputs).toHaveLength(1);
    });
  });

  describe("input management", () => {
    it("adds a new input row when add input button is clicked", async () => {
      const user = userEvent.setup();
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const addButton = screen.getByRole("button", { name: /add input/i });
      await user.click(addButton);

      const urlInputs = screen.getAllByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      expect(urlInputs).toHaveLength(2);
    });

    it("removes an input row when remove button is clicked", async () => {
      const user = userEvent.setup();
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Add a second row first.
      const addButton = screen.getByRole("button", { name: /add input/i });
      await user.click(addButton);

      // Now we should have two rows.
      let urlInputs = screen.getAllByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      expect(urlInputs).toHaveLength(2);

      // Find and click the first remove button (they are enabled when there's more than one row).
      const removeButtons = screen.getAllByRole("button").filter((btn) => {
        return btn.querySelector("svg") !== null && !btn.textContent?.includes("Add");
      });
      // Click the first remove button that is not disabled.
      const enabledRemoveButton = removeButtons.find((btn) => !btn.hasAttribute("disabled"));
      if (enabledRemoveButton) {
        await user.click(enabledRemoveButton);
      }

      urlInputs = screen.getAllByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      expect(urlInputs).toHaveLength(1);
    });

    it("disables the remove button when there is only one input row", () => {
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Find the remove button (it should be disabled).
      const allButtons = screen.getAllByRole("button");
      const removeButton = allButtons.find(
        (btn) => btn.querySelector("svg") !== null && !btn.textContent,
      );

      expect(removeButton).toBeDisabled();
    });

    it("allows changing the resource type for an input", async () => {
      const user = userEvent.setup();
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // The resource type selector is a combobox. Find the first one (for the input row).
      const comboboxes = screen.getAllByRole("combobox");
      // First combobox is for input format, second is for resource type.
      const resourceTypeCombobox = comboboxes[1];

      await user.click(resourceTypeCombobox);
      const observationOption = screen.getByRole("option", { name: "Observation" });
      await user.click(observationOption);

      // Submit and verify the resource type was changed.
      const urlInput = screen.getByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      await user.type(urlInput, "s3a://bucket/Observation.ndjson");
      await user.click(screen.getByRole("button", { name: /start import/i }));

      const request = mockOnSubmit.mock.calls[0][0] as ImportRequest;
      expect(request.input[0].type).toBe("Observation");
    });
  });

  describe("form submission", () => {
    it("submits the form with correct data", async () => {
      const user = userEvent.setup();
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Enter a URL.
      const urlInput = screen.getByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      await user.type(urlInput, "s3a://my-bucket/Patient.ndjson");

      await user.click(screen.getByRole("button", { name: /start import/i }));

      expect(mockOnSubmit).toHaveBeenCalledTimes(1);
      const request = mockOnSubmit.mock.calls[0][0] as ImportRequest;
      expect(request.inputFormat).toBe("application/fhir+ndjson");
      expect(request.saveMode).toBe("overwrite");
      expect(request.input).toHaveLength(1);
      expect(request.input[0].type).toBe("Patient");
      expect(request.input[0].url).toBe("s3a://my-bucket/Patient.ndjson");
    });

    it("filters out empty URL inputs on submission", async () => {
      const user = userEvent.setup();
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Add a second row.
      await user.click(screen.getByRole("button", { name: /add input/i }));

      // Only fill in the first row.
      const urlInputs = screen.getAllByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      await user.type(urlInputs[0], "s3a://my-bucket/Patient.ndjson");

      await user.click(screen.getByRole("button", { name: /start import/i }));

      const request = mockOnSubmit.mock.calls[0][0] as ImportRequest;
      expect(request.input).toHaveLength(1);
    });

    it("submits multiple input files", async () => {
      const user = userEvent.setup();
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Add a second row.
      await user.click(screen.getByRole("button", { name: /add input/i }));

      // Fill in both rows.
      const urlInputs = screen.getAllByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      await user.type(urlInputs[0], "s3a://my-bucket/Patient.ndjson");
      await user.type(urlInputs[1], "s3a://my-bucket/Observation.ndjson");

      await user.click(screen.getByRole("button", { name: /start import/i }));

      const request = mockOnSubmit.mock.calls[0][0] as ImportRequest;
      expect(request.input).toHaveLength(2);
    });
  });

  describe("button disabled states", () => {
    it("disables the submit button when no URL is entered", () => {
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const submitButton = screen.getByRole("button", { name: /start import/i });
      expect(submitButton).toBeDisabled();
    });

    it("enables the submit button when at least one URL is entered", async () => {
      const user = userEvent.setup();
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const urlInput = screen.getByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      await user.type(urlInput, "s3a://my-bucket/Patient.ndjson");

      const submitButton = screen.getByRole("button", { name: /start import/i });
      expect(submitButton).not.toBeDisabled();
    });

    it("disables the submit button when isSubmitting is true", async () => {
      const user = userEvent.setup();
      const { rerender } = render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const urlInput = screen.getByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      await user.type(urlInput, "s3a://my-bucket/Patient.ndjson");

      // Re-render with isSubmitting=true.
      rerender(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={true}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const submitButton = screen.getByRole("button", { name: /starting import/i });
      expect(submitButton).toBeDisabled();
    });

    it("disables the submit button when disabled is true", async () => {
      const user = userEvent.setup();
      const { rerender } = render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const urlInput = screen.getByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      await user.type(urlInput, "s3a://my-bucket/Patient.ndjson");

      // Re-render with disabled=true.
      rerender(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={true}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const submitButton = screen.getByRole("button", { name: /start import/i });
      expect(submitButton).toBeDisabled();
    });

    it("shows starting import text when isSubmitting is true", () => {
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={true}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByRole("button", { name: /starting import/i })).toBeInTheDocument();
    });
  });

  describe("input format selection", () => {
    it("defaults to NDJSON format", () => {
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // The format trigger should show NDJSON as selected.
      expect(screen.getByText("NDJSON")).toBeInTheDocument();
    });

    it("allows selecting Parquet format", async () => {
      const user = userEvent.setup();
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Find and click the format selector (first combobox).
      const comboboxes = screen.getAllByRole("combobox");
      const formatCombobox = comboboxes[0];
      await user.click(formatCombobox);

      const parquetOption = screen.getByRole("option", { name: "Parquet" });
      await user.click(parquetOption);

      // Enter a URL and submit.
      const urlInput = screen.getByPlaceholderText(/s3a:\/\/bucket\/Patient.ndjson/i);
      await user.type(urlInput, "s3a://my-bucket/Patient.parquet");
      await user.click(screen.getByRole("button", { name: /start import/i }));

      const request = mockOnSubmit.mock.calls[0][0] as ImportRequest;
      expect(request.inputFormat).toBe("application/vnd.apache.parquet");
    });
  });

  describe("help text", () => {
    it("displays URL scheme help text", () => {
      render(
        <ImportForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(
        screen.getByText(/supported url schemes: s3a:\/\/, hdfs:\/\/, file:\/\//i),
      ).toBeInTheDocument();
    });
  });
});

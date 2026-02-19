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
 * Tests for the ImportPnpForm component which handles ping-and-pull import
 * configuration.
 *
 * These tests verify that the form correctly renders all required fields,
 * handles export URL input, manages collapsible export options, validates
 * form data before submission, and handles disabled and submitting states
 * appropriately.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { ImportPnpForm } from "../ImportPnpForm";

import type { ImportPnpRequest } from "../../../types/importPnp";

describe("ImportPnpForm", () => {
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
        <ImportPnpForm
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
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByText(/input format/i)).toBeInTheDocument();
    });

    it("renders the export URL field", () => {
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByText(/export url/i)).toBeInTheDocument();
      expect(
        screen.getByPlaceholderText(/https:\/\/example.org\/fhir\/\$export/i),
      ).toBeInTheDocument();
    });

    it("renders the save mode field", () => {
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByText(/save mode/i)).toBeInTheDocument();
    });

    it("renders the collapsible export options section", () => {
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByText(/export options/i)).toBeInTheDocument();
    });

    it("renders the start import button", () => {
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByRole("button", { name: /start import/i })).toBeInTheDocument();
    });
  });

  describe("export options collapsible", () => {
    it("does not show export options content by default", () => {
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // The resource types label from ExportOptions should not be visible when collapsed.
      expect(screen.queryByText(/resource types/i)).not.toBeInTheDocument();
    });

    it("expands export options when clicked", async () => {
      const user = userEvent.setup();
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Click the export options trigger.
      const trigger = screen.getByText(/export options/i);
      await user.click(trigger);

      // Now the resource types should be visible.
      expect(screen.getByText(/resource types/i)).toBeInTheDocument();
    });

    it("hides output format in export options", async () => {
      const user = userEvent.setup();
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Click the export options trigger.
      const trigger = screen.getByText(/export options/i);
      await user.click(trigger);

      // Output format should not be visible in export options (it's at the top level).
      // Count occurrences - should only be "Input format" at the top, not "Output format"
      // in the export options.
      const outputFormatLabels = screen.queryAllByText(/output format/i);
      expect(outputFormatLabels).toHaveLength(0);
    });
  });

  describe("form submission", () => {
    it("submits the form with correct data", async () => {
      const user = userEvent.setup();
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Enter export URL.
      const urlInput = screen.getByPlaceholderText(/https:\/\/example.org\/fhir\/\$export/i);
      await user.type(urlInput, "https://server.example.org/fhir/$export");

      await user.click(screen.getByRole("button", { name: /start import/i }));

      expect(mockOnSubmit).toHaveBeenCalledTimes(1);
      const request = mockOnSubmit.mock.calls[0][0] as ImportPnpRequest;
      expect(request.exportUrl).toBe("https://server.example.org/fhir/$export");
      expect(request.exportType).toBe("dynamic");
      expect(request.saveMode).toBe("overwrite");
      expect(request.inputFormat).toBe("application/fhir+ndjson");
    });

    it("includes selected resource types in the request", async () => {
      const user = userEvent.setup();
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Enter export URL.
      const urlInput = screen.getByPlaceholderText(/https:\/\/example.org\/fhir\/\$export/i);
      await user.type(urlInput, "https://server.example.org/fhir/$export");

      // Expand export options.
      const trigger = screen.getByText(/export options/i);
      await user.click(trigger);

      // Select some resource types.
      const patientCheckbox = screen.getByRole("checkbox", { name: "Patient" });
      await user.click(patientCheckbox);

      await user.click(screen.getByRole("button", { name: /start import/i }));

      const request = mockOnSubmit.mock.calls[0][0] as ImportPnpRequest;
      expect(request.types).toContain("Patient");
    });
  });

  describe("button disabled states", () => {
    it("disables the submit button when no export URL is entered", () => {
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const submitButton = screen.getByRole("button", { name: /start import/i });
      expect(submitButton).toBeDisabled();
    });

    it("enables the submit button when export URL is entered", async () => {
      const user = userEvent.setup();
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const urlInput = screen.getByPlaceholderText(/https:\/\/example.org\/fhir\/\$export/i);
      await user.type(urlInput, "https://server.example.org/fhir/$export");

      const submitButton = screen.getByRole("button", { name: /start import/i });
      expect(submitButton).not.toBeDisabled();
    });

    it("disables the submit button when isSubmitting is true", async () => {
      const user = userEvent.setup();
      const { rerender } = render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const urlInput = screen.getByPlaceholderText(/https:\/\/example.org\/fhir\/\$export/i);
      await user.type(urlInput, "https://server.example.org/fhir/$export");

      // Re-render with isSubmitting=true.
      rerender(
        <ImportPnpForm
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
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      const urlInput = screen.getByPlaceholderText(/https:\/\/example.org\/fhir\/\$export/i);
      await user.type(urlInput, "https://server.example.org/fhir/$export");

      // Re-render with disabled=true.
      rerender(
        <ImportPnpForm
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
        <ImportPnpForm
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
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(screen.getByText("NDJSON")).toBeInTheDocument();
    });

    it("allows selecting Parquet format", async () => {
      const user = userEvent.setup();
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      // Find and click the format selector.
      const formatCombobox = screen.getByRole("combobox");
      await user.click(formatCombobox);

      const parquetOption = screen.getByRole("option", { name: "Parquet" });
      await user.click(parquetOption);

      // Enter export URL and submit.
      const urlInput = screen.getByPlaceholderText(/https:\/\/example.org\/fhir\/\$export/i);
      await user.type(urlInput, "https://server.example.org/fhir/$export");
      await user.click(screen.getByRole("button", { name: /start import/i }));

      const request = mockOnSubmit.mock.calls[0][0] as ImportPnpRequest;
      expect(request.inputFormat).toBe("application/vnd.apache.parquet");
    });
  });

  describe("help text", () => {
    it("displays export URL help text", () => {
      render(
        <ImportPnpForm
          onSubmit={mockOnSubmit}
          isSubmitting={false}
          disabled={false}
          resourceTypes={defaultResourceTypes}
        />,
      );

      expect(
        screen.getByText(/the bulk export endpoint url of the remote fhir server/i),
      ).toBeInTheDocument();
    });
  });
});

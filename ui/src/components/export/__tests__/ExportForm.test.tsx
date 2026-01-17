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
 * Tests for the ExportForm component which handles bulk export configuration.
 *
 * These tests verify that the form correctly renders export level options,
 * shows conditional fields based on the selected level, and submits the
 * correct request payload when the user initiates an export.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { ExportForm } from "../ExportForm";

import type { ExportRequest } from "../../../types/export";

describe("ExportForm", () => {
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
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      expect(screen.getByRole("heading", { name: /new export/i })).toBeInTheDocument();
    });

    it("renders the export level selector with system as default", () => {
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // The select trigger should show "All data in system" as the default selection.
      expect(screen.getByText(/all data in system/i)).toBeInTheDocument();
    });

    it("renders the start export button", () => {
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      expect(screen.getByRole("button", { name: /start export/i })).toBeInTheDocument();
    });

    it("does not show Patient ID field when system level is selected", () => {
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      expect(screen.queryByLabelText(/patient id/i)).not.toBeInTheDocument();
    });

    it("does not show Group ID field when system level is selected", () => {
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      expect(screen.queryByLabelText(/group id/i)).not.toBeInTheDocument();
    });
  });

  describe("export level selection", () => {
    it("shows Patient ID field when patient level is selected", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // Open the export level select dropdown (first combobox) and choose patient level.
      const comboboxes = screen.getAllByRole("combobox");
      const exportLevelTrigger = comboboxes[0];
      await user.click(exportLevelTrigger);
      const option = screen.getByRole("option", { name: /data for single patient/i });
      await user.click(option);

      // The Patient ID field should now be visible.
      expect(screen.getByPlaceholderText(/patient-123/i)).toBeInTheDocument();
    });

    it("shows Group ID field when group level is selected", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // Open the export level select dropdown (first combobox) and choose group level.
      const comboboxes = screen.getAllByRole("combobox");
      const exportLevelTrigger = comboboxes[0];
      await user.click(exportLevelTrigger);
      const option = screen.getByRole("option", { name: /data for patients in group/i });
      await user.click(option);

      // The Group ID field should now be visible.
      expect(screen.getByPlaceholderText(/group-456/i)).toBeInTheDocument();
    });

    it("hides Patient ID field when switching from patient to system level", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // Switch to patient level using the export level combobox (first one).
      const comboboxes = screen.getAllByRole("combobox");
      const exportLevelTrigger = comboboxes[0];
      await user.click(exportLevelTrigger);
      await user.click(screen.getByRole("option", { name: /data for single patient/i }));

      expect(screen.getByPlaceholderText(/patient-123/i)).toBeInTheDocument();

      // Switch back to system level.
      await user.click(exportLevelTrigger);
      await user.click(screen.getByRole("option", { name: /all data in system/i }));

      expect(screen.queryByPlaceholderText(/patient-123/i)).not.toBeInTheDocument();
    });
  });

  describe("form submission", () => {
    it("submits system level export with default options", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      await user.click(screen.getByRole("button", { name: /start export/i }));

      expect(mockOnSubmit).toHaveBeenCalledTimes(1);
      const request = mockOnSubmit.mock.calls[0][0] as ExportRequest;
      expect(request.level).toBe("system");
      expect(request.resourceTypes).toBeUndefined();
      expect(request.patientId).toBeUndefined();
      expect(request.groupId).toBeUndefined();
    });

    it("submits patient level export with patient ID", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // Select patient level using the export level combobox (first one).
      const comboboxes = screen.getAllByRole("combobox");
      const exportLevelTrigger = comboboxes[0];
      await user.click(exportLevelTrigger);
      await user.click(screen.getByRole("option", { name: /data for single patient/i }));

      // Enter patient ID.
      const patientInput = screen.getByPlaceholderText(/patient-123/i);
      await user.type(patientInput, "my-patient-id");

      await user.click(screen.getByRole("button", { name: /start export/i }));

      expect(mockOnSubmit).toHaveBeenCalledTimes(1);
      const request = mockOnSubmit.mock.calls[0][0] as ExportRequest;
      expect(request.level).toBe("patient");
      expect(request.patientId).toBe("my-patient-id");
      expect(request.groupId).toBeUndefined();
    });

    it("submits group level export with group ID", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // Select group level using the export level combobox (first one).
      const comboboxes = screen.getAllByRole("combobox");
      const exportLevelTrigger = comboboxes[0];
      await user.click(exportLevelTrigger);
      await user.click(screen.getByRole("option", { name: /data for patients in group/i }));

      // Enter group ID.
      const groupInput = screen.getByPlaceholderText(/group-456/i);
      await user.type(groupInput, "my-group-id");

      await user.click(screen.getByRole("button", { name: /start export/i }));

      expect(mockOnSubmit).toHaveBeenCalledTimes(1);
      const request = mockOnSubmit.mock.calls[0][0] as ExportRequest;
      expect(request.level).toBe("group");
      expect(request.groupId).toBe("my-group-id");
      expect(request.patientId).toBeUndefined();
    });

    it("includes selected resource types in the request", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // Select some resource types from the ResourceTypePicker.
      const patientCheckbox = screen.getByRole("checkbox", { name: /patient/i });
      const observationCheckbox = screen.getByRole("checkbox", { name: /observation/i });
      await user.click(patientCheckbox);
      await user.click(observationCheckbox);

      await user.click(screen.getByRole("button", { name: /start export/i }));

      expect(mockOnSubmit).toHaveBeenCalledTimes(1);
      const request = mockOnSubmit.mock.calls[0][0] as ExportRequest;
      expect(request.resourceTypes).toContain("Patient");
      expect(request.resourceTypes).toContain("Observation");
    });

    it("includes elements when specified", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // Enter elements.
      const elementsInput = screen.getByPlaceholderText(/id,meta,name/i);
      await user.type(elementsInput, "id,name,birthDate");

      await user.click(screen.getByRole("button", { name: /start export/i }));

      expect(mockOnSubmit).toHaveBeenCalledTimes(1);
      const request = mockOnSubmit.mock.calls[0][0] as ExportRequest;
      expect(request.elements).toBe("id,name,birthDate");
    });
  });

  describe("button disabled states", () => {
    it("disables the submit button when patient level is selected but no patient ID is entered", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // Select patient level without entering an ID using the export level combobox (first one).
      const comboboxes = screen.getAllByRole("combobox");
      const exportLevelTrigger = comboboxes[0];
      await user.click(exportLevelTrigger);
      await user.click(screen.getByRole("option", { name: /data for single patient/i }));

      const submitButton = screen.getByRole("button", { name: /start export/i });
      expect(submitButton).toBeDisabled();
    });

    it("enables the submit button when patient level is selected and patient ID is entered", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // Select patient level using the export level combobox (first one).
      const comboboxes = screen.getAllByRole("combobox");
      const exportLevelTrigger = comboboxes[0];
      await user.click(exportLevelTrigger);
      await user.click(screen.getByRole("option", { name: /data for single patient/i }));

      // Enter patient ID.
      const patientInput = screen.getByPlaceholderText(/patient-123/i);
      await user.type(patientInput, "test-patient");

      const submitButton = screen.getByRole("button", { name: /start export/i });
      expect(submitButton).not.toBeDisabled();
    });

    it("disables the submit button when group level is selected but no group ID is entered", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // Select group level without entering an ID using the export level combobox (first one).
      const comboboxes = screen.getAllByRole("combobox");
      const exportLevelTrigger = comboboxes[0];
      await user.click(exportLevelTrigger);
      await user.click(screen.getByRole("option", { name: /data for patients in group/i }));

      const submitButton = screen.getByRole("button", { name: /start export/i });
      expect(submitButton).toBeDisabled();
    });

    it("enables the submit button when group level is selected and group ID is entered", async () => {
      const user = userEvent.setup();
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      // Select group level using the export level combobox (first one).
      const comboboxes = screen.getAllByRole("combobox");
      const exportLevelTrigger = comboboxes[0];
      await user.click(exportLevelTrigger);
      await user.click(screen.getByRole("option", { name: /data for patients in group/i }));

      // Enter group ID.
      const groupInput = screen.getByPlaceholderText(/group-456/i);
      await user.type(groupInput, "test-group");

      const submitButton = screen.getByRole("button", { name: /start export/i });
      expect(submitButton).not.toBeDisabled();
    });

    it("does not disable the submit button when system level is selected", () => {
      render(<ExportForm onSubmit={mockOnSubmit} resourceTypes={defaultResourceTypes} />);

      const submitButton = screen.getByRole("button", { name: /start export/i });
      expect(submitButton).not.toBeDisabled();
    });
  });
});

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
 * Tests for the SqlOnFhirForm component which handles ViewDefinition execution
 * and saving.
 *
 * These tests verify that the form correctly renders tabs for stored and custom
 * ViewDefinitions, handles selection of stored ViewDefinitions, validates JSON
 * input for custom ViewDefinitions, executes ViewDefinitions, saves custom
 * ViewDefinitions to the server, and handles error states appropriately.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { fireEvent, render, screen, waitFor } from "../../../test/testUtils";
import { SqlOnFhirForm } from "../SqlOnFhirForm";

import type { ViewRunRequest } from "../../../hooks";
import type { CreateViewDefinitionResult } from "../../../types/sqlOnFhir";

// Mock the hooks.
const mockUseViewDefinitions = vi.fn();
const mockCopyToClipboard = vi.fn();

vi.mock("../../../hooks/useViewDefinitions", () => ({
  useViewDefinitions: () => mockUseViewDefinitions(),
}));

vi.mock("../../../hooks", () => ({
  useClipboard: () => mockCopyToClipboard,
}));

describe("SqlOnFhirForm", () => {
  const mockOnExecute = vi.fn();
  const mockOnSaveToServer = vi.fn();

  const sampleViewDefinitions = [
    {
      id: "vd-1",
      name: "Patient View",
      json: JSON.stringify({ resourceType: "ViewDefinition", name: "Patient View" }, null, 2),
    },
    {
      id: "vd-2",
      name: "Observation View",
      json: JSON.stringify({ resourceType: "ViewDefinition", name: "Observation View" }, null, 2),
    },
  ];

  beforeEach(() => {
    vi.clearAllMocks();
    mockUseViewDefinitions.mockReturnValue({
      data: sampleViewDefinitions,
      isLoading: false,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("initial render", () => {
    it("renders the form heading", () => {
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      expect(screen.getByRole("heading", { name: /sql on fhir/i })).toBeInTheDocument();
    });

    it("renders tabs for stored and custom ViewDefinitions", () => {
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      expect(screen.getByRole("tab", { name: /select view definition/i })).toBeInTheDocument();
      expect(screen.getByRole("tab", { name: /provide json/i })).toBeInTheDocument();
    });

    it("renders the execute button", () => {
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      expect(screen.getByRole("button", { name: /^execute$/i })).toBeInTheDocument();
    });

    it("defaults to stored tab", () => {
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      const storedTab = screen.getByRole("tab", { name: /select view definition/i });
      expect(storedTab).toHaveAttribute("data-state", "active");
    });
  });

  describe("stored view definitions tab", () => {
    it("shows loading state while view definitions are loading", () => {
      mockUseViewDefinitions.mockReturnValue({
        data: undefined,
        isLoading: true,
      });

      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      expect(screen.getByText(/loading view definitions/i)).toBeInTheDocument();
    });

    it("shows message when no view definitions are found", () => {
      mockUseViewDefinitions.mockReturnValue({
        data: [],
        isLoading: false,
      });

      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      expect(screen.getByText(/no view definitions found on the server/i)).toBeInTheDocument();
    });

    it("renders view definition selector when definitions exist", () => {
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      expect(screen.getByRole("combobox")).toBeInTheDocument();
    });

    it("shows view definition options in the selector", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);

      expect(screen.getByRole("option", { name: "Patient View" })).toBeInTheDocument();
      expect(screen.getByRole("option", { name: "Observation View" })).toBeInTheDocument();
    });

    it("shows selection hint when no view definition is selected", () => {
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      expect(
        screen.getByText(/select a view definition that has been loaded/i),
      ).toBeInTheDocument();
    });

    it("shows JSON preview when a view definition is selected", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "Patient View" }));

      // The JSON should be shown in a textarea.
      const textarea = screen.getByRole("textbox") as HTMLTextAreaElement;
      expect(textarea.value).toContain('"resourceType": "ViewDefinition"');
    });

    it("shows copy button when a view definition is selected", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "Patient View" }));

      expect(screen.getByRole("button", { name: /copy to clipboard/i })).toBeInTheDocument();
    });

    it("copies JSON to clipboard when copy button is clicked", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "Patient View" }));

      const copyButton = screen.getByRole("button", { name: /copy to clipboard/i });
      await user.click(copyButton);

      expect(mockCopyToClipboard).toHaveBeenCalledWith(
        expect.stringContaining('"resourceType": "ViewDefinition"'),
      );
    });
  });

  describe("custom JSON tab", () => {
    it("switches to custom tab when clicked", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      const customTab = screen.getByRole("tab", { name: /provide json/i });
      await user.click(customTab);

      expect(customTab).toHaveAttribute("data-state", "active");
    });

    it("renders JSON textarea in custom tab", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      expect(screen.getByRole("textbox")).toBeInTheDocument();
    });

    it("shows save to server button in custom tab", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      expect(screen.getByRole("button", { name: /save to server/i })).toBeInTheDocument();
    });

    it("does not show save to server button in stored tab", () => {
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      expect(screen.queryByRole("button", { name: /save to server/i })).not.toBeInTheDocument();
    });

    it("allows entering custom JSON", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      const textarea = screen.getByRole("textbox");
      const customJson = '{"resourceType": "ViewDefinition", "name": "test"}';
      fireEvent.change(textarea, { target: { value: customJson } });

      expect(textarea).toHaveValue(customJson);
    });
  });

  describe("form execution", () => {
    it("executes stored ViewDefinition when execute is clicked", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      // Select a view definition.
      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "Patient View" }));

      // Click execute.
      await user.click(screen.getByRole("button", { name: /^execute$/i }));

      expect(mockOnExecute).toHaveBeenCalledTimes(1);
      const request = mockOnExecute.mock.calls[0][0] as ViewRunRequest;
      expect(request.mode).toBe("stored");
      expect(request.viewDefinitionId).toBe("vd-1");
    });

    it("executes inline ViewDefinition when execute is clicked in custom tab", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      // Switch to custom tab.
      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      // Enter custom JSON.
      const textarea = screen.getByRole("textbox");
      const customJson = '{"resourceType": "ViewDefinition", "name": "test"}';
      fireEvent.change(textarea, { target: { value: customJson } });

      // Click execute.
      await user.click(screen.getByRole("button", { name: /^execute$/i }));

      expect(mockOnExecute).toHaveBeenCalledTimes(1);
      const request = mockOnExecute.mock.calls[0][0] as ViewRunRequest;
      expect(request.mode).toBe("inline");
      expect(request.viewDefinitionJson).toBe(customJson);
    });
  });

  describe("save to server", () => {
    it("saves custom ViewDefinition to server when save button is clicked", async () => {
      const user = userEvent.setup();
      const saveResult: CreateViewDefinitionResult = { id: "new-vd", name: "New View" };
      mockOnSaveToServer.mockResolvedValue(saveResult);

      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      // Switch to custom tab.
      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      // Enter custom JSON.
      const textarea = screen.getByRole("textbox");
      const customJson = '{"resourceType": "ViewDefinition", "name": "New View"}';
      fireEvent.change(textarea, { target: { value: customJson } });

      // Click save.
      await user.click(screen.getByRole("button", { name: /save to server/i }));

      await waitFor(() => {
        expect(mockOnSaveToServer).toHaveBeenCalledWith(customJson);
      });
    });

    it("shows error message when save fails", async () => {
      const user = userEvent.setup();
      mockOnSaveToServer.mockRejectedValue(new Error("Save failed"));

      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      // Switch to custom tab.
      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      // Enter custom JSON.
      const textarea = screen.getByRole("textbox");
      fireEvent.change(textarea, { target: { value: '{"resourceType": "ViewDefinition"}' } });

      // Click save.
      await user.click(screen.getByRole("button", { name: /save to server/i }));

      await waitFor(() => {
        expect(screen.getByText(/save failed/i)).toBeInTheDocument();
      });
    });

    it("switches to stored tab and selects new ViewDefinition after successful save", async () => {
      const user = userEvent.setup();
      const saveResult: CreateViewDefinitionResult = { id: "new-vd", name: "New View" };
      mockOnSaveToServer.mockResolvedValue(saveResult);

      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      // Switch to custom tab.
      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      // Enter custom JSON.
      const textarea = screen.getByRole("textbox");
      fireEvent.change(textarea, {
        target: { value: '{"resourceType": "ViewDefinition", "name": "New View"}' },
      });

      // Click save.
      await user.click(screen.getByRole("button", { name: /save to server/i }));

      // After save, should switch to stored tab.
      await waitFor(() => {
        const storedTab = screen.getByRole("tab", { name: /select view definition/i });
        expect(storedTab).toHaveAttribute("data-state", "active");
      });
    });
  });

  describe("button disabled states", () => {
    it("disables execute button when no stored ViewDefinition is selected", () => {
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      const executeButton = screen.getByRole("button", { name: /^execute$/i });
      expect(executeButton).toBeDisabled();
    });

    it("enables execute button when a stored ViewDefinition is selected", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "Patient View" }));

      const executeButton = screen.getByRole("button", { name: /^execute$/i });
      expect(executeButton).not.toBeDisabled();
    });

    it("disables execute button when no custom JSON is entered", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      const executeButton = screen.getByRole("button", { name: /^execute$/i });
      expect(executeButton).toBeDisabled();
    });

    it("enables execute button when custom JSON is entered", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      const textarea = screen.getByRole("textbox");
      fireEvent.change(textarea, { target: { value: '{"resourceType": "ViewDefinition"}' } });

      const executeButton = screen.getByRole("button", { name: /^execute$/i });
      expect(executeButton).not.toBeDisabled();
    });

    it("disables execute button when isExecuting is true", async () => {
      const user = userEvent.setup();
      const { rerender } = render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "Patient View" }));

      rerender(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={true}
          isSaving={false}
        />,
      );

      const executeButton = screen.getByRole("button", { name: /executing/i });
      expect(executeButton).toBeDisabled();
    });

    it("disables execute button when disabled is true", async () => {
      const user = userEvent.setup();
      const { rerender } = render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "Patient View" }));

      rerender(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
          disabled
        />,
      );

      const executeButton = screen.getByRole("button", { name: /^execute$/i });
      expect(executeButton).toBeDisabled();
    });

    it("disables save button when no custom JSON is entered", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      const saveButton = screen.getByRole("button", { name: /save to server/i });
      expect(saveButton).toBeDisabled();
    });

    it("disables save button when isSaving is true", async () => {
      const user = userEvent.setup();
      const { rerender } = render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      const textarea = screen.getByRole("textbox");
      fireEvent.change(textarea, { target: { value: '{"resourceType": "ViewDefinition"}' } });

      rerender(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={true}
        />,
      );

      const saveButton = screen.getByRole("button", { name: /saving/i });
      expect(saveButton).toBeDisabled();
    });

    it("shows executing text when isExecuting is true", async () => {
      const user = userEvent.setup();
      const { rerender } = render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "Patient View" }));

      rerender(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={true}
          isSaving={false}
        />,
      );

      expect(screen.getByRole("button", { name: /executing/i })).toBeInTheDocument();
    });

    it("shows saving text when isSaving is true", async () => {
      const user = userEvent.setup();
      const { rerender } = render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      const textarea = screen.getByRole("textbox");
      fireEvent.change(textarea, { target: { value: '{"resourceType": "ViewDefinition"}' } });

      rerender(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={true}
        />,
      );

      expect(screen.getByRole("button", { name: /saving/i })).toBeInTheDocument();
    });
  });

  describe("help text", () => {
    it("displays JSON input help text in custom tab", async () => {
      const user = userEvent.setup();
      render(
        <SqlOnFhirForm
          onExecute={mockOnExecute}
          onSaveToServer={mockOnSaveToServer}
          isExecuting={false}
          isSaving={false}
        />,
      );

      await user.click(screen.getByRole("tab", { name: /provide json/i }));

      expect(
        screen.getByText(/enter a valid view definition resource in json format/i),
      ).toBeInTheDocument();
    });
  });
});

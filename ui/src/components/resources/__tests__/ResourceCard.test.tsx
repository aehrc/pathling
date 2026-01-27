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
 * Tests for the ResourceCard component.
 *
 * This test suite verifies that the ResourceCard component correctly displays
 * FHIR resource information, handles expand/collapse interactions, and
 * triggers the appropriate callbacks for delete and external link actions.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, waitFor } from "../../../test/testUtils";
import { ResourceCard } from "../ResourceCard";

import type { Resource } from "fhir/r4";

// Mock the useClipboard hook.
const mockCopyToClipboard = vi.fn();
vi.mock("../../../hooks", () => ({
  useClipboard: () => mockCopyToClipboard,
}));

// Mock window.open for external link testing.
const mockWindowOpen = vi.fn();

describe("ResourceCard", () => {
  const defaultFhirBaseUrl = "https://fhir.example.org/fhir";
  const defaultOnDelete = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    // Mock window.open.
    vi.spyOn(window, "open").mockImplementation(mockWindowOpen);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // Helper function to create a minimal FHIR resource.
  // Uses Record<string, unknown> for overrides to allow resource-specific properties.
  function createResource(overrides: Record<string, unknown> = {}): Resource {
    return {
      resourceType: "Patient",
      id: "test-patient-123",
      ...overrides,
    } as Resource;
  }

  describe("Rendering", () => {
    it("displays the resource type badge", () => {
      const resource = createResource();

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Patient")).toBeInTheDocument();
    });

    it("displays the resource ID", () => {
      const resource = createResource({ id: "my-resource-id" });

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("my-resource-id")).toBeInTheDocument();
    });

    it("does not show JSON content initially", () => {
      const resource = createResource();

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      // The JSON content should not be visible initially.
      expect(screen.queryByText(/"resourceType": "Patient"/)).not.toBeInTheDocument();
    });
  });

  describe("Resource summaries", () => {
    it("displays Patient name summary", () => {
      const resource = createResource({
        resourceType: "Patient",
        name: [{ family: "Smith", given: ["John", "William"] }],
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("John William Smith")).toBeInTheDocument();
    });

    it("displays Practitioner name with prefix", () => {
      const resource = createResource({
        resourceType: "Practitioner",
        name: [{ prefix: ["Dr"], given: ["Jane"], family: "Doe" }],
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Dr Jane Doe")).toBeInTheDocument();
    });

    it("displays Organization name", () => {
      const resource = createResource({
        resourceType: "Organization",
        name: "CSIRO Health",
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("CSIRO Health")).toBeInTheDocument();
    });

    it("displays Observation code display", () => {
      const resource = createResource({
        resourceType: "Observation",
        code: {
          coding: [{ display: "Blood Pressure" }],
        },
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Blood Pressure")).toBeInTheDocument();
    });

    it("displays Observation code text when no coding display", () => {
      const resource = createResource({
        resourceType: "Observation",
        code: {
          text: "Manual Blood Pressure Reading",
        },
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Manual Blood Pressure Reading")).toBeInTheDocument();
    });

    it("displays Condition code display", () => {
      const resource = createResource({
        resourceType: "Condition",
        code: {
          coding: [{ display: "Diabetes mellitus" }],
        },
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Diabetes mellitus")).toBeInTheDocument();
    });

    it("displays Medication code display", () => {
      const resource = createResource({
        resourceType: "Medication",
        code: {
          coding: [{ display: "Aspirin 100mg" }],
        },
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Aspirin 100mg")).toBeInTheDocument();
    });

    it("displays MedicationRequest medication display", () => {
      const resource = createResource({
        resourceType: "MedicationRequest",
        medicationCodeableConcept: {
          coding: [{ display: "Paracetamol 500mg" }],
        },
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Paracetamol 500mg")).toBeInTheDocument();
    });

    it("displays Procedure code display", () => {
      const resource = createResource({
        resourceType: "Procedure",
        code: {
          coding: [{ display: "Appendectomy" }],
        },
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Appendectomy")).toBeInTheDocument();
    });

    it("displays Encounter type display", () => {
      const resource = createResource({
        resourceType: "Encounter",
        type: [{ coding: [{ display: "Emergency visit" }] }],
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Emergency visit")).toBeInTheDocument();
    });

    it("does not display summary for unsupported resource types", () => {
      const resource = createResource({
        resourceType: "Bundle",
        id: "test-bundle",
      });

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      // Should show type and ID but no summary text.
      expect(screen.getByText("Bundle")).toBeInTheDocument();
      expect(screen.getByText("test-bundle")).toBeInTheDocument();
    });
  });

  describe("Expand/collapse behaviour", () => {
    it("expands to show JSON when badge is clicked", async () => {
      const user = userEvent.setup();
      const resource = createResource();

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      // Click on the badge to expand.
      await user.click(screen.getByText("Patient"));

      // Now the JSON content should be visible.
      await waitFor(() => {
        expect(screen.getByText(/"resourceType": "Patient"/)).toBeInTheDocument();
      });
    });

    it("expands to show JSON when ID is clicked", async () => {
      const user = userEvent.setup();
      const resource = createResource({ id: "clickable-id" });

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      // Click on the ID to expand.
      await user.click(screen.getByText("clickable-id"));

      // Now the JSON content should be visible.
      await waitFor(() => {
        expect(screen.getByText(/"id": "clickable-id"/)).toBeInTheDocument();
      });
    });

    it("expands to show JSON when summary is clicked", async () => {
      const user = userEvent.setup();
      const resource = createResource({
        resourceType: "Organization",
        name: "Test Organisation",
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      // Click on the summary to expand.
      await user.click(screen.getByText("Test Organisation"));

      // Now the JSON content should be visible.
      await waitFor(() => {
        expect(screen.getByText(/"name": "Test Organisation"/)).toBeInTheDocument();
      });
    });

    it("collapses JSON when badge is clicked again", async () => {
      const user = userEvent.setup();
      const resource = createResource();

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      const badge = screen.getByText("Patient");

      // Expand.
      await user.click(badge);
      await waitFor(() => {
        expect(screen.getByText(/"resourceType": "Patient"/)).toBeInTheDocument();
      });

      // Collapse.
      await user.click(badge);
      await waitFor(() => {
        expect(screen.queryByText(/"resourceType": "Patient"/)).not.toBeInTheDocument();
      });
    });
  });

  describe("External link button", () => {
    it("opens the resource in a new tab when external link button is clicked", async () => {
      const user = userEvent.setup();
      const resource = createResource({
        resourceType: "Patient",
        id: "patient-456",
      });

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      // The buttons are in the Flex container. Find them by their position.
      // First button is the external link, second is delete.
      const buttons = screen.getAllByRole("button");
      // The first button should be the external link button.
      const externalLinkButton = buttons[0];
      await user.click(externalLinkButton);

      expect(mockWindowOpen).toHaveBeenCalledWith(
        "https://fhir.example.org/fhir/Patient/patient-456",
        "_blank",
        "noopener,noreferrer",
      );
    });
  });

  describe("Delete button", () => {
    it("calls onDelete with resource type, ID, and summary", async () => {
      const user = userEvent.setup();
      const resource = createResource({
        resourceType: "Patient",
        id: "patient-to-delete",
        name: [{ family: "DeleteMe" }],
      }) as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      // The buttons are: [0] external link, [1] delete.
      const buttons = screen.getAllByRole("button");
      const deleteButton = buttons[1];
      await user.click(deleteButton);

      expect(defaultOnDelete).toHaveBeenCalledWith("Patient", "patient-to-delete", "DeleteMe");
    });

    it("calls onDelete with null summary when resource has no summary", async () => {
      const user = userEvent.setup();
      const resource = createResource({
        resourceType: "Bundle",
        id: "bundle-to-delete",
      });

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      // The buttons are: [0] external link, [1] delete.
      const buttons = screen.getAllByRole("button");
      const deleteButton = buttons[1];
      await user.click(deleteButton);

      expect(defaultOnDelete).toHaveBeenCalledWith("Bundle", "bundle-to-delete", null);
    });

    it("does not call onDelete when resource has no ID", async () => {
      const user = userEvent.setup();
      const resource = { resourceType: "Patient" } as Resource;

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      // The buttons are: [0] external link, [1] delete.
      const buttons = screen.getAllByRole("button");
      const deleteButton = buttons[1];
      await user.click(deleteButton);

      expect(defaultOnDelete).not.toHaveBeenCalled();
    });
  });

  describe("Copy to clipboard", () => {
    it("copies JSON to clipboard when copy button is clicked", async () => {
      const user = userEvent.setup();
      const resource = createResource({ id: "copy-me" });

      render(
        <ResourceCard
          resource={resource}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      // First expand the card to show the copy button.
      await user.click(screen.getByText("Patient"));

      // Wait for expansion.
      await waitFor(() => {
        expect(screen.getByText(/"resourceType": "Patient"/)).toBeInTheDocument();
      });

      // Click the copy button.
      const copyButton = screen.getByRole("button", { name: /copy to clipboard/i });
      await user.click(copyButton);

      expect(mockCopyToClipboard).toHaveBeenCalledWith(JSON.stringify(resource, null, 2));
    });
  });
});

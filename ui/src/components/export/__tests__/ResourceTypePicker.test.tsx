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
 * Tests for the ResourceTypePicker component which allows users to select
 * FHIR resource types for export operations.
 *
 * These tests verify that the component renders all available resource types
 * as selectable checkboxes, correctly displays pre-selected types, handles
 * selection and deselection of types, and provides a clear function to
 * reset the selection.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { ResourceTypePicker } from "../ResourceTypePicker";

describe("ResourceTypePicker", () => {
  const defaultResourceTypes = ["Patient", "Observation", "Condition", "Procedure"];
  const mockOnSelectedTypesChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("initial render", () => {
    it("renders the resource types label", () => {
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={[]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      expect(screen.getByText(/resource types/i)).toBeInTheDocument();
    });

    it("renders the empty selection hint", () => {
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={[]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      expect(screen.getByText(/leave empty to export all/i)).toBeInTheDocument();
    });

    it("renders the clear button", () => {
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={[]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      expect(screen.getByText(/^clear$/i)).toBeInTheDocument();
    });

    it("renders all provided resource types as checkboxes", () => {
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={[]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      defaultResourceTypes.forEach((type) => {
        expect(screen.getByRole("checkbox", { name: type })).toBeInTheDocument();
      });
    });

    it("renders checkboxes as unchecked when no types are selected", () => {
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={[]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      defaultResourceTypes.forEach((type) => {
        expect(screen.getByRole("checkbox", { name: type })).not.toBeChecked();
      });
    });
  });

  describe("selected types display", () => {
    it("renders selected types as checked", () => {
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={["Patient", "Observation"]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      expect(screen.getByRole("checkbox", { name: "Patient" })).toBeChecked();
      expect(screen.getByRole("checkbox", { name: "Observation" })).toBeChecked();
      expect(screen.getByRole("checkbox", { name: "Condition" })).not.toBeChecked();
      expect(screen.getByRole("checkbox", { name: "Procedure" })).not.toBeChecked();
    });

    it("handles all types being selected", () => {
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={defaultResourceTypes}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      defaultResourceTypes.forEach((type) => {
        expect(screen.getByRole("checkbox", { name: type })).toBeChecked();
      });
    });
  });

  describe("user interactions", () => {
    it("calls onSelectedTypesChange when a type is selected", async () => {
      const user = userEvent.setup();
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={[]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      const patientCheckbox = screen.getByRole("checkbox", { name: "Patient" });
      await user.click(patientCheckbox);

      expect(mockOnSelectedTypesChange).toHaveBeenCalledWith(["Patient"]);
    });

    it("calls onSelectedTypesChange with updated array when adding to existing selection", async () => {
      const user = userEvent.setup();
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={["Patient"]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      const observationCheckbox = screen.getByRole("checkbox", { name: "Observation" });
      await user.click(observationCheckbox);

      expect(mockOnSelectedTypesChange).toHaveBeenCalledWith(["Patient", "Observation"]);
    });

    it("calls onSelectedTypesChange with updated array when deselecting a type", async () => {
      const user = userEvent.setup();
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={["Patient", "Observation"]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      const patientCheckbox = screen.getByRole("checkbox", { name: "Patient" });
      await user.click(patientCheckbox);

      expect(mockOnSelectedTypesChange).toHaveBeenCalledWith(["Observation"]);
    });

    it("calls onSelectedTypesChange with empty array when clear is clicked", async () => {
      const user = userEvent.setup();
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={["Patient", "Observation"]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      const clearButton = screen.getByText(/^clear$/i);
      await user.click(clearButton);

      expect(mockOnSelectedTypesChange).toHaveBeenCalledWith([]);
    });

    it("calls onSelectedTypesChange with empty array when clear is clicked even with no selection", async () => {
      const user = userEvent.setup();
      render(
        <ResourceTypePicker
          resourceTypes={defaultResourceTypes}
          selectedTypes={[]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      const clearButton = screen.getByText(/^clear$/i);
      await user.click(clearButton);

      expect(mockOnSelectedTypesChange).toHaveBeenCalledWith([]);
    });
  });

  describe("edge cases", () => {
    it("handles empty resource types array", () => {
      render(
        <ResourceTypePicker
          resourceTypes={[]}
          selectedTypes={[]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      // Should still render the label and help text.
      expect(screen.getByText(/resource types/i)).toBeInTheDocument();
      expect(screen.getByText(/leave empty to export all/i)).toBeInTheDocument();

      // Should not render any checkboxes.
      expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    });

    it("handles single resource type", () => {
      render(
        <ResourceTypePicker
          resourceTypes={["Patient"]}
          selectedTypes={[]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      expect(screen.getByRole("checkbox", { name: "Patient" })).toBeInTheDocument();
    });

    it("handles large number of resource types", () => {
      const manyTypes = Array.from({ length: 50 }, (_, i) => `ResourceType${i}`);
      render(
        <ResourceTypePicker
          resourceTypes={manyTypes}
          selectedTypes={[]}
          onSelectedTypesChange={mockOnSelectedTypesChange}
        />,
      );

      // Verify first and last types are rendered.
      expect(screen.getByRole("checkbox", { name: "ResourceType0" })).toBeInTheDocument();
      expect(screen.getByRole("checkbox", { name: "ResourceType49" })).toBeInTheDocument();
    });
  });
});

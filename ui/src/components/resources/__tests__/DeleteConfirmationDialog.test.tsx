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
 * Tests for the DeleteConfirmationDialog component.
 *
 * This test suite verifies that the DeleteConfirmationDialog correctly displays
 * resource information, handles user interactions for cancel and confirm actions,
 * and shows appropriate loading state during deletion.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { DeleteConfirmationDialog } from "../DeleteConfirmationDialog";

describe("DeleteConfirmationDialog", () => {
  const defaultOnOpenChange = vi.fn();
  const defaultOnConfirm = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("Rendering", () => {
    it("does not render content when closed", () => {
      render(
        <DeleteConfirmationDialog
          open={false}
          onOpenChange={defaultOnOpenChange}
          resourceType="Patient"
          resourceId="123"
          resourceSummary="John Smith"
          onConfirm={defaultOnConfirm}
          isDeleting={false}
        />,
      );

      expect(screen.queryByText("Delete resource")).not.toBeInTheDocument();
    });

    it("renders dialog content when open", () => {
      render(
        <DeleteConfirmationDialog
          open={true}
          onOpenChange={defaultOnOpenChange}
          resourceType="Patient"
          resourceId="123"
          resourceSummary="John Smith"
          onConfirm={defaultOnConfirm}
          isDeleting={false}
        />,
      );

      expect(screen.getByText("Delete resource")).toBeInTheDocument();
      expect(
        screen.getByText(
          "Are you sure you want to delete this resource? This action cannot be undone.",
        ),
      ).toBeInTheDocument();
    });

    it("displays resource type and ID", () => {
      render(
        <DeleteConfirmationDialog
          open={true}
          onOpenChange={defaultOnOpenChange}
          resourceType="Observation"
          resourceId="obs-456"
          resourceSummary={null}
          onConfirm={defaultOnConfirm}
          isDeleting={false}
        />,
      );

      expect(screen.getByText("Observation/obs-456")).toBeInTheDocument();
    });

    it("displays resource summary when provided", () => {
      render(
        <DeleteConfirmationDialog
          open={true}
          onOpenChange={defaultOnOpenChange}
          resourceType="Patient"
          resourceId="123"
          resourceSummary="John William Smith"
          onConfirm={defaultOnConfirm}
          isDeleting={false}
        />,
      );

      expect(screen.getByText("Patient/123")).toBeInTheDocument();
      expect(screen.getByText("John William Smith")).toBeInTheDocument();
    });

    it("does not display summary when null", () => {
      render(
        <DeleteConfirmationDialog
          open={true}
          onOpenChange={defaultOnOpenChange}
          resourceType="Bundle"
          resourceId="bundle-789"
          resourceSummary={null}
          onConfirm={defaultOnConfirm}
          isDeleting={false}
        />,
      );

      expect(screen.getByText("Bundle/bundle-789")).toBeInTheDocument();
      // Only the type/id should be present, not an additional summary line.
      const descriptions = screen.getAllByText(/Bundle/);
      expect(descriptions).toHaveLength(1);
    });
  });

  describe("User interactions", () => {
    it("calls onOpenChange with false when Cancel button is clicked", async () => {
      const user = userEvent.setup();

      render(
        <DeleteConfirmationDialog
          open={true}
          onOpenChange={defaultOnOpenChange}
          resourceType="Patient"
          resourceId="123"
          resourceSummary={null}
          onConfirm={defaultOnConfirm}
          isDeleting={false}
        />,
      );

      const cancelButton = screen.getByRole("button", { name: /cancel/i });
      await user.click(cancelButton);

      expect(defaultOnOpenChange).toHaveBeenCalledWith(false);
    });

    it("calls onConfirm when Delete button is clicked", async () => {
      const user = userEvent.setup();

      render(
        <DeleteConfirmationDialog
          open={true}
          onOpenChange={defaultOnOpenChange}
          resourceType="Patient"
          resourceId="123"
          resourceSummary={null}
          onConfirm={defaultOnConfirm}
          isDeleting={false}
        />,
      );

      const deleteButton = screen.getByRole("button", { name: /delete/i });
      await user.click(deleteButton);

      expect(defaultOnConfirm).toHaveBeenCalledTimes(1);
    });
  });

  describe("Loading state", () => {
    it("disables Cancel button when isDeleting is true", () => {
      render(
        <DeleteConfirmationDialog
          open={true}
          onOpenChange={defaultOnOpenChange}
          resourceType="Patient"
          resourceId="123"
          resourceSummary={null}
          onConfirm={defaultOnConfirm}
          isDeleting={true}
        />,
      );

      const cancelButton = screen.getByRole("button", { name: /cancel/i });
      expect(cancelButton).toBeDisabled();
    });

    it("disables Delete button when isDeleting is true", () => {
      render(
        <DeleteConfirmationDialog
          open={true}
          onOpenChange={defaultOnOpenChange}
          resourceType="Patient"
          resourceId="123"
          resourceSummary={null}
          onConfirm={defaultOnConfirm}
          isDeleting={true}
        />,
      );

      const deleteButton = screen.getByRole("button", { name: /delete/i });
      expect(deleteButton).toBeDisabled();
    });

    it("enables both buttons when isDeleting is false", () => {
      render(
        <DeleteConfirmationDialog
          open={true}
          onOpenChange={defaultOnOpenChange}
          resourceType="Patient"
          resourceId="123"
          resourceSummary={null}
          onConfirm={defaultOnConfirm}
          isDeleting={false}
        />,
      );

      const cancelButton = screen.getByRole("button", { name: /cancel/i });
      const deleteButton = screen.getByRole("button", { name: /delete/i });

      expect(cancelButton).not.toBeDisabled();
      expect(deleteButton).not.toBeDisabled();
    });
  });
});

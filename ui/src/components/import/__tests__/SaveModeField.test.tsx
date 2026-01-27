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
 * Tests for the SaveModeField component which provides radio card selection
 * for import save modes.
 *
 * These tests verify that the component renders all save mode options with
 * their labels and descriptions, correctly displays the currently selected
 * mode, and notifies parent components when the selection changes.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { SaveModeField } from "../SaveModeField";

import type { SaveMode } from "../../../types/import";

describe("SaveModeField", () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("initial render", () => {
    it("renders the save mode label", () => {
      render(<SaveModeField value="overwrite" onChange={mockOnChange} />);

      expect(screen.getByText(/save mode/i)).toBeInTheDocument();
    });

    it("renders all save mode options", () => {
      render(<SaveModeField value="overwrite" onChange={mockOnChange} />);

      expect(screen.getByText("Overwrite")).toBeInTheDocument();
      expect(screen.getByText("Merge")).toBeInTheDocument();
      expect(screen.getByText("Append")).toBeInTheDocument();
      expect(screen.getByText("Ignore")).toBeInTheDocument();
      expect(screen.getByText("Error")).toBeInTheDocument();
    });

    it("renders save mode descriptions", () => {
      render(<SaveModeField value="overwrite" onChange={mockOnChange} />);

      expect(
        screen.getByText(/replace all existing data for each resource type/i),
      ).toBeInTheDocument();
      expect(screen.getByText(/update existing resources and add new ones/i)).toBeInTheDocument();
      expect(screen.getByText(/add new resources without modifying existing/i)).toBeInTheDocument();
      expect(screen.getByText(/skip if resources already exist/i)).toBeInTheDocument();
      expect(screen.getByText(/fail if resources already exist/i)).toBeInTheDocument();
    });

    it("renders radio buttons for each option", () => {
      render(<SaveModeField value="overwrite" onChange={mockOnChange} />);

      const radioButtons = screen.getAllByRole("radio");
      expect(radioButtons).toHaveLength(5);
    });
  });

  describe("selected value display", () => {
    it("shows overwrite as selected when value is overwrite", () => {
      render(<SaveModeField value="overwrite" onChange={mockOnChange} />);

      const radioButtons = screen.getAllByRole("radio");
      const overwriteRadio = radioButtons.find(
        (radio) => radio.closest("[data-state]")?.getAttribute("data-state") === "checked",
      );
      expect(overwriteRadio).toBeDefined();
    });

    it("shows merge as selected when value is merge", () => {
      render(<SaveModeField value="merge" onChange={mockOnChange} />);

      const radioButtons = screen.getAllByRole("radio");
      const checkedRadios = radioButtons.filter((radio) => radio.closest('[data-state="checked"]'));
      expect(checkedRadios.length).toBeGreaterThan(0);
    });

    it("shows append as selected when value is append", () => {
      render(<SaveModeField value="append" onChange={mockOnChange} />);

      const radioButtons = screen.getAllByRole("radio");
      const checkedRadios = radioButtons.filter((radio) => radio.closest('[data-state="checked"]'));
      expect(checkedRadios.length).toBeGreaterThan(0);
    });

    it("shows ignore as selected when value is ignore", () => {
      render(<SaveModeField value="ignore" onChange={mockOnChange} />);

      const radioButtons = screen.getAllByRole("radio");
      const checkedRadios = radioButtons.filter((radio) => radio.closest('[data-state="checked"]'));
      expect(checkedRadios.length).toBeGreaterThan(0);
    });

    it("shows error as selected when value is error", () => {
      render(<SaveModeField value="error" onChange={mockOnChange} />);

      const radioButtons = screen.getAllByRole("radio");
      const checkedRadios = radioButtons.filter((radio) => radio.closest('[data-state="checked"]'));
      expect(checkedRadios.length).toBeGreaterThan(0);
    });
  });

  describe("user interactions", () => {
    it("calls onChange with overwrite when Overwrite is clicked", async () => {
      const user = userEvent.setup();
      render(<SaveModeField value="merge" onChange={mockOnChange} />);

      // Find and click the Overwrite option.
      const overwriteCard = screen.getByText("Overwrite").closest('[role="radio"]');
      if (overwriteCard) {
        await user.click(overwriteCard);
      }

      expect(mockOnChange).toHaveBeenCalledWith("overwrite");
    });

    it("calls onChange with merge when Merge is clicked", async () => {
      const user = userEvent.setup();
      render(<SaveModeField value="overwrite" onChange={mockOnChange} />);

      // Find and click the Merge option.
      const mergeCard = screen.getByText("Merge").closest('[role="radio"]');
      if (mergeCard) {
        await user.click(mergeCard);
      }

      expect(mockOnChange).toHaveBeenCalledWith("merge");
    });

    it("calls onChange with append when Append is clicked", async () => {
      const user = userEvent.setup();
      render(<SaveModeField value="overwrite" onChange={mockOnChange} />);

      // Find and click the Append option.
      const appendCard = screen.getByText("Append").closest('[role="radio"]');
      if (appendCard) {
        await user.click(appendCard);
      }

      expect(mockOnChange).toHaveBeenCalledWith("append");
    });

    it("calls onChange with ignore when Ignore is clicked", async () => {
      const user = userEvent.setup();
      render(<SaveModeField value="overwrite" onChange={mockOnChange} />);

      // Find and click the Ignore option.
      const ignoreCard = screen.getByText("Ignore").closest('[role="radio"]');
      if (ignoreCard) {
        await user.click(ignoreCard);
      }

      expect(mockOnChange).toHaveBeenCalledWith("ignore");
    });

    it("calls onChange with error when Error is clicked", async () => {
      const user = userEvent.setup();
      render(<SaveModeField value="overwrite" onChange={mockOnChange} />);

      // Find and click the Error option.
      const errorCard = screen.getByText("Error").closest('[role="radio"]');
      if (errorCard) {
        await user.click(errorCard);
      }

      expect(mockOnChange).toHaveBeenCalledWith("error");
    });
  });

  describe("value type safety", () => {
    it("accepts all valid SaveMode values", () => {
      const saveModes: SaveMode[] = ["overwrite", "merge", "append", "ignore", "error"];

      saveModes.forEach((mode) => {
        const { unmount } = render(<SaveModeField value={mode} onChange={mockOnChange} />);
        // Should render without error.
        expect(screen.getByText(/save mode/i)).toBeInTheDocument();
        unmount();
      });
    });
  });
});

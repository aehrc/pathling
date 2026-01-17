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
 * Tests for the ExportControls component which provides format selection and
 * export button for ViewDefinition exports.
 *
 * These tests verify that the component renders the format selector with all
 * available options, defaults to NDJSON format, handles format selection
 * changes, and invokes the onExport callback with the correct format when
 * the export button is clicked.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { ExportControls } from "../ExportControls";

import type { ViewExportFormat } from "../../../types/viewExport";

describe("ExportControls", () => {
  const mockOnExport = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("initial render", () => {
    it("renders the format selector", () => {
      render(<ExportControls onExport={mockOnExport} />);

      expect(screen.getByRole("combobox")).toBeInTheDocument();
    });

    it("renders the export button", () => {
      render(<ExportControls onExport={mockOnExport} />);

      expect(screen.getByRole("button", { name: /^export$/i })).toBeInTheDocument();
    });

    it("defaults to NDJSON format", () => {
      render(<ExportControls onExport={mockOnExport} />);

      const combobox = screen.getByRole("combobox");
      expect(combobox).toHaveTextContent("NDJSON");
    });
  });

  describe("format options", () => {
    it("shows all format options when selector is opened", async () => {
      const user = userEvent.setup();
      render(<ExportControls onExport={mockOnExport} />);

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);

      expect(screen.getByRole("option", { name: "NDJSON" })).toBeInTheDocument();
      expect(screen.getByRole("option", { name: "CSV" })).toBeInTheDocument();
      expect(screen.getByRole("option", { name: "Parquet" })).toBeInTheDocument();
    });

    it("allows selecting CSV format", async () => {
      const user = userEvent.setup();
      render(<ExportControls onExport={mockOnExport} />);

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "CSV" }));

      expect(combobox).toHaveTextContent("CSV");
    });

    it("allows selecting Parquet format", async () => {
      const user = userEvent.setup();
      render(<ExportControls onExport={mockOnExport} />);

      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "Parquet" }));

      expect(combobox).toHaveTextContent("Parquet");
    });
  });

  describe("export button", () => {
    it("calls onExport with ndjson format when export is clicked with default selection", async () => {
      const user = userEvent.setup();
      render(<ExportControls onExport={mockOnExport} />);

      await user.click(screen.getByRole("button", { name: /^export$/i }));

      expect(mockOnExport).toHaveBeenCalledTimes(1);
      expect(mockOnExport).toHaveBeenCalledWith("ndjson");
    });

    it("calls onExport with csv format when CSV is selected", async () => {
      const user = userEvent.setup();
      render(<ExportControls onExport={mockOnExport} />);

      // Select CSV format.
      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "CSV" }));

      // Click export.
      await user.click(screen.getByRole("button", { name: /^export$/i }));

      expect(mockOnExport).toHaveBeenCalledTimes(1);
      expect(mockOnExport).toHaveBeenCalledWith("csv");
    });

    it("calls onExport with parquet format when Parquet is selected", async () => {
      const user = userEvent.setup();
      render(<ExportControls onExport={mockOnExport} />);

      // Select Parquet format.
      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "Parquet" }));

      // Click export.
      await user.click(screen.getByRole("button", { name: /^export$/i }));

      expect(mockOnExport).toHaveBeenCalledTimes(1);
      expect(mockOnExport).toHaveBeenCalledWith("parquet");
    });

    it("retains selected format across multiple exports", async () => {
      const user = userEvent.setup();
      render(<ExportControls onExport={mockOnExport} />);

      // Select CSV format.
      const combobox = screen.getByRole("combobox");
      await user.click(combobox);
      await user.click(screen.getByRole("option", { name: "CSV" }));

      // Click export twice.
      const exportButton = screen.getByRole("button", { name: /^export$/i });
      await user.click(exportButton);
      await user.click(exportButton);

      expect(mockOnExport).toHaveBeenCalledTimes(2);
      expect(mockOnExport).toHaveBeenNthCalledWith(1, "csv");
      expect(mockOnExport).toHaveBeenNthCalledWith(2, "csv");
    });
  });

  describe("disabled state", () => {
    it("disables the format selector when disabled is true", () => {
      render(<ExportControls onExport={mockOnExport} disabled />);

      const combobox = screen.getByRole("combobox");
      expect(combobox).toHaveAttribute("data-disabled", "");
    });

    it("disables the export button when disabled is true", () => {
      render(<ExportControls onExport={mockOnExport} disabled />);

      const exportButton = screen.getByRole("button", { name: /^export$/i });
      expect(exportButton).toBeDisabled();
    });

    it("enables controls when disabled is false", () => {
      render(<ExportControls onExport={mockOnExport} disabled={false} />);

      const exportButton = screen.getByRole("button", { name: /^export$/i });
      expect(exportButton).not.toBeDisabled();
    });

    it("enables controls when disabled is not provided", () => {
      render(<ExportControls onExport={mockOnExport} />);

      const exportButton = screen.getByRole("button", { name: /^export$/i });
      expect(exportButton).not.toBeDisabled();
    });
  });

  describe("format type safety", () => {
    it("accepts all valid ViewExportFormat values through onExport", async () => {
      const user = userEvent.setup();
      const formats: ViewExportFormat[] = ["ndjson", "csv", "parquet"];
      const formatLabels: Record<ViewExportFormat, string> = {
        ndjson: "NDJSON",
        csv: "CSV",
        parquet: "Parquet",
      };

      for (const format of formats) {
        vi.clearAllMocks();

        const { unmount } = render(<ExportControls onExport={mockOnExport} />);

        const combobox = screen.getByRole("combobox");
        await user.click(combobox);
        await user.click(screen.getByRole("option", { name: formatLabels[format] }));

        await user.click(screen.getByRole("button", { name: /^export$/i }));

        expect(mockOnExport).toHaveBeenCalledWith(format);

        unmount();
      }
    });
  });
});

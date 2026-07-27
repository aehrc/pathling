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
 * Tests for the Export page, covering the reporting of export failures.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../test/testUtils";
import { Export } from "../Export";

import type { ExportRequest } from "../../types/export";
import type { ReactNode } from "react";

// Mock the toast context, which is where failures must be reported.
const mockShowToast = vi.fn();
vi.mock("../../contexts/ToastContext", () => ({
  useToast: () => ({ showToast: mockShowToast }),
}));

// Mock the guard so the page content renders without an access check.
vi.mock("../../components/auth/CapabilityGuard", () => ({
  CapabilityGuard: ({ children }: { children: (capabilities: undefined) => ReactNode }) =>
    children(undefined),
}));

vi.mock("../../hooks", () => ({
  buildSearchParamMap: () => undefined,
}));

// Mock the form so a single click starts an export.
vi.mock("../../components/export/ExportForm", () => ({
  ExportForm: ({ onSubmit }: { onSubmit: (request: ExportRequest) => void }) => (
    <button onClick={() => onSubmit({ level: "system" })}>Start export</button>
  ),
}));

// Mock the card so a single click drives its error callback.
vi.mock("../../components/export/ExportCard", () => ({
  ExportCard: ({ onError }: { onError: (message: string) => void }) => (
    <button onClick={() => onError("Download failed: connection reset")}>Fail export</button>
  ),
}));

describe("Export page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // FR-015: the export card's error callback is the only channel for a failed
  // file download, so it must reach the user.
  it("reports an export failure to the user", async () => {
    const user = userEvent.setup();

    render(<Export />);

    await user.click(screen.getByRole("button", { name: "Start export" }));
    await user.click(screen.getByRole("button", { name: "Fail export" }));

    expect(mockShowToast).toHaveBeenCalledWith(
      "Export failed",
      "Download failed: connection reset",
    );
  });
});

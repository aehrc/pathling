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
 * Tests for the Import page, covering the reporting of import failures.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../test/testUtils";
import { Import } from "../Import";

import type { ImportRequest } from "../../types/import";
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

// Mock the forms so a single click starts an import.
vi.mock("../../components/import/ImportForm", () => ({
  ImportForm: ({ onSubmit }: { onSubmit: (request: ImportRequest) => void }) => (
    <button onClick={() => onSubmit({} as ImportRequest)}>Start import</button>
  ),
}));
vi.mock("../../components/import/ImportPnpForm", () => ({
  ImportPnpForm: () => <div data-testid="import-pnp-form" />,
}));

// Mock the card so a single click drives its error callback.
vi.mock("../../components/import/ImportCard", () => ({
  ImportCard: ({ onError }: { onError: (message: string) => void }) => (
    <button onClick={() => onError("Import job failed: invalid NDJSON")}>Fail import</button>
  ),
}));

describe("Import page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // FR-017: an import job failure must produce a visible message.
  it("reports an import failure to the user", async () => {
    const user = userEvent.setup();

    render(<Import />);

    await user.click(screen.getByRole("button", { name: "Start import" }));
    await user.click(screen.getByRole("button", { name: "Fail import" }));

    expect(mockShowToast).toHaveBeenCalledWith(
      "Import failed",
      "Import job failed: invalid NDJSON",
    );
  });
});

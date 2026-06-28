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
 * Tests for the SqlPreview component.
 *
 * Verifies the shared, height-bounded SQL display: the read-only text area
 * carries the supplied SQL and accessible label, defaults to ten visible
 * rows, honours a custom row count, and copies the SQL on demand.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { SqlPreview } from "../SqlPreview";

const mockCopy = vi.fn();

// The hooks barrel transitively imports main.tsx (via AuthContext), which
// runs createRoot at load. Mock it; the preview only needs useClipboard.
vi.mock("../../../hooks", () => ({
  useClipboard: () => mockCopy,
}));

describe("SqlPreview", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // The SQL is rendered in a read-only text area carrying the supplied label.
  it("renders the SQL in a labelled read-only text area", () => {
    render(<SqlPreview sql="SELECT 1" ariaLabel="Submitted SQL" />);

    const area = screen.getByRole("textbox", { name: /submitted sql/i });
    expect(area).toHaveValue("SELECT 1");
    expect(area).toHaveAttribute("readonly");
  });

  // The height is bounded by a default of ten visible rows so that long SQL
  // scrolls within the area rather than expanding the card.
  it("defaults to ten visible rows", () => {
    render(<SqlPreview sql="SELECT 1" ariaLabel="Submitted SQL" />);

    expect(screen.getByRole("textbox", { name: /submitted sql/i })).toHaveAttribute("rows", "10");
  });

  // The row count is configurable for callers that need a shorter area.
  it("honours a custom row count", () => {
    render(<SqlPreview sql="SELECT 1" ariaLabel="Submitted SQL" rows={4} />);

    expect(screen.getByRole("textbox", { name: /submitted sql/i })).toHaveAttribute("rows", "4");
  });

  // The copy control writes the current SQL to the clipboard.
  it("copies the SQL to the clipboard when the copy button is clicked", async () => {
    const user = userEvent.setup();
    render(<SqlPreview sql="SELECT * FROM patients" ariaLabel="Submitted SQL" />);

    await user.click(screen.getByRole("button", { name: /copy sql to clipboard/i }));

    expect(mockCopy).toHaveBeenCalledWith("SELECT * FROM patients");
  });
});

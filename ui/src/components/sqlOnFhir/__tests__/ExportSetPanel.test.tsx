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
 * Tests for the export set panel: what it shows, what it disables, and what it
 * reports back to the page.
 *
 * @author John Grimes
 */

import { Theme } from "@radix-ui/themes";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { ExportSetPanel } from "../ExportSetPanel";

import type { ExportSetEntry } from "../../../hooks/exportSetHelpers";

const ENTRIES: ExportSetEntry[] = [
  {
    id: "1",
    name: "demographics",
    kind: "view",
    subject: { kind: "reference", reference: "ViewDefinition/demographics" },
  },
  {
    id: "2",
    name: "bp_summary",
    kind: "query",
    subject: { kind: "reference", reference: "Library/bp-summary" },
  },
];

/**
 * Renders the panel with the given overrides.
 *
 * @param overrides - Props to override the defaults with.
 * @returns The spy callbacks passed to the panel.
 */
function renderPanel(overrides: Partial<React.ComponentProps<typeof ExportSetPanel>> = {}) {
  const callbacks = {
    onRename: vi.fn(),
    onRemove: vi.fn(),
    onClear: vi.fn(),
    onFormatChange: vi.fn(),
    onFilterChange: vi.fn(),
    onExport: vi.fn(),
  };
  render(
    <Theme>
      <ExportSetPanel
        entries={ENTRIES}
        format="ndjson"
        filters={{ patients: "", groups: "", since: "" }}
        collisions={[]}
        {...callbacks}
        {...overrides}
      />
    </Theme>,
  );
  return callbacks;
}

describe("ExportSetPanel", () => {
  // An empty basket has nothing to say and no action to offer, so it takes up
  // no room on the page.
  it("renders nothing when the set is empty", () => {
    const { container } = render(
      <Theme>
        <ExportSetPanel
          entries={[]}
          format="ndjson"
          filters={{ patients: "", groups: "", since: "" }}
          collisions={[]}
          onRename={vi.fn()}
          onRemove={vi.fn()}
          onClear={vi.fn()}
          onFormatChange={vi.fn()}
          onFilterChange={vi.fn()}
          onExport={vi.fn()}
        />
      </Theme>,
    );

    expect(container.querySelector(".rt-Card")).toBeNull();
  });

  it("lists each entry with its kind badge and current name", () => {
    renderPanel();

    expect(screen.getByText("Export set (2)")).toBeInTheDocument();
    expect(screen.getByText("view")).toBeInTheDocument();
    expect(screen.getByText("query")).toBeInTheDocument();
    expect(screen.getByDisplayValue("demographics")).toBeInTheDocument();
    expect(screen.getByDisplayValue("bp_summary")).toBeInTheDocument();
  });

  it("reports an inline rename", async () => {
    const user = userEvent.setup();
    const { onRename } = renderPanel();

    await user.type(screen.getByDisplayValue("demographics"), "!");

    expect(onRename).toHaveBeenCalledWith("1", "demographics!");
  });

  it("reports a removal and a clear-all", async () => {
    const user = userEvent.setup();
    const { onRemove, onClear } = renderPanel();

    await user.click(
      screen.getByRole("button", {
        name: /remove demographics from the export set/i,
      }),
    );
    await user.click(screen.getByRole("button", { name: /clear all/i }));

    expect(onRemove).toHaveBeenCalledWith("1");
    expect(onClear).toHaveBeenCalled();
  });

  // The manifest correlates outputs by name, so exporting a set with a
  // repeated name would produce files the user cannot tell apart.
  it("disables the export and explains why when two names collide", () => {
    renderPanel({
      entries: [ENTRIES[0], { ...ENTRIES[1], name: "demographics" }],
      collisions: ["demographics"],
    });

    expect(screen.getByRole("button", { name: "Export set" })).toBeDisabled();
    expect(screen.getByRole("alert")).toHaveTextContent(
      /distinct name; demographics is used more than once/i,
    );
  });

  it("disables the export and explains why when a name is blank", () => {
    renderPanel({ collisions: [""] });

    expect(screen.getByRole("button", { name: "Export set" })).toBeDisabled();
    expect(screen.getByRole("alert")).toHaveTextContent(/needs a name/i);
  });

  it("binds the job-wide filter inputs to their values", async () => {
    const user = userEvent.setup();
    const { onFilterChange } = renderPanel({
      filters: { patients: "p1", groups: "", since: "" },
    });

    expect(screen.getByLabelText("Patients")).toHaveValue("p1");
    await user.type(screen.getByLabelText("Groups"), "g");

    expect(onFilterChange).toHaveBeenCalledWith("groups", "g");
  });

  it("reports the export request when the set is valid", async () => {
    const user = userEvent.setup();
    const { onExport } = renderPanel();

    const exportButton = screen.getByRole("button", { name: "Export set" });
    expect(exportButton).toBeEnabled();
    await user.click(exportButton);

    expect(onExport).toHaveBeenCalled();
  });
});

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
 * Tests for the shared error callout presentation.
 *
 * @author John Grimes
 */

import { Button } from "@radix-ui/themes";
import { describe, expect, it } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { ErrorCallout } from "../ErrorCallout";

describe("ErrorCallout", () => {
  // The message is the only required prop and is always shown.
  it("renders the message", () => {
    render(<ErrorCallout message="Search failed: unknown search parameter." />);

    expect(screen.getByText("Search failed: unknown search parameter.")).toBeInTheDocument();
  });

  // A heading is optional and, when given, sits in bold above the message.
  it("renders the optional title above the message", () => {
    render(<ErrorCallout title="Authentication failed" message="No authorisation code." />);

    const title = screen.getByText("Authentication failed");
    const message = screen.getByText("No authorisation code.");
    expect(title).toBeInTheDocument();
    expect(message).toBeInTheDocument();
    // The title precedes the message in document order.
    expect(title.compareDocumentPosition(message)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
  });

  // Recovery actions are passed as children and stack under the message.
  it("renders children below the message", () => {
    render(
      <ErrorCallout message="Could not load jobs.">
        <Button>Retry</Button>
      </ErrorCallout>,
    );

    const message = screen.getByText("Could not load jobs.");
    const action = screen.getByRole("button", { name: "Retry" });
    expect(action).toBeInTheDocument();
    expect(message.compareDocumentPosition(action)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
  });

  // The two form sites need a smaller callout with a top margin.
  it("applies the size and top margin when given", () => {
    render(<ErrorCallout message="Save failed." size="1" mt="3" />);

    const callout = screen.getByRole("alert");
    expect(callout.className).toContain("rt-r-size-1");
    expect(callout.className).toContain("rt-r-mt-3");
  });

  // Every callout is announced to assistive technology (FR-020).
  it("is always announced as an alert", () => {
    render(<ErrorCallout message="Something went wrong." />);

    expect(screen.getByRole("alert")).toHaveTextContent("Something went wrong.");
  });

  // Every callout carries the same warning icon (FR-021).
  it("always renders the warning icon", () => {
    const { container } = render(<ErrorCallout message="Something went wrong." />);

    // The Radix exclamation triangle renders as an inline SVG within the callout.
    expect(container.querySelector(".rt-CalloutIcon svg")).toBeInTheDocument();
  });
});

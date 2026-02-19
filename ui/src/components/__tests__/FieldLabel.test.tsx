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
 * Tests for the FieldLabel component which renders consistent form field
 * labels.
 *
 * @author John Grimes
 */

import { describe, expect, it } from "vitest";

import { render, screen } from "../../test/testUtils";
import { FieldLabel } from "../FieldLabel";

describe("FieldLabel", () => {
  // Default rendering: renders children as a label with standard typography
  // and default bottom margin of "1".
  it("renders children as a label with default mb of 1", () => {
    render(<FieldLabel>Resource type</FieldLabel>);

    const label = screen.getByText("Resource type");
    expect(label).toBeInTheDocument();
    // Radix UI Text renders with rt-r-mb-* classes for margin bottom.
    expect(label).toHaveClass("rt-r-mb-1");
  });

  // Custom bottom margin: the mb prop overrides the default.
  it("renders with custom mb value", () => {
    render(<FieldLabel mb="2">Resource type</FieldLabel>);

    const label = screen.getByText("Resource type");
    expect(label).toHaveClass("rt-r-mb-2");
  });

  // No bottom margin: mb="0" removes the bottom margin.
  it("renders with no bottom margin when mb is 0", () => {
    render(<FieldLabel mb="0">Resource type</FieldLabel>);

    const label = screen.getByText("Resource type");
    expect(label).not.toHaveClass("rt-r-mb-1");
    expect(label).not.toHaveClass("rt-r-mb-2");
  });

  // Optional marker: when optional is true, appends "(optional)" suffix.
  it("renders optional marker when optional is true", () => {
    render(<FieldLabel optional>Elements</FieldLabel>);

    expect(screen.getByText("Elements")).toBeInTheDocument();
    expect(screen.getByText("(optional)")).toBeInTheDocument();
  });

  // No optional marker by default: the optional suffix is not rendered when
  // the prop is omitted.
  it("does not render optional marker by default", () => {
    render(<FieldLabel>Resource type</FieldLabel>);

    expect(screen.getByText("Resource type")).toBeInTheDocument();
    expect(screen.queryByText("(optional)")).not.toBeInTheDocument();
  });
});

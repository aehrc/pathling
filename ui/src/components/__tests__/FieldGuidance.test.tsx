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
 * Tests for the FieldGuidance component which renders small grey helper text
 * below form fields.
 *
 * @author John Grimes
 */

import { describe, expect, it } from "vitest";

import { render, screen } from "../../test/testUtils";
import { FieldGuidance } from "../FieldGuidance";

describe("FieldGuidance", () => {
  it("renders children text", () => {
    render(<FieldGuidance>Only resources updated after this time.</FieldGuidance>);

    expect(screen.getByText("Only resources updated after this time.")).toBeInTheDocument();
  });

  it("renders with default mt of 1", () => {
    render(<FieldGuidance>Some guidance text.</FieldGuidance>);

    // Radix UI Text renders with rt-r-mt-* classes for margin top.
    const textElement = screen.getByText("Some guidance text.");
    expect(textElement).toHaveClass("rt-r-mt-1");
  });

  it("renders with custom mt value", () => {
    render(<FieldGuidance mt="2">Some guidance text.</FieldGuidance>);

    const textElement = screen.getByText("Some guidance text.");
    expect(textElement).toHaveClass("rt-r-mt-2");
  });

  it("renders with mt 0 when no margin is needed", () => {
    render(<FieldGuidance mt="0">Some guidance text.</FieldGuidance>);

    const textElement = screen.getByText("Some guidance text.");
    expect(textElement).not.toHaveClass("rt-r-mt-1");
    expect(textElement).not.toHaveClass("rt-r-mt-2");
  });
});

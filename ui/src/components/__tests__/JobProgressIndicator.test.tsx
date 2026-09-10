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
 * Tests for the JobProgressIndicator component, which renders either a
 * determinate progress bar or an indeterminate spinner for active jobs.
 *
 * @author John Grimes
 */

import { describe, expect, it } from "vitest";

import { render, screen } from "../../test/testUtils";
import { JobProgressIndicator } from "../JobProgressIndicator";

describe("JobProgressIndicator", () => {
  // Determinate state: a numeric progress renders a progress bar with the
  // percentage complete.
  it("renders a progress bar with the percentage when progress is a number", () => {
    render(<JobProgressIndicator progress={42} pendingLabel="Processing..." />);

    expect(screen.getByText("Progress")).toBeInTheDocument();
    expect(screen.getByText("42%")).toBeInTheDocument();
    const bar = screen.getByRole("progressbar");
    expect(bar).toHaveAttribute("aria-valuenow", "42");
    // The pending spinner label should not be shown in the determinate state.
    expect(screen.queryByText("Processing...")).not.toBeInTheDocument();
  });

  // Edge case: zero is a valid numeric progress and must render the bar, not
  // the spinner.
  it("renders the progress bar when progress is zero", () => {
    render(<JobProgressIndicator progress={0} pendingLabel="Processing..." />);

    expect(screen.getByText("0%")).toBeInTheDocument();
    expect(screen.getByRole("progressbar")).toBeInTheDocument();
    expect(screen.queryByText("Processing...")).not.toBeInTheDocument();
  });

  // Indeterminate state: an undefined progress renders the spinner with the
  // supplied pending label.
  it("renders the spinner and pending label when progress is undefined", () => {
    render(<JobProgressIndicator progress={undefined} pendingLabel="Monitoring..." />);

    expect(screen.getByText("Monitoring...")).toBeInTheDocument();
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
    expect(screen.queryByText("Progress")).not.toBeInTheDocument();
  });

  // Indeterminate state: a null progress behaves the same as undefined.
  it("renders the spinner and pending label when progress is null", () => {
    render(<JobProgressIndicator progress={null} pendingLabel="Exporting..." />);

    expect(screen.getByText("Exporting...")).toBeInTheDocument();
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
  });
});

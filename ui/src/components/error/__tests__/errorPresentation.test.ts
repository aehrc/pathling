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
 * Tests for the derivation of displayable content from a failure.
 *
 * @author John Grimes
 */

import { describe, expect, it } from "vitest";

import { OperationOutcomeError } from "../../../types/errors";
import {
  severityColour,
  severityLabel,
  toDisplayIssues,
} from "../errorPresentation";

import type { OperationOutcome, OperationOutcomeIssue } from "fhir/r4";

/** The sentence shown when a failure carries nothing usable to display. */
const FALLBACK_TEXT =
  "The operation failed, but no further detail was provided.";

/**
 * Builds an OperationOutcome failure carrying the given issues.
 *
 * @param issues - The issues the outcome carries.
 * @returns A failure whose outcome carries those issues.
 */
function outcomeError(issues?: OperationOutcomeIssue[]): OperationOutcomeError {
  // The FHIR type requires the issue element, but a server is free to omit it,
  // which is one of the cases the derivation has to cope with.
  const outcome = {
    resourceType: "OperationOutcome",
    ...(issues === undefined ? {} : { issue: issues }),
  } as OperationOutcome;
  return new OperationOutcomeError(outcome, 400);
}

describe("toDisplayIssues", () => {
  // An outcome with a single issue produces a single line.
  it("derives one issue from an outcome carrying one issue", () => {
    const error = outcomeError([
      {
        severity: "error",
        code: "processing",
        diagnostics: "Cannot write to existing path",
      },
    ]);

    expect(toDisplayIssues(error)).toEqual([
      { severity: "error", text: "Cannot write to existing path" },
    ]);
  });

  // Multiple issues each produce a line, in the order the outcome gives them,
  // and each keeps its own severity (FR-008).
  it("derives one issue per outcome issue, preserving order and severity", () => {
    const error = outcomeError([
      { severity: "error", code: "processing", diagnostics: "First problem" },
      {
        severity: "warning",
        code: "informational",
        diagnostics: "Second problem",
      },
      {
        severity: "information",
        code: "informational",
        diagnostics: "Third note",
      },
    ]);

    expect(toDisplayIssues(error)).toEqual([
      { severity: "error", text: "First problem" },
      { severity: "warning", text: "Second problem" },
      { severity: "information", text: "Third note" },
    ]);
  });

  // Diagnostics is the preferred text when present.
  it("prefers the issue diagnostics", () => {
    const error = outcomeError([
      {
        severity: "error",
        code: "processing",
        diagnostics: "From diagnostics",
        details: { text: "From details" },
      },
    ]);

    expect(toDisplayIssues(error)[0].text).toBe("From diagnostics");
  });

  // Without diagnostics, the coded detail text is used.
  it("falls back from diagnostics to the details text", () => {
    const error = outcomeError([
      {
        severity: "error",
        code: "processing",
        details: { text: "From details" },
      },
    ]);

    expect(toDisplayIssues(error)[0].text).toBe("From details");
  });

  // With neither, the issue code is the last resort.
  it("falls back from the details text to the issue code", () => {
    const error = outcomeError([{ severity: "error", code: "processing" }]);

    expect(toDisplayIssues(error)[0].text).toBe("processing");
  });

  // A server may omit a severity the FHIR type declares as required.
  it("defaults a missing severity to error", () => {
    const error = outcomeError([
      {
        code: "processing",
        diagnostics: "No severity given",
      } as OperationOutcomeIssue,
    ]);

    expect(toDisplayIssues(error)).toEqual([
      { severity: "error", text: "No severity given" },
    ]);
  });

  // An outcome with no issues must still produce something to display (FR-010).
  it("returns the fallback for an outcome with an empty issue list", () => {
    const error = outcomeError([]);

    expect(toDisplayIssues(error)).toEqual([
      { severity: "error", text: FALLBACK_TEXT },
    ]);
  });

  // An outcome with no issue element at all is treated the same way.
  it("returns the fallback for an outcome with no issue element", () => {
    const error = outcomeError(undefined);

    expect(toDisplayIssues(error)).toEqual([
      { severity: "error", text: FALLBACK_TEXT },
    ]);
  });

  // Issues that carry no usable text anywhere are discarded, leaving the
  // fallback rather than a row of empty text.
  it("returns the fallback when no issue carries usable text", () => {
    const error = outcomeError([{ severity: "error", code: "   " }]);

    expect(toDisplayIssues(error)).toEqual([
      { severity: "error", text: FALLBACK_TEXT },
    ]);
  });

  // Usable issues survive alongside unusable ones, including an issue that
  // omits the code the FHIR type declares as required.
  it("keeps only the issues that carry usable text", () => {
    const error = outcomeError([
      { severity: "error" } as OperationOutcomeIssue,
      {
        severity: "warning",
        code: "processing",
        diagnostics: "Still worth showing",
      },
    ]);

    expect(toDisplayIssues(error)).toEqual([
      { severity: "warning", text: "Still worth showing" },
    ]);
  });

  // A failure that is not an outcome error shows its message at error severity.
  it("derives a single error issue from a plain failure", () => {
    expect(toDisplayIssues(new Error("Query failed"))).toEqual([
      { severity: "error", text: "Query failed" },
    ]);
  });

  // A failure with no message still produces something to display (FR-010).
  it("returns the fallback for a plain failure with an empty message", () => {
    expect(toDisplayIssues(new Error(""))).toEqual([
      { severity: "error", text: FALLBACK_TEXT },
    ]);
  });

  // Whitespace is not a message.
  it("returns the fallback for a plain failure with a whitespace-only message", () => {
    expect(toDisplayIssues(new Error("   \n  "))).toEqual([
      { severity: "error", text: FALLBACK_TEXT },
    ]);
  });
});

describe("severityLabel", () => {
  // Each severity has its own label, so that the rows are distinguishable.
  it.each([
    ["fatal", "Fatal"],
    ["error", "Error"],
    ["warning", "Warning"],
    ["information", "Info"],
  ] as const)("labels %s as %s", (severity, label) => {
    expect(severityLabel(severity)).toBe(label);
  });
});

describe("severityColour", () => {
  // Fatal and error share the callout's own red; the lesser severities do not,
  // so that a warning remains distinguishable from an error (FR-008).
  it.each([
    ["fatal", "red"],
    ["error", "red"],
    ["warning", "orange"],
    ["information", "blue"],
  ] as const)("colours %s as %s", (severity, colour) => {
    expect(severityColour(severity)).toBe(colour);
  });
});

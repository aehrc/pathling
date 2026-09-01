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
 * Derives the displayable content of a failure. Kept free of any component so
 * that the derivation can be tested directly, and so that every surface that
 * displays a failure derives it the same way.
 *
 * @author John Grimes
 */

import { OperationOutcomeError } from "../../types/errors";

import type { OperationOutcomeIssue } from "fhir/r4";

/** One line of a displayed failure. Derived from a failure, never authored directly. */
export interface DisplayIssue {
  /**
   * How the issue is distinguished from the others. Mirrors FHIR issue
   * severity, so that the derivation stays free of presentation values.
   */
  severity: "fatal" | "error" | "warning" | "information";
  /** The text shown to the user. Never empty. */
  text: string;
}

/**
 * Shown when a failure carries nothing usable, so that the presentation is
 * never empty.
 */
const FALLBACK_TEXT =
  "The operation failed, but no further detail was provided.";

/**
 * Derives the text of one outcome issue, preferring the human-readable forms
 * over the code.
 *
 * @param issue - The outcome issue to describe.
 * @returns The text to display, or an empty string if the issue carries none.
 */
function issueText(issue: OperationOutcomeIssue): string {
  return (issue.diagnostics ?? issue.details?.text ?? issue.code ?? "").trim();
}

/**
 * Turns a failure into the lines to display. Always returns at least one issue,
 * so that a caller can render the result without checking for emptiness.
 *
 * An outcome failure contributes one issue per outcome issue, in the order
 * given; any other failure contributes a single issue carrying its message. A
 * failure that carries no usable text contributes a single fallback issue.
 *
 * @param error - The failure to describe.
 * @returns One issue per line to display, never empty.
 * @example
 * const issues = toDisplayIssues(error);
 * return <ErrorCallout issues={issues} />;
 */
export function toDisplayIssues(error: Error): DisplayIssue[] {
  if (error instanceof OperationOutcomeError) {
    const issues = (error.operationOutcome.issue ?? [])
      .map((issue) => ({
        severity: issue.severity ?? "error",
        text: issueText(issue),
      }))
      .filter((issue) => issue.text.length > 0);
    if (issues.length > 0) {
      return issues;
    }
  } else if (error.message.trim().length > 0) {
    return [{ severity: "error", text: error.message }];
  }
  return [{ severity: "error", text: FALLBACK_TEXT }];
}

/**
 * Maps an issue severity to the label shown in its badge.
 *
 * @param severity - The issue severity.
 * @returns The label shown in the severity badge.
 */
export function severityLabel(severity: DisplayIssue["severity"]): string {
  switch (severity) {
    case "fatal":
      return "Fatal";
    case "error":
      return "Error";
    case "warning":
      return "Warning";
    case "information":
      return "Info";
  }
}

/**
 * Maps an issue severity to the colour that distinguishes it from the other
 * severities.
 *
 * @param severity - The issue severity.
 * @returns The Radix colour used for the issue.
 */
export function severityColour(
  severity: DisplayIssue["severity"],
): "red" | "orange" | "blue" {
  switch (severity) {
    case "fatal":
    case "error":
      return "red";
    case "warning":
      return "orange";
    case "information":
      return "blue";
  }
}

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
 * Component for displaying errors with enhanced support for FHIR OperationOutcome.
 * When given an OperationOutcomeError, displays each issue with severity indicators.
 * Falls back to displaying the error message for generic errors.
 *
 * @author John Grimes
 */

import { Badge, Flex, Text } from "@radix-ui/themes";

import { OperationOutcomeError } from "../../types/errors";

import type { OperationOutcomeIssue } from "fhir/r4";

interface OperationOutcomeDisplayProps {
  error: Error;
}

/**
 * Maps OperationOutcome severity to display label.
 *
 * @param severity - The issue severity code.
 * @returns Human-readable label for the severity.
 */
function getSeverityLabel(
  severity: OperationOutcomeIssue["severity"],
): string {
  switch (severity) {
    case "fatal":
      return "Fatal";
    case "error":
      return "Error";
    case "warning":
      return "Warning";
    case "information":
      return "Info";
    default:
      return "Error";
  }
}

/**
 * Maps OperationOutcome severity to Radix color.
 *
 * @param severity - The issue severity code.
 * @returns Radix color name for the severity.
 */
function getSeverityColor(
  severity: OperationOutcomeIssue["severity"],
): "red" | "orange" | "blue" {
  switch (severity) {
    case "fatal":
    case "error":
      return "red";
    case "warning":
      return "orange";
    case "information":
      return "blue";
    default:
      return "red";
  }
}

/**
 * Gets the display text for an issue.
 *
 * @param issue - The OperationOutcome issue.
 * @returns The diagnostics message or the code if diagnostics is missing.
 */
function getIssueText(issue: OperationOutcomeIssue): string {
  return issue.diagnostics ?? issue.code;
}

/**
 * Displays an error with enhanced support for FHIR OperationOutcome errors.
 *
 * @param props - Component props.
 * @param props.error - The error to display.
 * @returns The rendered error display component.
 */
export function OperationOutcomeDisplay({
  error,
}: Readonly<OperationOutcomeDisplayProps>) {
  // Check if this is an OperationOutcomeError with structured data.
  if (error instanceof OperationOutcomeError) {
    const { operationOutcome } = error;
    const issues = operationOutcome.issue ?? [];

    return (
      <Flex direction="column" gap="1">
        {issues.map((issue, index) => {
          // Create a key from issue properties since OperationOutcome issues have no unique ID.
          const key = `${issue.severity}-${issue.code}-${index}`;
          return (
            <Flex key={key} align="start" gap="2">
              <Badge size="1" color={getSeverityColor(issue.severity)}>
                {getSeverityLabel(issue.severity)}
              </Badge>
              <Text size="2" color={getSeverityColor(issue.severity)}>
                {getIssueText(issue)}
              </Text>
            </Flex>
          );
        })}
      </Flex>
    );
  }

  // Fall back to displaying the error message for generic errors.
  return (
    <Flex align="start" gap="2">
      <Badge size="1" color="red">
        Error
      </Badge>
      <Text size="2" color="red">
        {error.message}
      </Text>
    </Flex>
  );
}

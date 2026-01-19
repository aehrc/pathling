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
 * Unit tests for custom error types.
 *
 * @author John Grimes
 */

import { describe, expect, it } from "vitest";

import {
  NotFoundError,
  OperationOutcomeError,
  UnauthorizedError,
} from "../errors";

import type { OperationOutcome } from "fhir/r4";

describe("UnauthorizedError", () => {
  it("creates error with default message", () => {
    const error = new UnauthorizedError();
    expect(error.message).toBe("Session expired or unauthorized");
    expect(error.name).toBe("UnauthorizedError");
  });

  it("creates error with custom message", () => {
    const error = new UnauthorizedError("Custom message");
    expect(error.message).toBe("Custom message");
  });
});

describe("NotFoundError", () => {
  it("creates error with default message", () => {
    const error = new NotFoundError();
    expect(error.message).toBe("Resource not found");
    expect(error.name).toBe("NotFoundError");
  });

  it("creates error with custom message", () => {
    const error = new NotFoundError("Patient/123 not found");
    expect(error.message).toBe("Patient/123 not found");
  });
});

describe("OperationOutcomeError", () => {
  // A standard OperationOutcome with a single error issue.
  const singleIssueOutcome: OperationOutcome = {
    resourceType: "OperationOutcome",
    issue: [
      {
        severity: "error",
        code: "processing",
        diagnostics: "[DELTA_PATH_EXISTS] Cannot write to existing path",
      },
    ],
  };

  // An OperationOutcome with multiple issues.
  const multiIssueOutcome: OperationOutcome = {
    resourceType: "OperationOutcome",
    issue: [
      {
        severity: "error",
        code: "processing",
        diagnostics: "Primary error message",
      },
      {
        severity: "warning",
        code: "informational",
        diagnostics: "Additional warning",
      },
    ],
  };

  // An OperationOutcome with no diagnostics.
  const nodiagnosticsOutcome: OperationOutcome = {
    resourceType: "OperationOutcome",
    issue: [
      {
        severity: "error",
        code: "exception",
      },
    ],
  };

  // An OperationOutcome with empty issues array.
  const emptyIssuesOutcome: OperationOutcome = {
    resourceType: "OperationOutcome",
    issue: [],
  };

  it("creates error with operationOutcome and status code", () => {
    const error = new OperationOutcomeError(singleIssueOutcome, 400);

    expect(error.operationOutcome).toBe(singleIssueOutcome);
    expect(error.status).toBe(400);
    expect(error.name).toBe("OperationOutcomeError");
  });

  it("formats message with diagnostics from first issue", () => {
    const error = new OperationOutcomeError(singleIssueOutcome, 400);

    expect(error.message).toBe(
      "Request failed: 400 - [DELTA_PATH_EXISTS] Cannot write to existing path",
    );
  });

  it("formats message with context when provided", () => {
    const error = new OperationOutcomeError(
      singleIssueOutcome,
      400,
      "Import kick-off",
    );

    expect(error.message).toBe(
      "Import kick-off failed: 400 - [DELTA_PATH_EXISTS] Cannot write to existing path",
    );
  });

  it("uses first issue diagnostics when multiple issues present", () => {
    const error = new OperationOutcomeError(multiIssueOutcome, 422);

    expect(error.message).toBe("Request failed: 422 - Primary error message");
    // But all issues are preserved in the operationOutcome.
    expect(error.operationOutcome.issue).toHaveLength(2);
  });

  it("uses 'Unknown error' when diagnostics is missing", () => {
    const error = new OperationOutcomeError(nodiagnosticsOutcome, 500);

    expect(error.message).toBe("Request failed: 500 - Unknown error");
  });

  it("uses 'Unknown error' when issues array is empty", () => {
    const error = new OperationOutcomeError(emptyIssuesOutcome, 500);

    expect(error.message).toBe("Request failed: 500 - Unknown error");
  });

  it("is instanceof Error", () => {
    const error = new OperationOutcomeError(singleIssueOutcome, 400);

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(OperationOutcomeError);
  });

  it("preserves stack trace", () => {
    const error = new OperationOutcomeError(singleIssueOutcome, 400);

    expect(error.stack).toBeDefined();
    expect(error.stack).toContain("OperationOutcomeError");
  });
});

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
 * Tests for the OperationOutcomeDisplay component.
 *
 * @author John Grimes
 */

import { describe, expect, it } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { OperationOutcomeError } from "../../../types/errors";
import { OperationOutcomeDisplay } from "../OperationOutcomeDisplay";

import type { OperationOutcome } from "fhir/r4";

describe("OperationOutcomeDisplay", () => {
  describe("with OperationOutcomeError", () => {
    it("displays single issue with diagnostics", () => {
      // An error with a single issue.
      const outcome: OperationOutcome = {
        resourceType: "OperationOutcome",
        issue: [
          {
            severity: "error",
            code: "processing",
            diagnostics: "[DELTA_PATH_EXISTS] Cannot write to existing path",
          },
        ],
      };
      const error = new OperationOutcomeError(outcome, 400, "Import kick-off");

      render(<OperationOutcomeDisplay error={error} />);

      expect(
        screen.getByText("[DELTA_PATH_EXISTS] Cannot write to existing path"),
      ).toBeInTheDocument();
    });

    it("displays multiple issues", () => {
      // An error with multiple issues.
      const outcome: OperationOutcome = {
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
      const error = new OperationOutcomeError(outcome, 422);

      render(<OperationOutcomeDisplay error={error} />);

      expect(screen.getByText("Primary error message")).toBeInTheDocument();
      expect(screen.getByText("Additional warning")).toBeInTheDocument();
    });

    it("displays severity indicator for error severity", () => {
      // Verify error severity is displayed.
      const outcome: OperationOutcome = {
        resourceType: "OperationOutcome",
        issue: [
          {
            severity: "error",
            code: "processing",
            diagnostics: "Error level issue",
          },
        ],
      };
      const error = new OperationOutcomeError(outcome, 400);

      render(<OperationOutcomeDisplay error={error} />);

      // The component should show the severity.
      expect(screen.getByText("Error")).toBeInTheDocument();
    });

    it("displays severity indicator for warning severity", () => {
      // Verify warning severity is displayed.
      const outcome: OperationOutcome = {
        resourceType: "OperationOutcome",
        issue: [
          {
            severity: "warning",
            code: "informational",
            diagnostics: "Warning level issue",
          },
        ],
      };
      const error = new OperationOutcomeError(outcome, 400);

      render(<OperationOutcomeDisplay error={error} />);

      expect(screen.getByText("Warning")).toBeInTheDocument();
    });

    it("displays severity indicator for fatal severity", () => {
      // Verify fatal severity is displayed.
      const outcome: OperationOutcome = {
        resourceType: "OperationOutcome",
        issue: [
          {
            severity: "fatal",
            code: "exception",
            diagnostics: "Fatal level issue",
          },
        ],
      };
      const error = new OperationOutcomeError(outcome, 500);

      render(<OperationOutcomeDisplay error={error} />);

      expect(screen.getByText("Fatal")).toBeInTheDocument();
    });

    it("displays severity indicator for information severity", () => {
      // Verify information severity is displayed.
      const outcome: OperationOutcome = {
        resourceType: "OperationOutcome",
        issue: [
          {
            severity: "information",
            code: "informational",
            diagnostics: "Informational issue",
          },
        ],
      };
      const error = new OperationOutcomeError(outcome, 200);

      render(<OperationOutcomeDisplay error={error} />);

      expect(screen.getByText("Info")).toBeInTheDocument();
    });

    it("handles issue without diagnostics", () => {
      // An issue without diagnostics should show code instead.
      const outcome: OperationOutcome = {
        resourceType: "OperationOutcome",
        issue: [
          {
            severity: "error",
            code: "processing",
          },
        ],
      };
      const error = new OperationOutcomeError(outcome, 400);

      render(<OperationOutcomeDisplay error={error} />);

      // Should show the code when diagnostics is missing.
      expect(screen.getByText("processing")).toBeInTheDocument();
    });
  });

  describe("with generic Error", () => {
    it("displays error message for generic Error", () => {
      // A standard Error should fall back to showing the message.
      const error = new Error("Something went wrong");

      render(<OperationOutcomeDisplay error={error} />);

      expect(screen.getByText("Something went wrong")).toBeInTheDocument();
    });

    it("displays error severity for generic Error", () => {
      // Generic errors should be displayed as errors.
      const error = new Error("Generic error");

      render(<OperationOutcomeDisplay error={error} />);

      expect(screen.getByText("Error")).toBeInTheDocument();
    });
  });
});

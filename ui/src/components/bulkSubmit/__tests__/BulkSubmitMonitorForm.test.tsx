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
 * Tests for the BulkSubmitMonitorForm component.
 *
 * This test suite verifies that the BulkSubmitMonitorForm correctly handles
 * user input, validates form fields, and calls the onMonitor callback with
 * the correct parameters.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { BulkSubmitMonitorForm } from "../BulkSubmitMonitorForm";

describe("BulkSubmitMonitorForm", () => {
  const defaultOnMonitor = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("Rendering", () => {
    it("displays the form heading", () => {
      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      expect(screen.getByRole("heading", { name: /monitor submission/i })).toBeInTheDocument();
    });

    it("displays description text", () => {
      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      expect(
        screen.getByText("Enter the details of an existing submission to monitor its status."),
      ).toBeInTheDocument();
    });

    it("displays submitter system field", () => {
      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      expect(screen.getByText("Submitter system")).toBeInTheDocument();
      expect(
        screen.getByPlaceholderText("e.g., http://example.org/submitters"),
      ).toBeInTheDocument();
    });

    it("displays submitter value field", () => {
      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      expect(screen.getByText("Submitter value")).toBeInTheDocument();
      expect(screen.getByPlaceholderText("e.g., my-submitter-id")).toBeInTheDocument();
    });

    it("displays submission ID field", () => {
      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      expect(screen.getByText("Submission ID")).toBeInTheDocument();
      expect(
        screen.getByPlaceholderText("e.g., 550e8400-e29b-41d4-a716-446655440000"),
      ).toBeInTheDocument();
    });

    it("displays submit button with default text", () => {
      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      expect(screen.getByRole("button", { name: /start monitoring/i })).toBeInTheDocument();
    });

    it("displays submit button with loading text when submitting", () => {
      render(
        <BulkSubmitMonitorForm onMonitor={defaultOnMonitor} isSubmitting={true} disabled={false} />,
      );

      expect(screen.getByRole("button", { name: /starting.../i })).toBeInTheDocument();
    });
  });

  describe("Form validation", () => {
    it("disables submit button when submitter system is empty", () => {
      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      const submitButton = screen.getByRole("button", { name: /start monitoring/i });
      expect(submitButton).toBeDisabled();
    });

    it("disables submit button when submitter value is empty", async () => {
      const user = userEvent.setup();

      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      await user.type(
        screen.getByPlaceholderText("e.g., http://example.org/submitters"),
        "http://example.org",
      );
      await user.type(
        screen.getByPlaceholderText("e.g., 550e8400-e29b-41d4-a716-446655440000"),
        "submission-123",
      );

      const submitButton = screen.getByRole("button", { name: /start monitoring/i });
      expect(submitButton).toBeDisabled();
    });

    it("disables submit button when submission ID is empty", async () => {
      const user = userEvent.setup();

      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      await user.type(
        screen.getByPlaceholderText("e.g., http://example.org/submitters"),
        "http://example.org",
      );
      await user.type(screen.getByPlaceholderText("e.g., my-submitter-id"), "submitter-1");

      const submitButton = screen.getByRole("button", { name: /start monitoring/i });
      expect(submitButton).toBeDisabled();
    });

    it("enables submit button when all fields are filled", async () => {
      const user = userEvent.setup();

      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      await user.type(
        screen.getByPlaceholderText("e.g., http://example.org/submitters"),
        "http://example.org",
      );
      await user.type(screen.getByPlaceholderText("e.g., my-submitter-id"), "submitter-1");
      await user.type(
        screen.getByPlaceholderText("e.g., 550e8400-e29b-41d4-a716-446655440000"),
        "submission-123",
      );

      const submitButton = screen.getByRole("button", { name: /start monitoring/i });
      expect(submitButton).not.toBeDisabled();
    });

    it("disables submit button when disabled prop is true", async () => {
      const user = userEvent.setup();

      render(
        <BulkSubmitMonitorForm onMonitor={defaultOnMonitor} isSubmitting={false} disabled={true} />,
      );

      await user.type(
        screen.getByPlaceholderText("e.g., http://example.org/submitters"),
        "http://example.org",
      );
      await user.type(screen.getByPlaceholderText("e.g., my-submitter-id"), "submitter-1");
      await user.type(
        screen.getByPlaceholderText("e.g., 550e8400-e29b-41d4-a716-446655440000"),
        "submission-123",
      );

      const submitButton = screen.getByRole("button", { name: /start monitoring/i });
      expect(submitButton).toBeDisabled();
    });

    it("disables submit button when isSubmitting is true", async () => {
      const user = userEvent.setup();

      render(
        <BulkSubmitMonitorForm onMonitor={defaultOnMonitor} isSubmitting={true} disabled={false} />,
      );

      await user.type(
        screen.getByPlaceholderText("e.g., http://example.org/submitters"),
        "http://example.org",
      );
      await user.type(screen.getByPlaceholderText("e.g., my-submitter-id"), "submitter-1");
      await user.type(
        screen.getByPlaceholderText("e.g., 550e8400-e29b-41d4-a716-446655440000"),
        "submission-123",
      );

      const submitButton = screen.getByRole("button", { name: /starting.../i });
      expect(submitButton).toBeDisabled();
    });

    it("trims whitespace when validating fields", async () => {
      const user = userEvent.setup();

      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      // Enter only whitespace.
      await user.type(screen.getByPlaceholderText("e.g., http://example.org/submitters"), "   ");
      await user.type(screen.getByPlaceholderText("e.g., my-submitter-id"), "   ");
      await user.type(
        screen.getByPlaceholderText("e.g., 550e8400-e29b-41d4-a716-446655440000"),
        "   ",
      );

      const submitButton = screen.getByRole("button", { name: /start monitoring/i });
      expect(submitButton).toBeDisabled();
    });
  });

  describe("Form submission", () => {
    it("calls onMonitor with correct parameters when form is submitted", async () => {
      const user = userEvent.setup();

      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      await user.type(
        screen.getByPlaceholderText("e.g., http://example.org/submitters"),
        "http://my-system.org",
      );
      await user.type(screen.getByPlaceholderText("e.g., my-submitter-id"), "submitter-abc");
      await user.type(
        screen.getByPlaceholderText("e.g., 550e8400-e29b-41d4-a716-446655440000"),
        "submission-xyz",
      );

      const submitButton = screen.getByRole("button", { name: /start monitoring/i });
      await user.click(submitButton);

      expect(defaultOnMonitor).toHaveBeenCalledWith("submission-xyz", {
        system: "http://my-system.org",
        value: "submitter-abc",
      });
    });

    it("calls onMonitor only once per click", async () => {
      const user = userEvent.setup();

      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      await user.type(
        screen.getByPlaceholderText("e.g., http://example.org/submitters"),
        "http://system.org",
      );
      await user.type(screen.getByPlaceholderText("e.g., my-submitter-id"), "submitter");
      await user.type(
        screen.getByPlaceholderText("e.g., 550e8400-e29b-41d4-a716-446655440000"),
        "submission",
      );

      const submitButton = screen.getByRole("button", { name: /start monitoring/i });
      await user.click(submitButton);

      expect(defaultOnMonitor).toHaveBeenCalledTimes(1);
    });
  });

  describe("Input handling", () => {
    it("updates submitter system field on input", async () => {
      const user = userEvent.setup();

      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      const input = screen.getByPlaceholderText("e.g., http://example.org/submitters");
      await user.type(input, "http://test.org");

      expect(input).toHaveValue("http://test.org");
    });

    it("updates submitter value field on input", async () => {
      const user = userEvent.setup();

      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      const input = screen.getByPlaceholderText("e.g., my-submitter-id");
      await user.type(input, "my-value");

      expect(input).toHaveValue("my-value");
    });

    it("updates submission ID field on input", async () => {
      const user = userEvent.setup();

      render(
        <BulkSubmitMonitorForm
          onMonitor={defaultOnMonitor}
          isSubmitting={false}
          disabled={false}
        />,
      );

      const input = screen.getByPlaceholderText("e.g., 550e8400-e29b-41d4-a716-446655440000");
      await user.type(input, "my-submission-id");

      expect(input).toHaveValue("my-submission-id");
    });
  });
});

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
 * Tests for the SessionExpiredDialog component.
 *
 * This test suite verifies that the dialog displays when the session has
 * expired, stays open while an authorisation attempt is under way, reports a
 * failure to initiate authorisation inside itself, and can be dismissed without
 * initiating anything.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, waitFor } from "../../../test/testUtils";
import { SessionExpiredDialog } from "../SessionExpiredDialog";

// Mock state for auth context.
let mockSessionExpired = false;
const mockSetSessionExpired = vi.fn();

// Mock the auth context.
vi.mock("../../../contexts/AuthContext", () => ({
  useAuth: () => ({
    sessionExpired: mockSessionExpired,
    setSessionExpired: mockSetSessionExpired,
  }),
}));

// Mock config.
vi.mock("../../../config", () => ({
  config: {
    fhirBaseUrl: "https://fhir.example.org/fhir",
  },
}));

// Mock initiateAuth function.
const mockInitiateAuth = vi.fn();
vi.mock("../../../services/auth", () => ({
  initiateAuth: (url: string) => mockInitiateAuth(url),
}));

/**
 * Creates a promise that the test controls, so an authorisation attempt can be
 * held pending while assertions are made.
 *
 * @returns The promise and the functions that settle it.
 */
function deferred(): {
  promise: Promise<void>;
  resolve: () => void;
  reject: (reason: unknown) => void;
} {
  let resolve!: () => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("SessionExpiredDialog", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSessionExpired = false;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("Rendering", () => {
    it("does not render content when sessionExpired is false", () => {
      mockSessionExpired = false;

      render(<SessionExpiredDialog />);

      expect(screen.queryByText("Session expired")).not.toBeInTheDocument();
    });

    it("renders dialog content when sessionExpired is true", () => {
      mockSessionExpired = true;

      render(<SessionExpiredDialog />);

      expect(screen.getByText("Session expired")).toBeInTheDocument();
      expect(
        screen.getByText("Your session has expired. Please log in again to continue working."),
      ).toBeInTheDocument();
    });

    it("shows Dismiss button when dialog is open", () => {
      mockSessionExpired = true;

      render(<SessionExpiredDialog />);

      expect(screen.getByRole("button", { name: /dismiss/i })).toBeInTheDocument();
    });

    it("shows Log in button when dialog is open", () => {
      mockSessionExpired = true;

      render(<SessionExpiredDialog />);

      expect(screen.getByRole("button", { name: /log in/i })).toBeInTheDocument();
    });

    it("shows no error before any attempt is made", () => {
      mockSessionExpired = true;

      render(<SessionExpiredDialog />);

      expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    });
  });

  describe("Dismiss action", () => {
    // FR-006: dismissing closes the dialog and initiates nothing.
    it("closes the dialog without initiating authorisation", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;

      render(<SessionExpiredDialog />);

      await user.click(screen.getByRole("button", { name: /dismiss/i }));

      expect(mockSetSessionExpired).toHaveBeenCalledWith(false);
      expect(mockInitiateAuth).not.toHaveBeenCalled();
    });
  });

  describe("Login action", () => {
    it("calls initiateAuth with FHIR base URL when Log in is clicked", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;
      mockInitiateAuth.mockResolvedValue(undefined);

      render(<SessionExpiredDialog />);

      await user.click(screen.getByRole("button", { name: /log in/i }));

      expect(mockInitiateAuth).toHaveBeenCalledWith("https://fhir.example.org/fhir");
    });

    // FR-005: the dialog must not close itself when the attempt starts.
    it("leaves the dialog open when Log in is clicked", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;
      const attempt = deferred();
      mockInitiateAuth.mockReturnValue(attempt.promise);

      render(<SessionExpiredDialog />);

      await user.click(screen.getByRole("button", { name: /log in/i }));

      expect(mockSetSessionExpired).not.toHaveBeenCalled();
      expect(screen.getByText("Session expired")).toBeInTheDocument();

      attempt.resolve();
    });

    // FR-001, FR-002: both controls are locked out while the attempt runs.
    it("disables both buttons and shows a pending indication while the attempt is under way", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;
      const attempt = deferred();
      mockInitiateAuth.mockReturnValue(attempt.promise);

      render(<SessionExpiredDialog />);

      const loginButton = screen.getByRole("button", { name: /log in/i });
      const dismissButton = screen.getByRole("button", { name: /dismiss/i });
      await user.click(loginButton);

      await waitFor(() => {
        expect(loginButton).toBeDisabled();
      });
      expect(dismissButton).toBeDisabled();
      expect(document.querySelector(".rt-Spinner")).toBeInTheDocument();

      attempt.resolve();
    });

    // FR-003, FR-005: the failure is reported inside the dialog, which stays open.
    it("shows the failure inside the dialog and keeps it open", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;
      mockInitiateAuth.mockRejectedValue(new Error("Login failed"));

      render(<SessionExpiredDialog />);

      await user.click(screen.getByRole("button", { name: /log in/i }));

      const alert = await screen.findByRole("alert");
      expect(alert).toHaveTextContent("Login failed");
      expect(screen.getByText("Session expired")).toBeInTheDocument();
      expect(mockSetSessionExpired).not.toHaveBeenCalled();
    });

    // FR-003: a rejection that is not an Error still produces a message.
    it("shows a fallback message when the failure is not an Error", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;
      mockInitiateAuth.mockRejectedValue("string error");

      render(<SessionExpiredDialog />);

      await user.click(screen.getByRole("button", { name: /log in/i }));

      const alert = await screen.findByRole("alert");
      expect(alert).toHaveTextContent("Authentication failed");
    });

    // FR-004: both controls become usable again so the attempt can be retried.
    it("re-enables both buttons after a failed attempt", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;
      mockInitiateAuth.mockRejectedValue(new Error("Login failed"));

      render(<SessionExpiredDialog />);

      const loginButton = screen.getByRole("button", { name: /log in/i });
      const dismissButton = screen.getByRole("button", { name: /dismiss/i });
      await user.click(loginButton);

      await screen.findByRole("alert");
      expect(loginButton).toBeEnabled();
      expect(dismissButton).toBeEnabled();
    });
  });
});

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
 * This test suite verifies that the SessionExpiredDialog correctly displays
 * when session is expired, handles user interactions for dismiss and login
 * actions, and properly manages auth state.
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
const mockSetLoading = vi.fn();
const mockSetError = vi.fn();

// Mock the auth context.
vi.mock("../../../contexts/AuthContext", () => ({
  useAuth: () => ({
    sessionExpired: mockSessionExpired,
    setSessionExpired: mockSetSessionExpired,
    setLoading: mockSetLoading,
    setError: mockSetError,
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
  });

  describe("Dismiss action", () => {
    it("calls setSessionExpired with false when Dismiss is clicked", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;

      render(<SessionExpiredDialog />);

      const dismissButton = screen.getByRole("button", { name: /dismiss/i });
      await user.click(dismissButton);

      expect(mockSetSessionExpired).toHaveBeenCalledWith(false);
    });
  });

  describe("Login action", () => {
    it("clears session expired state when Log in is clicked", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;
      mockInitiateAuth.mockResolvedValue(undefined);

      render(<SessionExpiredDialog />);

      const loginButton = screen.getByRole("button", { name: /log in/i });
      await user.click(loginButton);

      expect(mockSetSessionExpired).toHaveBeenCalledWith(false);
    });

    it("sets loading state when Log in is clicked", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;
      mockInitiateAuth.mockResolvedValue(undefined);

      render(<SessionExpiredDialog />);

      const loginButton = screen.getByRole("button", { name: /log in/i });
      await user.click(loginButton);

      expect(mockSetLoading).toHaveBeenCalledWith(true);
    });

    it("calls initiateAuth with FHIR base URL when Log in is clicked", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;
      mockInitiateAuth.mockResolvedValue(undefined);

      render(<SessionExpiredDialog />);

      const loginButton = screen.getByRole("button", { name: /log in/i });
      await user.click(loginButton);

      expect(mockInitiateAuth).toHaveBeenCalledWith("https://fhir.example.org/fhir");
    });

    it("calls setError when initiateAuth fails with Error", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;
      mockInitiateAuth.mockRejectedValue(new Error("Login failed"));

      render(<SessionExpiredDialog />);

      const loginButton = screen.getByRole("button", { name: /log in/i });
      await user.click(loginButton);

      await waitFor(() => {
        expect(mockSetError).toHaveBeenCalledWith("Login failed");
      });
    });

    it("calls setError with generic message when initiateAuth fails with non-Error", async () => {
      const user = userEvent.setup();
      mockSessionExpired = true;
      mockInitiateAuth.mockRejectedValue("string error");

      render(<SessionExpiredDialog />);

      const loginButton = screen.getByRole("button", { name: /log in/i });
      await user.click(loginButton);

      await waitFor(() => {
        expect(mockSetError).toHaveBeenCalledWith("Authentication failed");
      });
    });
  });

  describe("Dialog state management", () => {
    it("passes sessionExpired to onOpenChange", () => {
      mockSessionExpired = true;

      render(<SessionExpiredDialog />);

      // The dialog should be open (content visible).
      expect(screen.getByText("Session expired")).toBeInTheDocument();
    });
  });
});

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
 * Tests for the LoginRequired component.
 *
 * This test suite verifies that the LoginRequired component correctly displays
 * the login prompt, handles login button clicks, and shows appropriate server
 * name in the button text.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, waitFor } from "../../../test/testUtils";
import { LoginRequired } from "../LoginRequired";

// Mock state for auth context.
const mockSetLoading = vi.fn();
const mockSetError = vi.fn();

// Mock the auth context.
vi.mock("../../../contexts/AuthContext", () => ({
  useAuth: () => ({
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

// Mock server capabilities hook.
let mockCapabilities: { serverName?: string } | undefined = undefined;
vi.mock("../../../hooks/useServerCapabilities", () => ({
  useServerCapabilities: () => ({
    data: mockCapabilities,
  }),
}));

// Mock initiateAuth function.
const mockInitiateAuth = vi.fn();
vi.mock("../../../services/auth", () => ({
  initiateAuth: (url: string) => mockInitiateAuth(url),
}));

// Mock SessionExpiredDialog to simplify testing.
vi.mock("../SessionExpiredDialog", () => ({
  SessionExpiredDialog: () => <div data-testid="session-expired-dialog" />,
}));

describe("LoginRequired", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockCapabilities = undefined;
    // Mock window.location.hostname.
    Object.defineProperty(window, "location", {
      value: { hostname: "localhost" },
      writable: true,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("Rendering", () => {
    it("displays login required callout message", () => {
      render(<LoginRequired />);

      expect(
        screen.getByText("You need to login before you can use this page."),
      ).toBeInTheDocument();
    });

    it("displays login button with server name from capabilities", () => {
      mockCapabilities = { serverName: "Test FHIR Server" };

      render(<LoginRequired />);

      expect(
        screen.getByRole("button", { name: /login to test fhir server/i }),
      ).toBeInTheDocument();
    });

    it("displays login button with hostname when server name is not available", () => {
      mockCapabilities = undefined;

      render(<LoginRequired />);

      expect(screen.getByRole("button", { name: /login to localhost/i })).toBeInTheDocument();
    });

    it("renders SessionExpiredDialog", () => {
      render(<LoginRequired />);

      expect(screen.getByTestId("session-expired-dialog")).toBeInTheDocument();
    });
  });

  describe("Login action", () => {
    it("calls setLoading and initiateAuth when login button is clicked", async () => {
      const user = userEvent.setup();
      mockInitiateAuth.mockResolvedValue(undefined);

      render(<LoginRequired />);

      const loginButton = screen.getByRole("button", { name: /login/i });
      await user.click(loginButton);

      expect(mockSetLoading).toHaveBeenCalledWith(true);
      expect(mockInitiateAuth).toHaveBeenCalledWith("https://fhir.example.org/fhir");
    });

    it("calls setError when initiateAuth fails", async () => {
      const user = userEvent.setup();
      mockInitiateAuth.mockRejectedValue(new Error("Auth failed"));

      render(<LoginRequired />);

      const loginButton = screen.getByRole("button", { name: /login/i });
      await user.click(loginButton);

      await waitFor(() => {
        expect(mockSetError).toHaveBeenCalledWith("Auth failed");
      });
    });

    it("sets generic error message when auth fails with non-Error", async () => {
      const user = userEvent.setup();
      mockInitiateAuth.mockRejectedValue("string error");

      render(<LoginRequired />);

      const loginButton = screen.getByRole("button", { name: /login/i });
      await user.click(loginButton);

      await waitFor(() => {
        expect(mockSetError).toHaveBeenCalledWith("Authentication failed");
      });
    });
  });
});

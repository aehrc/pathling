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
 * the login prompt, shows a pending indication while an authorisation attempt
 * is under way, and reports a failure to initiate authorisation.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, waitFor } from "../../../test/testUtils";
import { LoginRequired } from "../LoginRequired";

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

    it("shows no error before any attempt is made", () => {
      render(<LoginRequired />);

      expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    });
  });

  describe("Login action", () => {
    it("initiates authorisation with the FHIR base URL", async () => {
      const user = userEvent.setup();
      mockInitiateAuth.mockResolvedValue(undefined);

      render(<LoginRequired />);

      await user.click(screen.getByRole("button", { name: /login/i }));

      expect(mockInitiateAuth).toHaveBeenCalledWith("https://fhir.example.org/fhir");
    });

    // FR-001, FR-002: the button must show it is busy and refuse a second press.
    it("disables the button and shows a pending indication while the attempt is under way", async () => {
      const user = userEvent.setup();
      const attempt = deferred();
      mockInitiateAuth.mockReturnValue(attempt.promise);

      const { container } = render(<LoginRequired />);

      const loginButton = screen.getByRole("button", { name: /login/i });
      await user.click(loginButton);

      await waitFor(() => {
        expect(loginButton).toBeDisabled();
      });
      expect(container.querySelector(".rt-Spinner")).toBeInTheDocument();

      attempt.resolve();
    });

    // FR-002: repeated clicks must not start a second attempt.
    it("starts no second attempt while one is already pending", async () => {
      const user = userEvent.setup();
      const attempt = deferred();
      mockInitiateAuth.mockReturnValue(attempt.promise);

      render(<LoginRequired />);

      const loginButton = screen.getByRole("button", { name: /login/i });
      await user.click(loginButton);
      await waitFor(() => {
        expect(loginButton).toBeDisabled();
      });
      // A disabled button swallows the click, so drive the handler directly too.
      loginButton.click();

      expect(mockInitiateAuth).toHaveBeenCalledTimes(1);

      attempt.resolve();
    });

    // FR-003: the reason reported by the failure must be shown.
    it("displays the failure message when authorisation cannot be initiated", async () => {
      const user = userEvent.setup();
      mockInitiateAuth.mockRejectedValue(new Error("Auth failed"));

      render(<LoginRequired />);

      await user.click(screen.getByRole("button", { name: /login/i }));

      const alert = await screen.findByRole("alert");
      expect(alert).toHaveTextContent("Auth failed");
    });

    // FR-003: a rejection that is not an Error still produces a message.
    it("displays a fallback message when the failure is not an Error", async () => {
      const user = userEvent.setup();
      mockInitiateAuth.mockRejectedValue("string error");

      render(<LoginRequired />);

      await user.click(screen.getByRole("button", { name: /login/i }));

      const alert = await screen.findByRole("alert");
      expect(alert).toHaveTextContent("Authentication failed");
    });

    // FR-004: the user must be able to retry after a failure.
    it("re-enables the button after a failed attempt", async () => {
      const user = userEvent.setup();
      mockInitiateAuth.mockRejectedValue(new Error("Auth failed"));

      render(<LoginRequired />);

      const loginButton = screen.getByRole("button", { name: /login/i });
      await user.click(loginButton);

      await screen.findByRole("alert");
      expect(loginButton).toBeEnabled();

      // A second attempt can be made.
      await user.click(loginButton);
      expect(mockInitiateAuth).toHaveBeenCalledTimes(2);
    });
  });
});

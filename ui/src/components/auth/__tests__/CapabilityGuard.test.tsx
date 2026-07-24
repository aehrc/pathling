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
 * Tests for the CapabilityGuard component.
 *
 * This suite verifies that the guard shows the capability-loading state, gates
 * content behind authentication when the server requires it, and otherwise
 * renders its children with the resolved capabilities alongside the shared
 * session-expiry dialog.
 *
 * @author John Grimes
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { CapabilityGuard } from "../CapabilityGuard";

import type { ServerCapabilities } from "../../../hooks/useServerCapabilities";

// Mock auth state, toggled per test.
let mockIsAuthenticated = false;
vi.mock("../../../contexts/AuthContext", () => ({
  useAuth: () => ({ isAuthenticated: mockIsAuthenticated }),
}));

// Mock config.
vi.mock("../../../config", () => ({
  config: { fhirBaseUrl: "https://fhir.example.org/fhir" },
}));

// Mock the capabilities hook, toggled per test.
let mockCapabilities: ServerCapabilities | undefined;
let mockIsLoading = false;
vi.mock("../../../hooks/useServerCapabilities", () => ({
  useServerCapabilities: () => ({
    data: mockCapabilities,
    isLoading: mockIsLoading,
  }),
}));

// Mock the login and session dialogs to keep assertions focused on the guard.
vi.mock("../LoginRequired", () => ({
  LoginRequired: () => <div data-testid="login-required" />,
}));
vi.mock("../SessionExpiredDialog", () => ({
  SessionExpiredDialog: () => <div data-testid="session-expired-dialog" />,
}));

describe("CapabilityGuard", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockIsAuthenticated = false;
    mockCapabilities = undefined;
    mockIsLoading = false;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("shows the capability-loading state while capabilities are loading", () => {
    mockIsLoading = true;

    render(<CapabilityGuard>{() => <div data-testid="content" />}</CapabilityGuard>);

    expect(screen.getByText("Checking server capabilities...")).toBeInTheDocument();
    expect(screen.getByTestId("session-expired-dialog")).toBeInTheDocument();
    expect(screen.queryByTestId("content")).not.toBeInTheDocument();
  });

  it("shows the login prompt when auth is required but the user is not authenticated", () => {
    mockCapabilities = { authRequired: true, resourceTypes: [] };
    mockIsAuthenticated = false;

    render(<CapabilityGuard>{() => <div data-testid="content" />}</CapabilityGuard>);

    expect(screen.getByTestId("login-required")).toBeInTheDocument();
    expect(screen.queryByTestId("content")).not.toBeInTheDocument();
  });

  it("renders children when auth is required and the user is authenticated", () => {
    mockCapabilities = { authRequired: true, resourceTypes: [] };
    mockIsAuthenticated = true;

    render(<CapabilityGuard>{() => <div data-testid="content" />}</CapabilityGuard>);

    expect(screen.getByTestId("content")).toBeInTheDocument();
    expect(screen.getByTestId("session-expired-dialog")).toBeInTheDocument();
    expect(screen.queryByTestId("login-required")).not.toBeInTheDocument();
  });

  it("renders children when auth is not required, even without authentication", () => {
    mockCapabilities = { authRequired: false, resourceTypes: [] };
    mockIsAuthenticated = false;

    render(<CapabilityGuard>{() => <div data-testid="content" />}</CapabilityGuard>);

    expect(screen.getByTestId("content")).toBeInTheDocument();
    expect(screen.getByTestId("session-expired-dialog")).toBeInTheDocument();
  });

  it("passes the resolved capabilities to the children render prop", () => {
    mockCapabilities = {
      authRequired: false,
      resourceTypes: ["Patient", "Observation"],
      serverName: "Test FHIR Server",
    };

    render(
      <CapabilityGuard>
        {(capabilities) => <div data-testid="content">{capabilities?.serverName}</div>}
      </CapabilityGuard>,
    );

    expect(screen.getByTestId("content")).toHaveTextContent("Test FHIR Server");
  });
});

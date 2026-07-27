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
 * Tests for the authentication context, focused on when a failed request
 * raises the session expiry prompt.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { act } from "react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { notifyUnauthorized } from "../../services/sessionExpiry";
import { render, screen } from "../../test/testUtils";
import { AuthProvider, useAuth } from "../AuthContext";

import type Client from "fhirclient/lib/Client";

/** A stand-in for an authenticated fhirclient. */
const stubClient = { state: {} } as Client;

/**
 * Harness exposing the parts of the authentication context under test.
 *
 * @returns The harness component.
 */
function Harness() {
  const { isAuthenticated, sessionExpired, setClient, setSessionExpired } = useAuth();

  return (
    <>
      <div data-testid="authenticated">{String(isAuthenticated)}</div>
      <div data-testid="expired">{String(sessionExpired)}</div>
      <button
        onClick={() => {
          setClient(stubClient);
        }}
      >
        Authenticate
      </button>
      <button
        onClick={() => {
          setSessionExpired(false);
        }}
      >
        Dismiss
      </button>
    </>
  );
}

/**
 * Renders the harness inside a provider.
 *
 * @returns The render result.
 */
function renderHarness() {
  return render(
    <AuthProvider>
      <Harness />
    </AuthProvider>,
  );
}

describe("AuthContext", () => {
  beforeEach(() => {
    sessionStorage.clear();
  });

  afterEach(() => {
    sessionStorage.clear();
  });

  // FR-008, FR-010: an expiry while a session is held raises the prompt and
  // drops the session so the access check falls back to the login prompt.
  it("raises the expiry prompt and clears the session when authenticated", async () => {
    const user = userEvent.setup();
    sessionStorage.setItem("SMART_KEY", "some-key");
    renderHarness();

    await user.click(screen.getByRole("button", { name: "Authenticate" }));
    expect(screen.getByTestId("authenticated")).toHaveTextContent("true");

    act(() => {
      notifyUnauthorized();
    });

    expect(screen.getByTestId("expired")).toHaveTextContent("true");
    expect(screen.getByTestId("authenticated")).toHaveTextContent("false");
    expect(sessionStorage.getItem("SMART_KEY")).toBeNull();
  });

  // FR-011: there is no session to expire, so no prompt is raised. The stale
  // key is still cleared.
  it("raises no prompt when no session is held", () => {
    sessionStorage.setItem("SMART_KEY", "stale-key");
    renderHarness();

    act(() => {
      notifyUnauthorized();
    });

    expect(screen.getByTestId("expired")).toHaveTextContent("false");
    expect(sessionStorage.getItem("SMART_KEY")).toBeNull();
  });

  // FR-008: this is the defect in #2676. Every expiry is reported, not only
  // the first one in the life of the page.
  it("raises the prompt again after re-authenticating", async () => {
    const user = userEvent.setup();
    renderHarness();

    await user.click(screen.getByRole("button", { name: "Authenticate" }));
    act(() => {
      notifyUnauthorized();
    });
    expect(screen.getByTestId("expired")).toHaveTextContent("true");

    await user.click(screen.getByRole("button", { name: "Dismiss" }));
    expect(screen.getByTestId("expired")).toHaveTextContent("false");

    await user.click(screen.getByRole("button", { name: "Authenticate" }));
    act(() => {
      notifyUnauthorized();
    });

    expect(screen.getByTestId("expired")).toHaveTextContent("true");
  });

  // FR-009: concurrent failures collapse into a single prompt, so one dismiss
  // is enough to clear it.
  it("produces a single expiry from concurrent failures", async () => {
    const user = userEvent.setup();
    renderHarness();

    await user.click(screen.getByRole("button", { name: "Authenticate" }));
    act(() => {
      notifyUnauthorized();
      notifyUnauthorized();
      notifyUnauthorized();
    });
    expect(screen.getByTestId("expired")).toHaveTextContent("true");

    await user.click(screen.getByRole("button", { name: "Dismiss" }));

    expect(screen.getByTestId("expired")).toHaveTextContent("false");
  });
});

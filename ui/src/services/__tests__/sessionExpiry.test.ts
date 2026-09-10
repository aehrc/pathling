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
 * Tests for the session expiry notification bridge.
 *
 * @author John Grimes
 */

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  notifyUnauthorized,
  registerSessionExpiryHandler,
} from "../sessionExpiry";

describe("sessionExpiry", () => {
  afterEach(() => {
    // Leave no handler behind for the next test.
    registerSessionExpiryHandler(() => {});
    vi.restoreAllMocks();
  });

  // The bridge is used from the query cache, which can fire before the
  // application has mounted.
  it("does nothing when no handler is registered", () => {
    expect(() => {
      notifyUnauthorized();
    }).not.toThrow();
  });

  it("invokes the registered handler", () => {
    const handler = vi.fn();
    registerSessionExpiryHandler(handler);

    notifyUnauthorized();

    expect(handler).toHaveBeenCalledTimes(1);
  });

  // FR-008: there is no deduplication here, so a second expiry is reported.
  it("invokes the handler again on a second notification", () => {
    const handler = vi.fn();
    registerSessionExpiryHandler(handler);

    notifyUnauthorized();
    notifyUnauthorized();

    expect(handler).toHaveBeenCalledTimes(2);
  });

  it("replaces the handler when a second one is registered", () => {
    const first = vi.fn();
    const second = vi.fn();
    registerSessionExpiryHandler(first);
    registerSessionExpiryHandler(second);

    notifyUnauthorized();

    expect(first).not.toHaveBeenCalled();
    expect(second).toHaveBeenCalledTimes(1);
  });
});

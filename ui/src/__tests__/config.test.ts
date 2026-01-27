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
 * Unit tests for application configuration utilities.
 *
 * These tests verify the behaviour of configuration functions that read from
 * Vite environment variables, ensuring correct fallback to default values.
 *
 * @author John Grimes
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// We need to test with different env configurations, so we dynamically import
// the module after setting up the environment mocks.

describe("config constants", () => {
  it("exports correct DEFAULT_CLIENT_ID value", async () => {
    const { DEFAULT_CLIENT_ID } = await import("../config");
    expect(DEFAULT_CLIENT_ID).toBe("pathling-admin-ui");
  });

  it("exports correct DEFAULT_SCOPE value", async () => {
    const { DEFAULT_SCOPE } = await import("../config");
    expect(DEFAULT_SCOPE).toBe("openid profile user/*.*");
  });
});

describe("getEnvClientId", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("returns environment variable when VITE_CLIENT_ID is set", async () => {
    // Stub the environment variable before importing the module.
    vi.stubEnv("VITE_CLIENT_ID", "custom-client-id");

    const { getEnvClientId } = await import("../config");

    expect(getEnvClientId()).toBe("custom-client-id");
  });

  it("returns undefined when VITE_CLIENT_ID is not set", async () => {
    // Ensure the env var is not set by stubbing with undefined.
    vi.stubEnv("VITE_CLIENT_ID", undefined as unknown as string);

    const { getEnvClientId } = await import("../config");

    expect(getEnvClientId()).toBeUndefined();
  });
});

describe("getScope", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("returns environment variable when VITE_SCOPE is set", async () => {
    vi.stubEnv("VITE_SCOPE", "custom-scope openid");

    const { getScope } = await import("../config");

    expect(getScope()).toBe("custom-scope openid");
  });

  it("returns DEFAULT_SCOPE when VITE_SCOPE is not set", async () => {
    vi.stubEnv("VITE_SCOPE", undefined as unknown as string);

    const { getScope, DEFAULT_SCOPE } = await import("../config");

    expect(getScope()).toBe(DEFAULT_SCOPE);
  });

  it("returns DEFAULT_SCOPE when VITE_SCOPE is empty string", async () => {
    // An empty string is falsy, so should fall back to default.
    vi.stubEnv("VITE_SCOPE", "");

    const { getScope, DEFAULT_SCOPE } = await import("../config");

    expect(getScope()).toBe(DEFAULT_SCOPE);
  });
});

describe("config object", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("uses VITE_FHIR_BASE_URL when set", async () => {
    vi.stubEnv("VITE_FHIR_BASE_URL", "https://custom.fhir.server/api");

    const { config } = await import("../config");

    expect(config.fhirBaseUrl).toBe("https://custom.fhir.server/api");
  });

  it("defaults to /fhir when VITE_FHIR_BASE_URL is not set", async () => {
    vi.stubEnv("VITE_FHIR_BASE_URL", undefined as unknown as string);

    const { config } = await import("../config");

    expect(config.fhirBaseUrl).toBe("/fhir");
  });

  it("defaults to /fhir when VITE_FHIR_BASE_URL is empty string", async () => {
    vi.stubEnv("VITE_FHIR_BASE_URL", "");

    const { config } = await import("../config");

    expect(config.fhirBaseUrl).toBe("/fhir");
  });
});

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
 * Unit tests for authentication service.
 *
 * @author John Grimes
 */

import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  checkServerCapabilities,
  clearReturnUrl,
  completeAuth,
  DEFAULT_CLIENT_ID,
  getClientId,
  getReturnUrl,
  hasExistingSession,
  initiateAuth,
} from "./auth";
import * as config from "../config";

import type { CapabilityStatement } from "fhir/r4";

// Mock fhirclient library.
vi.mock("fhirclient", () => ({
  default: {
    oauth2: {
      authorize: vi.fn(),
      ready: vi.fn(),
    },
  },
}));

describe("getClientId", () => {
  const mockFhirBaseUrl = "http://localhost:8080/fhir";

  beforeEach(() => {
    // Reset mocks before each test.
    vi.resetAllMocks();
    global.fetch = vi.fn();
    // Default to no env var.
    vi.spyOn(config, "getEnvClientId").mockReturnValue(undefined);
  });

  it("returns admin_ui_client_id from SMART configuration when present", async () => {
    // When the SMART configuration contains admin_ui_client_id, it should be returned.
    const mockClientId = "server-configured-client-id";
    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          admin_ui_client_id: mockClientId,
          authorization_endpoint: "https://auth.example.com/authorize",
          token_endpoint: "https://auth.example.com/token",
        }),
    } as Response);

    const result = await getClientId(mockFhirBaseUrl);

    expect(result).toBe(mockClientId);
    expect(global.fetch).toHaveBeenCalledWith(
      `${mockFhirBaseUrl}/.well-known/smart-configuration`,
    );
  });

  it("falls back to env var when SMART config has no admin_ui_client_id", async () => {
    // When SMART config doesn't have admin_ui_client_id, fall back to env var.
    vi.spyOn(config, "getEnvClientId").mockReturnValue("env-var-client-id");

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          authorization_endpoint: "https://auth.example.com/authorize",
          token_endpoint: "https://auth.example.com/token",
        }),
    } as Response);

    const result = await getClientId(mockFhirBaseUrl);

    expect(result).toBe("env-var-client-id");
  });

  it("falls back to default when SMART config fetch fails", async () => {
    // When SMART config fetch fails, fall back to env var or default.
    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: false,
      status: 404,
    } as Response);

    const result = await getClientId(mockFhirBaseUrl);

    expect(result).toBe(DEFAULT_CLIENT_ID);
  });

  it("falls back to default when both SMART config and env var are missing", async () => {
    // When both SMART config and env var are unavailable, use default.
    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          authorization_endpoint: "https://auth.example.com/authorize",
        }),
    } as Response);

    const result = await getClientId(mockFhirBaseUrl);

    expect(result).toBe(DEFAULT_CLIENT_ID);
  });

  it("falls back to default when SMART config fetch throws error", async () => {
    // When SMART config fetch throws an error, fall back gracefully.
    vi.mocked(global.fetch).mockRejectedValueOnce(new Error("Network error"));

    const result = await getClientId(mockFhirBaseUrl);

    expect(result).toBe(DEFAULT_CLIENT_ID);
  });

  it("uses env var when SMART config returns empty admin_ui_client_id", async () => {
    // Empty string in SMART config should fall back to env var.
    vi.spyOn(config, "getEnvClientId").mockReturnValue("env-var-client-id");

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          admin_ui_client_id: "",
        }),
    } as Response);

    const result = await getClientId(mockFhirBaseUrl);

    expect(result).toBe("env-var-client-id");
  });
});

describe("checkServerCapabilities", () => {
  const mockFhirBaseUrl = "http://localhost:8080/fhir";

  beforeEach(() => {
    vi.resetAllMocks();
    global.fetch = vi.fn();
  });

  // Helper to create a minimal capability statement.
  function createCapabilityStatement(
    overrides: Partial<CapabilityStatement> = {},
  ): CapabilityStatement {
    return {
      resourceType: "CapabilityStatement",
      status: "active",
      date: "2024-01-01",
      kind: "instance",
      fhirVersion: "4.0.1",
      format: ["json"],
      ...overrides,
    };
  }

  it("returns authRequired true when SMART service coding is present", async () => {
    // When the capability statement includes SMART-on-FHIR in the security
    // service codings, authentication should be required.
    const capability = createCapabilityStatement({
      rest: [
        {
          mode: "server",
          security: {
            service: [
              {
                coding: [
                  {
                    system:
                      "http://terminology.hl7.org/CodeSystem/restful-security-service",
                    code: "SMART-on-FHIR",
                  },
                ],
              },
            ],
          },
        },
      ],
    });

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(capability),
    } as Response);

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.authRequired).toBe(true);
    expect(global.fetch).toHaveBeenCalledWith(`${mockFhirBaseUrl}/metadata`, {
      headers: { Accept: "application/fhir+json" },
    });
  });

  it("returns authRequired false when no SMART service is present", async () => {
    // When the capability statement has no security service indicating SMART,
    // authentication should not be required.
    const capability = createCapabilityStatement({
      rest: [
        {
          mode: "server",
        },
      ],
    });

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(capability),
    } as Response);

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.authRequired).toBe(false);
  });

  it("detects auth requirement via OAuth extension URL", async () => {
    // Authentication should be detected when oauth-uris extension is present
    // in the security section, even without SMART service coding.
    const capability = createCapabilityStatement({
      rest: [
        {
          mode: "server",
          security: {
            extension: [
              {
                url: "http://fhir-registry.smarthealthit.org/StructureDefinition/oauth-uris",
                extension: [
                  {
                    url: "authorize",
                    valueUri: "https://auth.example.com/authorize",
                  },
                ],
              },
            ],
          },
        },
      ],
    });

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(capability),
    } as Response);

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.authRequired).toBe(true);
  });

  it("detects auth requirement via smart-configuration extension URL", async () => {
    // Authentication should be detected when smart-configuration extension is
    // present in the security section.
    const capability = createCapabilityStatement({
      rest: [
        {
          mode: "server",
          security: {
            extension: [
              {
                url: "http://fhir-registry.smarthealthit.org/StructureDefinition/smart-configuration",
                valueUri:
                  "https://example.com/fhir/.well-known/smart-configuration",
              },
            ],
          },
        },
      ],
    });

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(capability),
    } as Response);

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.authRequired).toBe(true);
  });

  it("extracts server metadata from capability statement", async () => {
    // Server metadata should be extracted from the capability statement's
    // implementation, software, and top-level fields.
    const capability = createCapabilityStatement({
      name: "Test FHIR Server",
      description: "A test FHIR server for unit testing.",
      publisher: "Test Organisation",
      fhirVersion: "4.0.1",
      implementation: {
        description: "Production FHIR Server",
      },
      software: {
        name: "Test Server",
        version: "1.2.3",
      },
      rest: [{ mode: "server" }],
    });

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(capability),
    } as Response);

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.serverName).toBe("Production FHIR Server");
    expect(result.serverVersion).toBe("1.2.3");
    expect(result.fhirVersion).toBe("4.0.1");
    expect(result.publisher).toBe("Test Organisation");
    expect(result.description).toBe("A test FHIR server for unit testing.");
  });

  it("falls back to name when implementation description is absent", async () => {
    // When implementation.description is missing, the server name should fall
    // back to the capability statement's name field.
    const capability = createCapabilityStatement({
      name: "Fallback Server Name",
      rest: [{ mode: "server" }],
    });

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(capability),
    } as Response);

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.serverName).toBe("Fallback Server Name");
  });

  it("extracts resource capabilities with interactions", async () => {
    // Resource capabilities should include the resource type and all supported
    // interactions (read, search-type, etc.).
    const capability = createCapabilityStatement({
      rest: [
        {
          mode: "server",
          resource: [
            {
              type: "Patient",
              interaction: [{ code: "read" }, { code: "search-type" }],
            },
            {
              type: "Observation",
              interaction: [
                { code: "read" },
                { code: "create" },
                { code: "update" },
              ],
            },
          ],
        },
      ],
    });

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(capability),
    } as Response);

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.resources).toHaveLength(2);
    expect(result.resources?.[0]).toEqual({
      type: "Patient",
      operations: ["read", "search-type"],
    });
    expect(result.resources?.[1]).toEqual({
      type: "Observation",
      operations: ["read", "create", "update"],
    });
  });

  it("extracts resource-level operations with $ prefix", async () => {
    // Resource-level operations should be extracted and prefixed with $.
    const capability = createCapabilityStatement({
      rest: [
        {
          mode: "server",
          resource: [
            {
              type: "Patient",
              interaction: [{ code: "read" }],
              operation: [
                {
                  name: "everything",
                  definition:
                    "http://hl7.org/fhir/OperationDefinition/Patient-everything",
                },
                {
                  name: "match",
                  definition:
                    "http://hl7.org/fhir/OperationDefinition/Patient-match",
                },
              ],
            },
          ],
        },
      ],
    });

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(capability),
    } as Response);

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.resources?.[0]).toEqual({
      type: "Patient",
      operations: ["read", "$everything", "$match"],
    });
  });

  it("extracts system-level operations", async () => {
    // System-level operations should be extracted from rest.operation.
    const capability = createCapabilityStatement({
      rest: [
        {
          mode: "server",
          operation: [
            {
              name: "aggregate",
              definition:
                "http://example.org/fhir/OperationDefinition/aggregate",
            },
            {
              name: "search",
              definition: "http://example.org/fhir/OperationDefinition/search",
            },
          ],
        },
      ],
    });

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(capability),
    } as Response);

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.operations).toHaveLength(2);
    expect(result.operations?.[0]).toEqual({
      name: "aggregate",
      definition: "http://example.org/fhir/OperationDefinition/aggregate",
    });
    expect(result.operations?.[1]).toEqual({
      name: "search",
      definition: "http://example.org/fhir/OperationDefinition/search",
    });
  });

  it("returns authRequired true when fetch fails", async () => {
    // When the metadata fetch fails (non-200 response), we default to requiring
    // authentication for security.
    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: false,
      status: 500,
    } as Response);

    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.authRequired).toBe(true);
    expect(result.serverName).toBeUndefined();
    expect(consoleSpy).toHaveBeenCalled();

    consoleSpy.mockRestore();
  });

  it("returns authRequired true when network error occurs", async () => {
    // When a network error occurs during fetch, we default to requiring
    // authentication for security.
    vi.mocked(global.fetch).mockRejectedValueOnce(new Error("Network error"));

    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.authRequired).toBe(true);
    expect(result.serverName).toBeUndefined();
    expect(consoleSpy).toHaveBeenCalled();

    consoleSpy.mockRestore();
  });

  it("handles empty rest array gracefully", async () => {
    // When the capability statement has an empty rest array, the function
    // should return empty resources and operations arrays.
    const capability = createCapabilityStatement({
      rest: [],
    });

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(capability),
    } as Response);

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.authRequired).toBe(false);
    expect(result.resources).toEqual([]);
    expect(result.operations).toEqual([]);
  });

  it("handles missing rest field gracefully", async () => {
    // When the capability statement has no rest field at all, the function
    // should handle it gracefully.
    const capability = createCapabilityStatement();

    vi.mocked(global.fetch).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(capability),
    } as Response);

    const result = await checkServerCapabilities(mockFhirBaseUrl);

    expect(result.authRequired).toBe(false);
    expect(result.resources).toEqual([]);
    expect(result.operations).toEqual([]);
  });
});

describe("initiateAuth", () => {
  const mockFhirBaseUrl = "http://localhost:8080/fhir";

  beforeEach(async () => {
    vi.resetAllMocks();
    global.fetch = vi.fn();
    sessionStorage.clear();

    // Mock window.location.
    Object.defineProperty(window, "location", {
      value: {
        pathname: "/admin/some/path",
        origin: "http://localhost:3000",
      },
      writable: true,
    });

    // Default mock for SMART config fetch (no admin_ui_client_id).
    vi.mocked(global.fetch).mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({}),
    } as Response);

    // Import fresh FHIR mock.
    const FHIR = await import("fhirclient");
    vi.mocked(FHIR.default.oauth2.authorize).mockResolvedValue(undefined);
  });

  it("stores current path in sessionStorage without /admin prefix", async () => {
    // The current pathname should be stored for post-auth redirect, with the
    // /admin prefix stripped since React Router will add it back.
    await initiateAuth(mockFhirBaseUrl);

    expect(sessionStorage.getItem("pathling_return_url")).toBe("/some/path");
  });

  it("stores root path when pathname is just /admin", async () => {
    // When the pathname is just /admin, the stored path should be /.
    Object.defineProperty(window, "location", {
      value: {
        pathname: "/admin",
        origin: "http://localhost:3000",
      },
      writable: true,
    });

    await initiateAuth(mockFhirBaseUrl);

    expect(sessionStorage.getItem("pathling_return_url")).toBe("/");
  });

  it("converts relative FHIR URL to absolute URL", async () => {
    // When fhirBaseUrl is relative, it should be converted to absolute for
    // fhirclient compatibility.
    const FHIR = await import("fhirclient");

    await initiateAuth("/fhir");

    expect(FHIR.default.oauth2.authorize).toHaveBeenCalledWith(
      expect.objectContaining({
        iss: "http://localhost:3000/fhir",
      }),
    );
  });

  it("uses absolute FHIR URL as-is", async () => {
    // When fhirBaseUrl is already absolute, it should be used directly.
    const FHIR = await import("fhirclient");

    await initiateAuth("https://fhir.example.com/r4");

    expect(FHIR.default.oauth2.authorize).toHaveBeenCalledWith(
      expect.objectContaining({
        iss: "https://fhir.example.com/r4",
      }),
    );
  });

  it("calls FHIR.oauth2.authorize with correct parameters", async () => {
    // The authorize call should include the correct client ID, scope, and
    // redirect URI.
    const FHIR = await import("fhirclient");
    vi.spyOn(config, "getScope").mockReturnValue("openid profile user/*.*");

    await initiateAuth(mockFhirBaseUrl);

    expect(FHIR.default.oauth2.authorize).toHaveBeenCalledWith({
      iss: mockFhirBaseUrl,
      clientId: DEFAULT_CLIENT_ID,
      scope: "openid profile user/*.*",
      redirectUri: "http://localhost:3000/admin/callback",
    });
  });
});

describe("getReturnUrl", () => {
  beforeEach(() => {
    sessionStorage.clear();
  });

  it("returns stored URL from sessionStorage", () => {
    // When a return URL is stored, it should be returned.
    sessionStorage.setItem("pathling_return_url", "/dashboard");

    const result = getReturnUrl();

    expect(result).toBe("/dashboard");
  });

  it("returns root path when no URL is stored", () => {
    // When no return URL is stored, the function should return /.
    const result = getReturnUrl();

    expect(result).toBe("/");
  });
});

describe("clearReturnUrl", () => {
  beforeEach(() => {
    sessionStorage.clear();
  });

  it("removes return URL from sessionStorage", () => {
    // The stored return URL should be removed after calling clearReturnUrl.
    sessionStorage.setItem("pathling_return_url", "/dashboard");

    clearReturnUrl();

    expect(sessionStorage.getItem("pathling_return_url")).toBeNull();
  });
});

describe("completeAuth", () => {
  beforeEach(async () => {
    vi.resetAllMocks();
  });

  it("calls FHIR.oauth2.ready and returns the client", async () => {
    // completeAuth should delegate to FHIR.oauth2.ready() and return the
    // authenticated client.
    const FHIR = await import("fhirclient");
    const mockClient = { patient: { id: "123" } };
    vi.mocked(FHIR.default.oauth2.ready).mockResolvedValue(mockClient as never);

    const result = await completeAuth();

    expect(FHIR.default.oauth2.ready).toHaveBeenCalled();
    expect(result).toBe(mockClient);
  });
});

describe("hasExistingSession", () => {
  beforeEach(() => {
    sessionStorage.clear();
  });

  it("returns true when SMART_KEY exists in sessionStorage", () => {
    // When SMART_KEY is present, indicating an existing SMART session, the
    // function should return true.
    sessionStorage.setItem("SMART_KEY", "some-session-key");

    const result = hasExistingSession();

    expect(result).toBe(true);
  });

  it("returns false when SMART_KEY does not exist", () => {
    // When SMART_KEY is absent, the function should return false.
    const result = hasExistingSession();

    expect(result).toBe(false);
  });
});

/**
 * Unit tests for authentication service.
 *
 * @author John Grimes
 */

import { beforeEach, describe, expect, it, vi } from "vitest";

import { DEFAULT_CLIENT_ID, getClientId } from "./auth";
import * as config from "../config";

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

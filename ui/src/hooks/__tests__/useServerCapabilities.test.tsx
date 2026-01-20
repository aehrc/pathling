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
 * Tests for the useServerCapabilities hook.
 *
 * This test suite verifies that the useServerCapabilities hook correctly fetches
 * and parses the CapabilityStatement from a FHIR server, extracting server info,
 * auth requirements, resources, and operations.
 *
 * @author John Grimes
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { useServerCapabilities } from "../useServerCapabilities";

import type { CapabilityStatement } from "fhir/r4";
import type { ReactNode } from "react";

/**
 * Creates a wrapper component that provides TanStack Query context.
 *
 * @returns A wrapper function suitable for renderHook.
 */
function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
}

describe("useServerCapabilities", () => {
  const mockCapabilityStatement: CapabilityStatement = {
    resourceType: "CapabilityStatement",
    status: "active",
    date: "2024-01-15",
    kind: "instance",
    fhirVersion: "4.0.1",
    format: ["application/fhir+json"],
    name: "TestServer",
    implementation: {
      description: "Test FHIR Server Implementation",
    },
    software: {
      name: "TestServer",
      version: "1.0.0",
    },
    publisher: "Test Organisation",
    description: "A test FHIR server for unit testing",
    rest: [
      {
        mode: "server",
        resource: [
          {
            type: "Patient",
            interaction: [
              { code: "read" },
              { code: "search-type" },
              { code: "create" },
              { code: "update" },
              { code: "delete" },
            ],
            operation: [{ name: "match", definition: "http://example.org/match" }],
          },
          {
            type: "Observation",
            interaction: [{ code: "read" }, { code: "search-type" }],
          },
        ],
        operation: [
          { name: "export", definition: "http://example.org/export" },
          { name: "import", definition: "http://example.org/import" },
        ],
      },
    ],
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
    vi.restoreAllMocks();
  });

  describe("successful fetch", () => {
    it("returns server capabilities on successful fetch", async () => {
      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(mockCapabilityStatement),
      } as Response);

      const { result } = renderHook(() => useServerCapabilities("http://localhost:8080/fhir"), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data).toEqual({
        authRequired: false,
        serverName: "Test FHIR Server Implementation",
        serverVersion: "1.0.0",
        fhirVersion: "4.0.1",
        publisher: "Test Organisation",
        description: "A test FHIR server for unit testing",
        resources: [
          {
            type: "Patient",
            operations: ["read", "search-type", "create", "update", "delete", "$match"],
          },
          {
            type: "Observation",
            operations: ["read", "search-type"],
          },
        ],
        resourceTypes: ["Observation", "Patient"],
        operations: [
          { name: "export", definition: "http://example.org/export" },
          { name: "import", definition: "http://example.org/import" },
        ],
      });
    });

    it("calls fetch with correct URL and headers", async () => {
      const fetchSpy = vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(mockCapabilityStatement),
      } as Response);

      const { result } = renderHook(() => useServerCapabilities("http://localhost:8080/fhir"), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(fetchSpy).toHaveBeenCalledWith("http://localhost:8080/fhir/metadata", {
        headers: { Accept: "application/fhir+json" },
      });
    });

    it("falls back to name when implementation description is not present", async () => {
      const capabilityWithoutImpl: CapabilityStatement = {
        ...mockCapabilityStatement,
        implementation: undefined,
      };

      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(capabilityWithoutImpl),
      } as Response);

      const { result } = renderHook(() => useServerCapabilities("http://localhost:8080/fhir"), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data?.serverName).toBe("TestServer");
    });
  });

  describe("auth detection - SMART-on-FHIR service", () => {
    it("detects auth required from SMART-on-FHIR service coding", async () => {
      const capabilityWithAuth: CapabilityStatement = {
        ...mockCapabilityStatement,
        rest: [
          {
            mode: "server",
            security: {
              service: [
                {
                  coding: [
                    {
                      system: "http://terminology.hl7.org/CodeSystem/restful-security-service",
                      code: "SMART-on-FHIR",
                    },
                  ],
                },
              ],
            },
            resource: [],
          },
        ],
      };

      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(capabilityWithAuth),
      } as Response);

      const { result } = renderHook(() => useServerCapabilities("http://localhost:8080/fhir"), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data?.authRequired).toBe(true);
    });
  });

  describe("auth detection - OAuth extension", () => {
    it("detects auth required from oauth-uris extension", async () => {
      const capabilityWithOAuth: CapabilityStatement = {
        ...mockCapabilityStatement,
        rest: [
          {
            mode: "server",
            security: {
              extension: [
                {
                  url: "http://fhir-registry.smarthealthit.org/StructureDefinition/oauth-uris",
                },
              ],
            },
            resource: [],
          },
        ],
      };

      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(capabilityWithOAuth),
      } as Response);

      const { result } = renderHook(() => useServerCapabilities("http://localhost:8080/fhir"), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data?.authRequired).toBe(true);
    });

    it("detects auth required from smart-configuration extension", async () => {
      const capabilityWithSmartConfig: CapabilityStatement = {
        ...mockCapabilityStatement,
        rest: [
          {
            mode: "server",
            security: {
              extension: [
                {
                  url: "http://fhir-registry.smarthealthit.org/StructureDefinition/smart-configuration",
                },
              ],
            },
            resource: [],
          },
        ],
      };

      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(capabilityWithSmartConfig),
      } as Response);

      const { result } = renderHook(() => useServerCapabilities("http://localhost:8080/fhir"), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data?.authRequired).toBe(true);
    });
  });

  describe("resource types", () => {
    it("returns sorted, deduplicated resource types", async () => {
      const capabilityWithDuplicates: CapabilityStatement = {
        ...mockCapabilityStatement,
        rest: [
          {
            mode: "server",
            resource: [
              { type: "Patient", interaction: [] },
              { type: "Observation", interaction: [] },
              { type: "Patient", interaction: [] }, // Duplicate
              { type: "Condition", interaction: [] },
            ],
          },
        ],
      };

      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(capabilityWithDuplicates),
      } as Response);

      const { result } = renderHook(() => useServerCapabilities("http://localhost:8080/fhir"), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      // Should be sorted alphabetically and deduplicated.
      expect(result.current.data?.resourceTypes).toEqual(["Condition", "Observation", "Patient"]);
    });
  });

  describe("enabled state", () => {
    it("does not fetch when fhirBaseUrl is null", async () => {
      const fetchSpy = vi.spyOn(global, "fetch");

      const { result } = renderHook(() => useServerCapabilities(null), {
        wrapper: createWrapper(),
      });

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.fetchStatus).toBe("idle");
      expect(fetchSpy).not.toHaveBeenCalled();
    });

    it("does not fetch when fhirBaseUrl is undefined", async () => {
      const fetchSpy = vi.spyOn(global, "fetch");

      const { result } = renderHook(() => useServerCapabilities(undefined), {
        wrapper: createWrapper(),
      });

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.fetchStatus).toBe("idle");
      expect(fetchSpy).not.toHaveBeenCalled();
    });

    it("does not fetch when fhirBaseUrl is empty string", async () => {
      const fetchSpy = vi.spyOn(global, "fetch");

      const { result } = renderHook(() => useServerCapabilities(""), {
        wrapper: createWrapper(),
      });

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.fetchStatus).toBe("idle");
      expect(fetchSpy).not.toHaveBeenCalled();
    });
  });

  describe("error handling", () => {
    it("returns error state when fetch fails", async () => {
      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: false,
        status: 500,
      } as Response);

      const { result } = renderHook(() => useServerCapabilities("http://localhost:8080/fhir"), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error?.message).toBe("Failed to fetch metadata: 500");
    });

    it("returns error state when fetch throws", async () => {
      vi.spyOn(global, "fetch").mockRejectedValue(new Error("Network error"));

      const { result } = renderHook(() => useServerCapabilities("http://localhost:8080/fhir"), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error?.message).toBe("Network error");
    });
  });

  describe("query key", () => {
    it("uses fhirBaseUrl in query key", async () => {
      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(mockCapabilityStatement),
      } as Response);

      const { result, rerender } = renderHook(({ url }) => useServerCapabilities(url), {
        wrapper: createWrapper(),
        initialProps: { url: "http://server1.com/fhir" },
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(fetch).toHaveBeenCalledWith("http://server1.com/fhir/metadata", expect.any(Object));

      rerender({ url: "http://server2.com/fhir" });

      await waitFor(() => {
        expect(fetch).toHaveBeenCalledWith("http://server2.com/fhir/metadata", expect.any(Object));
      });
    });
  });
});

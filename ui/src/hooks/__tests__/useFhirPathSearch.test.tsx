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
 * Tests for the useFhirPathSearch hook.
 *
 * This test suite verifies that the useFhirPathSearch hook correctly searches
 * for FHIR resources using FHIRPath filter expressions, extracting resources
 * from the bundle and providing proper loading and error states.
 *
 * @author John Grimes
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Mock the API module.
vi.mock("../../api", () => ({
  search: vi.fn(),
}));

// Mock the config module.
vi.mock("../../config", () => ({
  config: {
    fhirBaseUrl: "http://localhost:8080/fhir",
  },
}));

// Mock the AuthContext.
vi.mock("../../contexts/AuthContext", () => ({
  useAuth: vi.fn(() => ({
    client: {
      state: {
        tokenResponse: {
          access_token: "test-token",
        },
      },
    },
  })),
}));

import { search } from "../../api";
import { config } from "../../config";
import { useFhirPathSearch } from "../useFhirPathSearch";

import type { Bundle, Patient } from "fhir/r4";
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

describe("useFhirPathSearch", () => {
  const mockPatient1: Patient = {
    resourceType: "Patient",
    id: "1",
    active: true,
    name: [{ family: "Smith", given: ["John"] }],
  };

  const mockPatient2: Patient = {
    resourceType: "Patient",
    id: "2",
    active: true,
    name: [{ family: "Jones", given: ["Jane"] }],
  };

  const mockBundle: Bundle = {
    resourceType: "Bundle",
    type: "searchset",
    total: 2,
    entry: [{ resource: mockPatient1 }, { resource: mockPatient2 }],
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("successful search", () => {
    it("returns resources from search bundle", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters: ["active = true"],
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false);
      });

      expect(result.current.resources).toEqual([mockPatient1, mockPatient2]);
      expect(result.current.total).toBe(2);
      expect(result.current.bundle).toEqual(mockBundle);
    });

    it("calls search API with FHIRPath query parameters", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters: ["name.family = 'Smith'", "active = true"],
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false);
      });

      expect(search).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Patient",
        params: {
          _query: "fhirPath",
          _count: "10",
          filter: ["name.family = 'Smith'", "active = true"],
        },
        accessToken: "test-token",
      });
    });

    it("trims and filters empty filter expressions", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters: ["  active = true  ", "", "  ", "name.exists()"],
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false);
      });

      expect(search).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          params: expect.objectContaining({
            filter: ["active = true", "name.exists()"],
          }),
        }),
      );
    });

    it("omits filter param when all filters are empty", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters: ["", "  "],
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false);
      });

      expect(search).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          params: {
            _query: "fhirPath",
            _count: "10",
          },
        }),
      );
    });
  });

  describe("empty results", () => {
    it("returns empty array when bundle has no entries", async () => {
      const emptyBundle: Bundle = {
        resourceType: "Bundle",
        type: "searchset",
        total: 0,
      };

      vi.mocked(search).mockResolvedValue(emptyBundle);

      const { result } = renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters: ["name.family = 'NonexistentName'"],
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false);
      });

      expect(result.current.resources).toEqual([]);
      expect(result.current.total).toBe(0);
    });

    it("handles entries without resources", async () => {
      const bundleWithMissingResources: Bundle = {
        resourceType: "Bundle",
        type: "searchset",
        entry: [
          { resource: mockPatient1 },
          { fullUrl: "http://example.com/Patient/2" }, // No resource
          { resource: mockPatient2 },
        ],
      };

      vi.mocked(search).mockResolvedValue(bundleWithMissingResources);

      const { result } = renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters: ["active = true"],
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false);
      });

      expect(result.current.resources).toEqual([mockPatient1, mockPatient2]);
    });
  });

  describe("query key behaviour", () => {
    it("uses resourceType and filters in query key", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters: ["active = true"],
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(search).toHaveBeenCalledTimes(1);
      });
    });

    it("refetches when filters change", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result, rerender } = renderHook(
        ({ filters }) =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters,
          }),
        {
          wrapper: createWrapper(),
          initialProps: { filters: ["active = true"] },
        },
      );

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false);
      });

      rerender({ filters: ["name.exists()"] });

      await waitFor(() => {
        expect(search).toHaveBeenCalledTimes(2);
      });
    });
  });

  describe("enabled state", () => {
    it("does not fetch when enabled is false", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters: ["active = true"],
            enabled: false,
          }),
        { wrapper: createWrapper() },
      );

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.isLoading).toBe(false);
      expect(search).not.toHaveBeenCalled();
    });

    it("does not fetch when resourceType is empty", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "",
            filters: ["active = true"],
          }),
        { wrapper: createWrapper() },
      );

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.isLoading).toBe(false);
      expect(search).not.toHaveBeenCalled();
    });

    it("does not fetch when fhirBaseUrl is not configured", async () => {
      vi.mocked(config).fhirBaseUrl = undefined as unknown as string;
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters: ["active = true"],
          }),
        { wrapper: createWrapper() },
      );

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.isLoading).toBe(false);
      expect(search).not.toHaveBeenCalled();

      // Restore config for other tests.
      vi.mocked(config).fhirBaseUrl = "http://localhost:8080/fhir";
    });
  });

  describe("error handling", () => {
    it("returns error state on failed query", async () => {
      const testError = new Error("Network error");
      vi.mocked(search).mockRejectedValue(testError);

      const { result } = renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters: ["active = true"],
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error).toBe(testError);
      expect(result.current.resources).toEqual([]);
    });
  });

  describe("refetch function", () => {
    it("provides a refetch function", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(
        () =>
          useFhirPathSearch({
            resourceType: "Patient",
            filters: ["active = true"],
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false);
      });

      expect(typeof result.current.refetch).toBe("function");

      // Call refetch.
      result.current.refetch();

      await waitFor(() => {
        expect(search).toHaveBeenCalledTimes(2);
      });
    });
  });
});

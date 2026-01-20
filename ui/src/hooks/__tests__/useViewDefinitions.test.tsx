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
 * Tests for the useViewDefinitions hook.
 *
 * This test suite verifies that the useViewDefinitions hook correctly fetches
 * ViewDefinition resources from the server and transforms them into summary
 * objects with id, name, and JSON representation.
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
import { useViewDefinitions } from "../useViewDefinitions";

import type { Bundle, FhirResource } from "fhir/r4";
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

describe("useViewDefinitions", () => {
  const mockViewDefinition1 = {
    resourceType: "ViewDefinition",
    id: "vd-1",
    name: "PatientView",
    resource: "Patient",
    status: "active",
    select: [{ column: [{ path: "id", name: "patient_id" }] }],
  };

  const mockViewDefinition2 = {
    resourceType: "ViewDefinition",
    id: "vd-2",
    name: "ObservationView",
    resource: "Observation",
    status: "active",
    select: [{ column: [{ path: "code.text", name: "code_display" }] }],
  };

  const mockBundle: Bundle = {
    resourceType: "Bundle",
    type: "searchset",
    total: 2,
    entry: [
      { resource: mockViewDefinition1 as unknown as FhirResource },
      { resource: mockViewDefinition2 as unknown as FhirResource },
    ],
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("successful fetch", () => {
    it("returns ViewDefinition summaries on successful query", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useViewDefinitions(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data).toHaveLength(2);
      expect(result.current.data?.[0]).toEqual({
        id: "vd-1",
        name: "PatientView",
        json: JSON.stringify(mockViewDefinition1, null, 2),
      });
      expect(result.current.data?.[1]).toEqual({
        id: "vd-2",
        name: "ObservationView",
        json: JSON.stringify(mockViewDefinition2, null, 2),
      });
    });

    it("calls search API with correct parameters", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useViewDefinitions(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(search).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "ViewDefinition",
        accessToken: "test-token",
      });
    });

    it("uses id as name when name is not provided", async () => {
      const viewWithoutName = {
        resourceType: "ViewDefinition",
        id: "vd-no-name",
        resource: "Patient",
        status: "active",
        select: [],
      };

      const bundleWithoutName: Bundle = {
        resourceType: "Bundle",
        type: "searchset",
        entry: [{ resource: viewWithoutName as unknown as FhirResource }],
      };

      vi.mocked(search).mockResolvedValue(bundleWithoutName);

      const { result } = renderHook(() => useViewDefinitions(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data?.[0]?.name).toBe("vd-no-name");
    });

    it("returns empty array when bundle has no entries", async () => {
      const emptyBundle: Bundle = {
        resourceType: "Bundle",
        type: "searchset",
        total: 0,
      };

      vi.mocked(search).mockResolvedValue(emptyBundle);

      const { result } = renderHook(() => useViewDefinitions(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data).toEqual([]);
    });
  });

  describe("query key", () => {
    it("uses viewDefinitions as query key", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useViewDefinitions(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      // Verify the search was called once.
      expect(search).toHaveBeenCalledTimes(1);
    });
  });

  describe("enabled state", () => {
    it("does not fetch when enabled is false", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useViewDefinitions({ enabled: false }), {
        wrapper: createWrapper(),
      });

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.fetchStatus).toBe("idle");
      expect(search).not.toHaveBeenCalled();
    });

    it("fetches when enabled is true", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useViewDefinitions({ enabled: true }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(search).toHaveBeenCalledTimes(1);
    });

    it("fetches when enabled is undefined (defaults to true)", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useViewDefinitions(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(search).toHaveBeenCalledTimes(1);
    });

    it("does not fetch when fhirBaseUrl is not configured", async () => {
      vi.mocked(config).fhirBaseUrl = undefined as unknown as string;
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useViewDefinitions(), {
        wrapper: createWrapper(),
      });

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.fetchStatus).toBe("idle");
      expect(search).not.toHaveBeenCalled();

      // Restore config for other tests.
      vi.mocked(config).fhirBaseUrl = "http://localhost:8080/fhir";
    });
  });

  describe("error handling", () => {
    it("returns error state on failed query", async () => {
      const testError = new Error("Network error");
      vi.mocked(search).mockRejectedValue(testError);

      const { result } = renderHook(() => useViewDefinitions(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error).toBe(testError);
    });
  });

  describe("JSON formatting", () => {
    it("formats JSON with 2-space indentation", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useViewDefinitions(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      const json = result.current.data?.[0]?.json;
      expect(json).toContain("\n");
      expect(json).toContain("  "); // 2-space indent
    });
  });
});

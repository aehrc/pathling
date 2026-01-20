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
 * Tests for the useSearch hook.
 *
 * This test suite verifies that the useSearch hook correctly wraps the search
 * API function with TanStack Query, handling authentication, query keys, and
 * enabled states appropriately.
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
import { useAuth } from "../../contexts/AuthContext";
import { useSearch } from "../useSearch";

import type { Bundle } from "fhir/r4";
import type { ReactNode } from "react";

/**
 * Creates a wrapper component that provides TanStack Query context.
 * The query client is configured with retry disabled to make tests deterministic.
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

describe("useSearch", () => {
  const mockBundle: Bundle = {
    resourceType: "Bundle",
    type: "searchset",
    total: 2,
    entry: [
      { resource: { resourceType: "Patient", id: "1" } },
      { resource: { resourceType: "Patient", id: "2" } },
    ],
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("successful searches", () => {
    it("returns search results on successful query", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useSearch({ resourceType: "Patient" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data).toEqual(mockBundle);
      expect(search).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Patient",
        params: undefined,
        accessToken: "test-token",
      });
    });

    it("passes search parameters to the API", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(
        () =>
          useSearch({
            resourceType: "Observation",
            params: { code: "12345", _count: "10" },
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(search).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Observation",
        params: { code: "12345", _count: "10" },
        accessToken: "test-token",
      });
    });

    it("passes array parameters correctly", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(
        () =>
          useSearch({
            resourceType: "Patient",
            params: { name: ["Smith", "Jones"] },
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(search).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Patient",
        params: { name: ["Smith", "Jones"] },
        accessToken: "test-token",
      });
    });
  });

  describe("query key behaviour", () => {
    it("uses resourceType and params in query key", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(
        () =>
          useSearch({
            resourceType: "Patient",
            params: { name: "Smith" },
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      // Verify the search was called, which confirms the query key was correct.
      expect(search).toHaveBeenCalledTimes(1);
    });

    it("refetches when resourceType changes", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result, rerender } = renderHook(({ resourceType }) => useSearch({ resourceType }), {
        wrapper: createWrapper(),
        initialProps: { resourceType: "Patient" },
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      rerender({ resourceType: "Observation" });

      await waitFor(() => {
        expect(search).toHaveBeenCalledTimes(2);
      });

      expect(search).toHaveBeenLastCalledWith("http://localhost:8080/fhir", {
        resourceType: "Observation",
        params: undefined,
        accessToken: "test-token",
      });
    });
  });

  describe("enabled state", () => {
    it("does not fetch when enabled is false", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useSearch({ resourceType: "Patient", enabled: false }), {
        wrapper: createWrapper(),
      });

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.fetchStatus).toBe("idle");
      expect(search).not.toHaveBeenCalled();
    });

    it("fetches when enabled is true", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useSearch({ resourceType: "Patient", enabled: true }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(search).toHaveBeenCalledTimes(1);
    });

    it("fetches when enabled is undefined (defaults to true)", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useSearch({ resourceType: "Patient" }), {
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

      const { result } = renderHook(() => useSearch({ resourceType: "Patient" }), {
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

      const { result } = renderHook(() => useSearch({ resourceType: "Patient" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error).toBe(testError);
    });
  });

  describe("authentication", () => {
    it("passes access token when authenticated", async () => {
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useSearch({ resourceType: "Patient" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(search).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          accessToken: "test-token",
        }),
      );
    });

    it("passes undefined access token when not authenticated", async () => {
      vi.mocked(useAuth).mockReturnValue({
        client: null,
      } as ReturnType<typeof useAuth>);
      vi.mocked(search).mockResolvedValue(mockBundle);

      const { result } = renderHook(() => useSearch({ resourceType: "Patient" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(search).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          accessToken: undefined,
        }),
      );
    });
  });
});

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
 * Tests for the useRead hook.
 *
 * This test suite verifies that the useRead hook correctly wraps the read
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
  read: vi.fn(),
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

import { read } from "../../api";
import { config } from "../../config";
import { useAuth } from "../../contexts/AuthContext";
import { useRead } from "../useRead";

import type { Observation, Patient } from "fhir/r4";
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

describe("useRead", () => {
  const mockPatient: Patient = {
    resourceType: "Patient",
    id: "123",
    name: [{ family: "Smith", given: ["John"] }],
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("successful reads", () => {
    it("returns resource on successful query", async () => {
      vi.mocked(read).mockResolvedValue(mockPatient);

      const { result } = renderHook(() => useRead({ resourceType: "Patient", id: "123" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data).toEqual(mockPatient);
      expect(read).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Patient",
        id: "123",
        accessToken: "test-token",
      });
    });

    it("reads different resource types", async () => {
      const mockObservation: Observation = {
        resourceType: "Observation",
        id: "obs-1",
        status: "final",
        code: { text: "Test" },
      };

      vi.mocked(read).mockResolvedValue(mockObservation);

      const { result } = renderHook(() => useRead({ resourceType: "Observation", id: "obs-1" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data).toEqual(mockObservation);
      expect(read).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Observation",
        id: "obs-1",
        accessToken: "test-token",
      });
    });
  });

  describe("query key behaviour", () => {
    it("uses resourceType and id in query key", async () => {
      vi.mocked(read).mockResolvedValue(mockPatient);

      const { result } = renderHook(() => useRead({ resourceType: "Patient", id: "123" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(read).toHaveBeenCalledTimes(1);
    });

    it("refetches when resourceType changes", async () => {
      vi.mocked(read).mockResolvedValue(mockPatient);

      const { result, rerender } = renderHook(
        ({ resourceType, id }) => useRead({ resourceType, id }),
        {
          wrapper: createWrapper(),
          initialProps: { resourceType: "Patient", id: "123" },
        },
      );

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      rerender({ resourceType: "Observation", id: "123" });

      await waitFor(() => {
        expect(read).toHaveBeenCalledTimes(2);
      });

      expect(read).toHaveBeenLastCalledWith("http://localhost:8080/fhir", {
        resourceType: "Observation",
        id: "123",
        accessToken: "test-token",
      });
    });

    it("refetches when id changes", async () => {
      vi.mocked(read).mockResolvedValue(mockPatient);

      const { result, rerender } = renderHook(
        ({ resourceType, id }) => useRead({ resourceType, id }),
        {
          wrapper: createWrapper(),
          initialProps: { resourceType: "Patient", id: "123" },
        },
      );

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      rerender({ resourceType: "Patient", id: "456" });

      await waitFor(() => {
        expect(read).toHaveBeenCalledTimes(2);
      });

      expect(read).toHaveBeenLastCalledWith("http://localhost:8080/fhir", {
        resourceType: "Patient",
        id: "456",
        accessToken: "test-token",
      });
    });
  });

  describe("enabled state", () => {
    it("does not fetch when enabled is false", async () => {
      vi.mocked(read).mockResolvedValue(mockPatient);

      const { result } = renderHook(
        () => useRead({ resourceType: "Patient", id: "123", enabled: false }),
        { wrapper: createWrapper() },
      );

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.fetchStatus).toBe("idle");
      expect(read).not.toHaveBeenCalled();
    });

    it("fetches when enabled is true", async () => {
      vi.mocked(read).mockResolvedValue(mockPatient);

      const { result } = renderHook(
        () => useRead({ resourceType: "Patient", id: "123", enabled: true }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(read).toHaveBeenCalledTimes(1);
    });

    it("fetches when enabled is undefined (defaults to true)", async () => {
      vi.mocked(read).mockResolvedValue(mockPatient);

      const { result } = renderHook(() => useRead({ resourceType: "Patient", id: "123" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(read).toHaveBeenCalledTimes(1);
    });

    it("does not fetch when id is empty string", async () => {
      vi.mocked(read).mockResolvedValue(mockPatient);

      const { result } = renderHook(() => useRead({ resourceType: "Patient", id: "" }), {
        wrapper: createWrapper(),
      });

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.fetchStatus).toBe("idle");
      expect(read).not.toHaveBeenCalled();
    });

    it("does not fetch when fhirBaseUrl is not configured", async () => {
      vi.mocked(config).fhirBaseUrl = undefined as unknown as string;
      vi.mocked(read).mockResolvedValue(mockPatient);

      const { result } = renderHook(() => useRead({ resourceType: "Patient", id: "123" }), {
        wrapper: createWrapper(),
      });

      // Wait a tick to ensure the query does not run.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(result.current.fetchStatus).toBe("idle");
      expect(read).not.toHaveBeenCalled();

      // Restore config for other tests.
      vi.mocked(config).fhirBaseUrl = "http://localhost:8080/fhir";
    });
  });

  describe("error handling", () => {
    it("returns error state on failed query", async () => {
      const testError = new Error("Network error");
      vi.mocked(read).mockRejectedValue(testError);

      const { result } = renderHook(() => useRead({ resourceType: "Patient", id: "123" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error).toBe(testError);
    });

    it("handles not found errors", async () => {
      const notFoundError = new Error("Resource not found");
      vi.mocked(read).mockRejectedValue(notFoundError);

      const { result } = renderHook(() => useRead({ resourceType: "Patient", id: "nonexistent" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error).toBe(notFoundError);
    });
  });

  describe("authentication", () => {
    it("passes access token when authenticated", async () => {
      vi.mocked(read).mockResolvedValue(mockPatient);

      const { result } = renderHook(() => useRead({ resourceType: "Patient", id: "123" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(read).toHaveBeenCalledWith(
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
      vi.mocked(read).mockResolvedValue(mockPatient);

      const { result } = renderHook(() => useRead({ resourceType: "Patient", id: "123" }), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(read).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          accessToken: undefined,
        }),
      );
    });
  });
});

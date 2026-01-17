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
 * Tests for the useCreate hook.
 *
 * This test suite verifies that the useCreate hook correctly wraps the create
 * API function with TanStack Query's useMutation, handling authentication,
 * callbacks, and error states appropriately.
 *
 * @author John Grimes
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Mock the API module.
vi.mock("../../api", () => ({
  create: vi.fn(),
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

import { create } from "../../api";
import { useAuth } from "../../contexts/AuthContext";
import { useCreate } from "../useCreate";

import type { Observation, Patient, Resource } from "fhir/r4";
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

describe("useCreate", () => {
  const mockPatient: Patient = {
    resourceType: "Patient",
    name: [{ family: "Smith", given: ["John"] }],
  };

  const mockCreatedPatient: Patient = {
    resourceType: "Patient",
    id: "123",
    name: [{ family: "Smith", given: ["John"] }],
  };

  beforeEach(() => {
    vi.clearAllMocks();
    // Reset useAuth mock to the authenticated state before each test.
    vi.mocked(useAuth).mockReturnValue({
      client: {
        state: {
          tokenResponse: {
            access_token: "test-token",
          },
        },
      },
    } as ReturnType<typeof useAuth>);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("successful creation", () => {
    it("creates a resource and returns the created resource", async () => {
      vi.mocked(create).mockResolvedValue(mockCreatedPatient);

      const { result } = renderHook(() => useCreate(), {
        wrapper: createWrapper(),
      });

      expect(result.current.status).toBe("idle");

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data).toEqual(mockCreatedPatient);
      expect(create).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Patient",
        resource: mockPatient,
        accessToken: "test-token",
      });
    });

    it("extracts resourceType from the resource being created", async () => {
      const observation: Observation = {
        resourceType: "Observation",
        status: "final",
        code: { text: "Test" },
      };
      const createdObservation: Observation = {
        ...observation,
        id: "obs-1",
      };

      vi.mocked(create).mockResolvedValue(createdObservation);

      const { result } = renderHook(() => useCreate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: observation });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(create).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Observation",
        resource: observation,
        accessToken: "test-token",
      });
    });
  });

  describe("callbacks", () => {
    it("calls onSuccess callback with created resource", async () => {
      vi.mocked(create).mockResolvedValue(mockCreatedPatient);
      const onSuccess = vi.fn();

      const { result } = renderHook(() => useCreate({ onSuccess }), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(onSuccess).toHaveBeenCalledWith(
        mockCreatedPatient,
        { resource: mockPatient },
        undefined,
        expect.any(Object),
      );
    });

    it("calls onError callback when creation fails", async () => {
      const testError = new Error("Creation failed");
      vi.mocked(create).mockRejectedValue(testError);
      const onError = vi.fn();

      const { result } = renderHook(() => useCreate({ onError }), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(onError).toHaveBeenCalledWith(
        testError,
        { resource: mockPatient },
        undefined,
        expect.any(Object),
      );
    });

    it("works without any callbacks provided", async () => {
      vi.mocked(create).mockResolvedValue(mockCreatedPatient);

      const { result } = renderHook(() => useCreate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data).toEqual(mockCreatedPatient);
    });
  });

  describe("error handling", () => {
    it("returns error state when creation fails", async () => {
      const testError = new Error("Network error");
      vi.mocked(create).mockRejectedValue(testError);

      const { result } = renderHook(() => useCreate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error).toBe(testError);
    });

    it("can be reset after error", async () => {
      const testError = new Error("Network error");
      vi.mocked(create).mockRejectedValue(testError);

      const { result } = renderHook(() => useCreate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      act(() => {
        result.current.reset();
      });

      await waitFor(() => {
        expect(result.current.status).toBe("idle");
      });

      expect(result.current.error).toBe(null);
    });
  });

  describe("authentication", () => {
    it("passes access token when authenticated", async () => {
      vi.mocked(create).mockResolvedValue(mockCreatedPatient);

      const { result } = renderHook(() => useCreate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(create).toHaveBeenCalledWith(
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
      vi.mocked(create).mockResolvedValue(mockCreatedPatient);

      const { result } = renderHook(() => useCreate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(create).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          accessToken: undefined,
        }),
      );
    });
  });

  describe("mutation state", () => {
    it("shows pending state while creating", async () => {
      // Create a promise that we control.
      let resolveCreate: (value: Resource) => void;
      const createPromise = new Promise<Resource>((resolve) => {
        resolveCreate = resolve;
      });
      vi.mocked(create).mockReturnValue(createPromise);

      const { result } = renderHook(() => useCreate(), {
        wrapper: createWrapper(),
      });

      act(() => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isPending).toBe(true);
      });

      expect(result.current.status).toBe("pending");

      // Resolve the promise.
      await act(async () => {
        resolveCreate!(mockCreatedPatient);
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });
    });

    it("can mutate multiple times", async () => {
      vi.mocked(create).mockResolvedValue(mockCreatedPatient);

      const { result } = renderHook(() => useCreate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      // Create another resource.
      const anotherPatient: Patient = {
        resourceType: "Patient",
        name: [{ family: "Jones" }],
      };

      await act(async () => {
        result.current.mutate({ resource: anotherPatient });
      });

      await waitFor(() => {
        expect(create).toHaveBeenCalledTimes(2);
      });
    });
  });
});

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
 * Tests for the useUpdate hook.
 *
 * This test suite verifies that the useUpdate hook correctly wraps the update
 * API function with TanStack Query's useMutation, handling authentication,
 * callbacks, validation, and error states appropriately.
 *
 * @author John Grimes
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Mock the API module.
vi.mock("../../api", () => ({
  update: vi.fn(),
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

import { update } from "../../api";
import { useAuth } from "../../contexts/AuthContext";
import { useUpdate } from "../useUpdate";

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

describe("useUpdate", () => {
  const mockPatient: Patient = {
    resourceType: "Patient",
    id: "123",
    name: [{ family: "Smith", given: ["John"] }],
  };

  const mockUpdatedPatient: Patient = {
    resourceType: "Patient",
    id: "123",
    name: [{ family: "Jones", given: ["John"] }],
    meta: { versionId: "2" },
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

  describe("successful update", () => {
    it("updates a resource and returns the updated resource", async () => {
      vi.mocked(update).mockResolvedValue(mockUpdatedPatient);

      const { result } = renderHook(() => useUpdate(), {
        wrapper: createWrapper(),
      });

      expect(result.current.status).toBe("idle");

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data).toEqual(mockUpdatedPatient);
      expect(update).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Patient",
        id: "123",
        resource: mockPatient,
        accessToken: "test-token",
      });
    });

    it("updates different resource types", async () => {
      const observation: Observation = {
        resourceType: "Observation",
        id: "obs-1",
        status: "final",
        code: { text: "Updated Test" },
      };
      const updatedObservation: Observation = {
        ...observation,
        meta: { versionId: "2" },
      };

      vi.mocked(update).mockResolvedValue(updatedObservation);

      const { result } = renderHook(() => useUpdate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: observation });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(update).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Observation",
        id: "obs-1",
        resource: observation,
        accessToken: "test-token",
      });
    });
  });

  describe("validation", () => {
    it("throws error when resource has no id", async () => {
      const resourceWithoutId: Patient = {
        resourceType: "Patient",
        name: [{ family: "Smith" }],
      };

      const { result } = renderHook(() => useUpdate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: resourceWithoutId });
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error?.message).toBe("Resource must have an id for update");
      // The API should not have been called.
      expect(update).not.toHaveBeenCalled();
    });
  });

  describe("callbacks", () => {
    it("calls onSuccess callback with updated resource", async () => {
      vi.mocked(update).mockResolvedValue(mockUpdatedPatient);
      const onSuccess = vi.fn();

      const { result } = renderHook(() => useUpdate({ onSuccess }), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(onSuccess).toHaveBeenCalledWith(
        mockUpdatedPatient,
        { resource: mockPatient },
        undefined,
        expect.any(Object),
      );
    });

    it("calls onError callback when update fails", async () => {
      const testError = new Error("Update failed");
      vi.mocked(update).mockRejectedValue(testError);
      const onError = vi.fn();

      const { result } = renderHook(() => useUpdate({ onError }), {
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

    it("calls onError callback when validation fails", async () => {
      const resourceWithoutId: Patient = {
        resourceType: "Patient",
        name: [{ family: "Smith" }],
      };
      const onError = vi.fn();

      const { result } = renderHook(() => useUpdate({ onError }), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: resourceWithoutId });
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({
          message: "Resource must have an id for update",
        }),
        { resource: resourceWithoutId },
        undefined,
        expect.any(Object),
      );
    });

    it("works without any callbacks provided", async () => {
      vi.mocked(update).mockResolvedValue(mockUpdatedPatient);

      const { result } = renderHook(() => useUpdate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data).toEqual(mockUpdatedPatient);
    });
  });

  describe("error handling", () => {
    it("returns error state when update fails", async () => {
      const testError = new Error("Network error");
      vi.mocked(update).mockRejectedValue(testError);

      const { result } = renderHook(() => useUpdate(), {
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

    it("handles conflict errors", async () => {
      const conflictError = new Error("Conflict: resource was modified");
      vi.mocked(update).mockRejectedValue(conflictError);

      const { result } = renderHook(() => useUpdate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error).toBe(conflictError);
    });

    it("can be reset after error", async () => {
      const testError = new Error("Network error");
      vi.mocked(update).mockRejectedValue(testError);

      const { result } = renderHook(() => useUpdate(), {
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
      vi.mocked(update).mockResolvedValue(mockUpdatedPatient);

      const { result } = renderHook(() => useUpdate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(update).toHaveBeenCalledWith(
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
      vi.mocked(update).mockResolvedValue(mockUpdatedPatient);

      const { result } = renderHook(() => useUpdate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(update).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          accessToken: undefined,
        }),
      );
    });
  });

  describe("mutation state", () => {
    it("shows pending state while updating", async () => {
      let resolveUpdate: (value: Resource) => void;
      const updatePromise = new Promise<Resource>((resolve) => {
        resolveUpdate = resolve;
      });
      vi.mocked(update).mockReturnValue(updatePromise);

      const { result } = renderHook(() => useUpdate(), {
        wrapper: createWrapper(),
      });

      act(() => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isPending).toBe(true);
      });

      expect(result.current.status).toBe("pending");

      await act(async () => {
        resolveUpdate!(mockUpdatedPatient);
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });
    });

    it("can update the same resource multiple times", async () => {
      vi.mocked(update).mockResolvedValue(mockUpdatedPatient);

      const { result } = renderHook(() => useUpdate(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resource: mockPatient });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      const furtherUpdate: Patient = {
        ...mockPatient,
        name: [{ family: "Brown" }],
      };

      await act(async () => {
        result.current.mutate({ resource: furtherUpdate });
      });

      await waitFor(() => {
        expect(update).toHaveBeenCalledTimes(2);
      });

      expect(update).toHaveBeenLastCalledWith("http://localhost:8080/fhir", {
        resourceType: "Patient",
        id: "123",
        resource: furtherUpdate,
        accessToken: "test-token",
      });
    });
  });
});

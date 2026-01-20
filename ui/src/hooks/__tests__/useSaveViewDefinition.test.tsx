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
 * Tests for the useSaveViewDefinition hook.
 *
 * This test suite verifies that the useSaveViewDefinition hook correctly
 * creates ViewDefinition resources on the server, validates the resource type,
 * updates the query cache, and calls appropriate callbacks.
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
import { useSaveViewDefinition } from "../useSaveViewDefinition";

import type { Resource } from "fhir/r4";
import type { ReactNode } from "react";

/**
 * Creates a wrapper component that provides TanStack Query context.
 * Returns both the wrapper and the queryClient for cache inspection.
 *
 * @returns Object with wrapper function and queryClient.
 */
function createWrapperWithClient() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });
  const wrapper = function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
  return { wrapper, queryClient };
}

describe("useSaveViewDefinition", () => {
  const mockViewDefinitionJson = JSON.stringify({
    resourceType: "ViewDefinition",
    name: "PatientView",
    resource: "Patient",
    status: "active",
    select: [
      {
        column: [
          { path: "id", name: "patient_id" },
          { path: "name.given.first()", name: "first_name" },
        ],
      },
    ],
  });

  const mockCreatedResource = {
    resourceType: "ViewDefinition",
    id: "vd-123",
    name: "PatientView",
    resource: "Patient",
    status: "active",
    select: [
      {
        column: [
          { path: "id", name: "patient_id" },
          { path: "name.given.first()", name: "first_name" },
        ],
      },
    ],
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("successful save", () => {
    it("creates a ViewDefinition and returns id and name", async () => {
      vi.mocked(create).mockResolvedValue(mockCreatedResource as unknown as Resource);
      const { wrapper } = createWrapperWithClient();

      const { result } = renderHook(() => useSaveViewDefinition(), { wrapper });

      expect(result.current.status).toBe("idle");

      await act(async () => {
        result.current.mutate(mockViewDefinitionJson);
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data).toEqual({
        id: "vd-123",
        name: "PatientView",
      });

      expect(create).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "ViewDefinition",
        resource: expect.objectContaining({
          resourceType: "ViewDefinition",
          name: "PatientView",
        }),
        accessToken: "test-token",
      });
    });

    it("uses id as name when name is not present in response", async () => {
      const resourceWithoutName = {
        resourceType: "ViewDefinition",
        id: "vd-no-name",
        resource: "Patient",
        status: "active",
        select: [],
      };

      vi.mocked(create).mockResolvedValue(resourceWithoutName as unknown as Resource);
      const { wrapper } = createWrapperWithClient();

      const { result } = renderHook(() => useSaveViewDefinition(), { wrapper });

      await act(async () => {
        result.current.mutate(
          JSON.stringify({
            resourceType: "ViewDefinition",
            resource: "Patient",
            status: "active",
            select: [],
          }),
        );
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.data?.name).toBe("vd-no-name");
    });
  });

  describe("validation", () => {
    it("throws error when resourceType is not ViewDefinition", async () => {
      const { wrapper } = createWrapperWithClient();

      const { result } = renderHook(() => useSaveViewDefinition(), { wrapper });

      await act(async () => {
        result.current.mutate(
          JSON.stringify({
            resourceType: "Patient",
            name: [{ family: "Smith" }],
          }),
        );
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error?.message).toBe(
        "Invalid resource: resourceType must be 'ViewDefinition'",
      );
      expect(create).not.toHaveBeenCalled();
    });

    it("throws error when resourceType is missing", async () => {
      const { wrapper } = createWrapperWithClient();

      const { result } = renderHook(() => useSaveViewDefinition(), { wrapper });

      await act(async () => {
        result.current.mutate(
          JSON.stringify({
            name: "SomeView",
            resource: "Patient",
          }),
        );
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error?.message).toBe(
        "Invalid resource: resourceType must be 'ViewDefinition'",
      );
    });

    it("throws error for invalid JSON", async () => {
      const { wrapper } = createWrapperWithClient();

      const { result } = renderHook(() => useSaveViewDefinition(), { wrapper });

      await act(async () => {
        result.current.mutate("not valid json");
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      // The error will be a JSON parse error.
      expect(result.current.error).toBeTruthy();
    });
  });

  describe("callbacks", () => {
    it("calls onSuccess callback with id and name", async () => {
      vi.mocked(create).mockResolvedValue(mockCreatedResource as unknown as Resource);
      const { wrapper } = createWrapperWithClient();
      const onSuccess = vi.fn();

      const { result } = renderHook(() => useSaveViewDefinition({ onSuccess }), {
        wrapper,
      });

      await act(async () => {
        result.current.mutate(mockViewDefinitionJson);
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(onSuccess).toHaveBeenCalledWith({
        id: "vd-123",
        name: "PatientView",
      });
    });

    it("calls onError callback when save fails", async () => {
      const testError = new Error("Create failed");
      vi.mocked(create).mockRejectedValue(testError);
      const { wrapper } = createWrapperWithClient();
      const onError = vi.fn();

      const { result } = renderHook(() => useSaveViewDefinition({ onError }), {
        wrapper,
      });

      await act(async () => {
        result.current.mutate(mockViewDefinitionJson);
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(onError).toHaveBeenCalledWith(
        testError,
        mockViewDefinitionJson,
        undefined,
        expect.any(Object),
      );
    });

    it("calls onError callback when validation fails", async () => {
      const { wrapper } = createWrapperWithClient();
      const onError = vi.fn();

      const { result } = renderHook(() => useSaveViewDefinition({ onError }), {
        wrapper,
      });

      await act(async () => {
        result.current.mutate(JSON.stringify({ resourceType: "Patient" }));
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({
          message: "Invalid resource: resourceType must be 'ViewDefinition'",
        }),
        expect.any(String),
        undefined,
        expect.any(Object),
      );
    });
  });

  describe("cache management", () => {
    it("updates viewDefinitions cache on success", async () => {
      vi.mocked(create).mockResolvedValue(mockCreatedResource as unknown as Resource);
      const { wrapper, queryClient } = createWrapperWithClient();

      // Pre-populate the cache with existing ViewDefinitions.
      queryClient.setQueryData(
        ["viewDefinitions"],
        [{ id: "existing-1", name: "ExistingView", json: "{}" }],
      );

      const { result } = renderHook(() => useSaveViewDefinition(), { wrapper });

      await act(async () => {
        result.current.mutate(mockViewDefinitionJson);
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      // Check that the cache was updated.
      const cachedData = queryClient.getQueryData(["viewDefinitions"]) as Array<{
        id: string;
        name: string;
        json: string;
      }>;
      expect(cachedData).toHaveLength(2);
      expect(cachedData[1].id).toBe("vd-123");
      expect(cachedData[1].name).toBe("PatientView");
    });

    it("invalidates viewDefinitions query on success", async () => {
      vi.mocked(create).mockResolvedValue(mockCreatedResource as unknown as Resource);
      const { wrapper, queryClient } = createWrapperWithClient();

      const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useSaveViewDefinition(), { wrapper });

      await act(async () => {
        result.current.mutate(mockViewDefinitionJson);
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(invalidateSpy).toHaveBeenCalledWith({
        queryKey: ["viewDefinitions"],
      });
    });
  });

  describe("error handling", () => {
    it("returns error state when API call fails", async () => {
      const testError = new Error("Network error");
      vi.mocked(create).mockRejectedValue(testError);
      const { wrapper } = createWrapperWithClient();

      const { result } = renderHook(() => useSaveViewDefinition(), { wrapper });

      await act(async () => {
        result.current.mutate(mockViewDefinitionJson);
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error).toBe(testError);
    });

    it("can be reset after error", async () => {
      const testError = new Error("Network error");
      vi.mocked(create).mockRejectedValue(testError);
      const { wrapper } = createWrapperWithClient();

      const { result } = renderHook(() => useSaveViewDefinition(), { wrapper });

      await act(async () => {
        result.current.mutate(mockViewDefinitionJson);
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

      expect(result.current.error).toBeNull();
    });
  });

  describe("mutation state", () => {
    it("shows pending state while saving", async () => {
      let resolveCreate: (value: Resource) => void;
      const createPromise = new Promise<Resource>((resolve) => {
        resolveCreate = resolve;
      });
      vi.mocked(create).mockReturnValue(createPromise);
      const { wrapper } = createWrapperWithClient();

      const { result } = renderHook(() => useSaveViewDefinition(), { wrapper });

      act(() => {
        result.current.mutate(mockViewDefinitionJson);
      });

      await waitFor(() => {
        expect(result.current.isPending).toBe(true);
      });

      expect(result.current.status).toBe("pending");

      await act(async () => {
        resolveCreate!(mockCreatedResource as unknown as Resource);
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(result.current.isPending).toBe(false);
    });
  });
});

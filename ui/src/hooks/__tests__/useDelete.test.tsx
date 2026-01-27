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
 * Tests for the useDelete hook.
 *
 * This test suite verifies that the useDelete hook correctly wraps the
 * deleteResource API function with TanStack Query's useMutation, handling
 * authentication, callbacks, and error states appropriately.
 *
 * @author John Grimes
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Mock the API module.
vi.mock("../../api", () => ({
  deleteResource: vi.fn(),
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

import { deleteResource } from "../../api";
import { useAuth } from "../../contexts/AuthContext";
import { useDelete } from "../useDelete";

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

describe("useDelete", () => {
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

  describe("successful deletion", () => {
    it("deletes a resource successfully", async () => {
      vi.mocked(deleteResource).mockResolvedValue(undefined);

      const { result } = renderHook(() => useDelete(), {
        wrapper: createWrapper(),
      });

      expect(result.current.status).toBe("idle");

      await act(async () => {
        result.current.mutate({ resourceType: "Patient", id: "123" });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(deleteResource).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Patient",
        id: "123",
        accessToken: "test-token",
      });
    });

    it("deletes different resource types", async () => {
      vi.mocked(deleteResource).mockResolvedValue(undefined);

      const { result } = renderHook(() => useDelete(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resourceType: "Observation", id: "obs-456" });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(deleteResource).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        resourceType: "Observation",
        id: "obs-456",
        accessToken: "test-token",
      });
    });
  });

  describe("callbacks", () => {
    it("calls onSuccess callback after successful deletion", async () => {
      vi.mocked(deleteResource).mockResolvedValue(undefined);
      const onSuccess = vi.fn();

      const { result } = renderHook(() => useDelete({ onSuccess }), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resourceType: "Patient", id: "123" });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(onSuccess).toHaveBeenCalledWith(
        undefined,
        { resourceType: "Patient", id: "123" },
        undefined,
        expect.any(Object),
      );
    });

    it("calls onError callback when deletion fails", async () => {
      const testError = new Error("Deletion failed");
      vi.mocked(deleteResource).mockRejectedValue(testError);
      const onError = vi.fn();

      const { result } = renderHook(() => useDelete({ onError }), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resourceType: "Patient", id: "123" });
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(onError).toHaveBeenCalledWith(
        testError,
        { resourceType: "Patient", id: "123" },
        undefined,
        expect.any(Object),
      );
    });

    it("works without any callbacks provided", async () => {
      vi.mocked(deleteResource).mockResolvedValue(undefined);

      const { result } = renderHook(() => useDelete(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resourceType: "Patient", id: "123" });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(deleteResource).toHaveBeenCalledTimes(1);
    });
  });

  describe("error handling", () => {
    it("returns error state when deletion fails", async () => {
      const testError = new Error("Network error");
      vi.mocked(deleteResource).mockRejectedValue(testError);

      const { result } = renderHook(() => useDelete(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resourceType: "Patient", id: "123" });
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error).toBe(testError);
    });

    it("handles not found errors", async () => {
      const notFoundError = new Error("Resource not found");
      vi.mocked(deleteResource).mockRejectedValue(notFoundError);

      const { result } = renderHook(() => useDelete(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resourceType: "Patient", id: "nonexistent" });
      });

      await waitFor(() => {
        expect(result.current.isError).toBe(true);
      });

      expect(result.current.error).toBe(notFoundError);
    });

    it("can be reset after error", async () => {
      const testError = new Error("Network error");
      vi.mocked(deleteResource).mockRejectedValue(testError);

      const { result } = renderHook(() => useDelete(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resourceType: "Patient", id: "123" });
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
      vi.mocked(deleteResource).mockResolvedValue(undefined);

      const { result } = renderHook(() => useDelete(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resourceType: "Patient", id: "123" });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(deleteResource).toHaveBeenCalledWith(
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
      vi.mocked(deleteResource).mockResolvedValue(undefined);

      const { result } = renderHook(() => useDelete(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resourceType: "Patient", id: "123" });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      expect(deleteResource).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          accessToken: undefined,
        }),
      );
    });
  });

  describe("mutation state", () => {
    it("shows pending state while deleting", async () => {
      let resolveDelete: () => void;
      const deletePromise = new Promise<void>((resolve) => {
        resolveDelete = resolve;
      });
      vi.mocked(deleteResource).mockReturnValue(deletePromise);

      const { result } = renderHook(() => useDelete(), {
        wrapper: createWrapper(),
      });

      act(() => {
        result.current.mutate({ resourceType: "Patient", id: "123" });
      });

      await waitFor(() => {
        expect(result.current.isPending).toBe(true);
      });

      expect(result.current.status).toBe("pending");

      await act(async () => {
        resolveDelete!();
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });
    });

    it("can delete multiple resources sequentially", async () => {
      vi.mocked(deleteResource).mockResolvedValue(undefined);

      const { result } = renderHook(() => useDelete(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.mutate({ resourceType: "Patient", id: "1" });
      });

      await waitFor(() => {
        expect(result.current.isSuccess).toBe(true);
      });

      await act(async () => {
        result.current.mutate({ resourceType: "Patient", id: "2" });
      });

      await waitFor(() => {
        expect(deleteResource).toHaveBeenCalledTimes(2);
      });

      expect(deleteResource).toHaveBeenNthCalledWith(
        1,
        "http://localhost:8080/fhir",
        expect.objectContaining({ id: "1" }),
      );
      expect(deleteResource).toHaveBeenNthCalledWith(
        2,
        "http://localhost:8080/fhir",
        expect.objectContaining({ id: "2" }),
      );
    });
  });
});

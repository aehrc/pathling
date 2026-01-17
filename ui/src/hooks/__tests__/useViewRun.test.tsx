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
 * Tests for the useViewRun hook.
 *
 * This test suite verifies that the useViewRun hook correctly executes
 * ViewDefinitions, both inline (by JSON) and stored (by ID), and parses
 * the NDJSON response into columns and rows.
 *
 * @author John Grimes
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";


// Mock the API module.
vi.mock("../../api", () => ({
  viewRun: vi.fn(),
  viewRunStored: vi.fn(),
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

// Mock the NDJSON utilities.
vi.mock("../../utils/ndjson", () => ({
  streamToText: vi.fn(),
  parseNdjsonResponse: vi.fn(),
  extractColumns: vi.fn(),
}));

import { viewRun, viewRunStored } from "../../api";
import { config } from "../../config";
import { streamToText, parseNdjsonResponse, extractColumns } from "../../utils/ndjson";
import { useViewRun } from "../useViewRun";

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

describe("useViewRun", () => {
  const mockStream = new ReadableStream();
  const mockNdjsonText = '{"id":"1","name":"John"}\n{"id":"2","name":"Jane"}\n';
  const mockRows = [
    { id: "1", name: "John" },
    { id: "2", name: "Jane" },
  ];
  const mockColumns = ["id", "name"];

  const mockViewDefinitionJson = JSON.stringify({
    resourceType: "ViewDefinition",
    name: "TestView",
    resource: "Patient",
    status: "active",
    select: [
      {
        column: [
          { path: "id", name: "id" },
          { path: "name.given.first()", name: "name" },
        ],
      },
    ],
  });

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(streamToText).mockResolvedValue(mockNdjsonText);
    vi.mocked(parseNdjsonResponse).mockReturnValue(mockRows);
    vi.mocked(extractColumns).mockReturnValue(mockColumns);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("hook initialisation", () => {
    it("returns all expected properties", () => {
      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      expect(result.current).toHaveProperty("status");
      expect(result.current).toHaveProperty("result");
      expect(result.current).toHaveProperty("error");
      expect(result.current).toHaveProperty("lastRequest");
      expect(result.current).toHaveProperty("execute");
      expect(result.current).toHaveProperty("reset");
      expect(result.current).toHaveProperty("isPending");
    });

    it("starts with idle status", () => {
      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      expect(result.current.status).toBe("idle");
      expect(result.current.result).toBeUndefined();
      expect(result.current.error).toBeNull();
      expect(result.current.lastRequest).toBeUndefined();
      expect(result.current.isPending).toBe(false);
    });
  });

  describe("inline mode execution", () => {
    it("executes ViewDefinition by JSON", async () => {
      vi.mocked(viewRun).mockResolvedValue(mockStream);

      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.execute({
          mode: "inline",
          viewDefinitionJson: mockViewDefinitionJson,
          limit: 20,
        });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("success");
      });

      expect(viewRun).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        viewDefinition: JSON.parse(mockViewDefinitionJson),
        limit: 20,
        accessToken: "test-token",
      });

      expect(result.current.result).toEqual({
        columns: mockColumns,
        rows: mockRows,
      });
    });

    it("uses default limit of 10", async () => {
      vi.mocked(viewRun).mockResolvedValue(mockStream);

      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.execute({
          mode: "inline",
          viewDefinitionJson: mockViewDefinitionJson,
        });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("success");
      });

      expect(viewRun).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ limit: 10 }),
      );
    });
  });

  describe("stored mode execution", () => {
    it("executes ViewDefinition by ID", async () => {
      vi.mocked(viewRunStored).mockResolvedValue(mockStream);

      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.execute({
          mode: "stored",
          viewDefinitionId: "vd-123",
          limit: 50,
        });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("success");
      });

      expect(viewRunStored).toHaveBeenCalledWith("http://localhost:8080/fhir", {
        viewDefinitionId: "vd-123",
        limit: 50,
        accessToken: "test-token",
      });

      expect(result.current.result).toEqual({
        columns: mockColumns,
        rows: mockRows,
      });
    });

    it("uses default limit of 10 for stored mode", async () => {
      vi.mocked(viewRunStored).mockResolvedValue(mockStream);

      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.execute({
          mode: "stored",
          viewDefinitionId: "vd-123",
        });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("success");
      });

      expect(viewRunStored).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ limit: 10 }),
      );
    });
  });

  describe("lastRequest tracking", () => {
    it("tracks the last executed request", async () => {
      vi.mocked(viewRunStored).mockResolvedValue(mockStream);

      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      const request = {
        mode: "stored" as const,
        viewDefinitionId: "vd-123",
        limit: 15,
      };

      await act(async () => {
        result.current.execute(request);
      });

      await waitFor(() => {
        expect(result.current.status).toBe("success");
      });

      expect(result.current.lastRequest).toEqual(request);
    });
  });

  describe("callbacks", () => {
    it("calls onSuccess callback with result", async () => {
      vi.mocked(viewRunStored).mockResolvedValue(mockStream);
      const onSuccess = vi.fn();

      const { result } = renderHook(() => useViewRun({ onSuccess }), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.execute({
          mode: "stored",
          viewDefinitionId: "vd-123",
        });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("success");
      });

      expect(onSuccess).toHaveBeenCalledWith(
        { columns: mockColumns, rows: mockRows },
        expect.objectContaining({ mode: "stored", viewDefinitionId: "vd-123" }),
        undefined,
        expect.any(Object),
      );
    });

    it("calls onError callback when execution fails", async () => {
      const testError = new Error("ViewDefinition execution failed");
      vi.mocked(viewRunStored).mockRejectedValue(testError);
      const onError = vi.fn();

      const { result } = renderHook(() => useViewRun({ onError }), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.execute({
          mode: "stored",
          viewDefinitionId: "vd-123",
        });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("error");
      });

      expect(onError).toHaveBeenCalledWith(
        testError,
        expect.objectContaining({ mode: "stored", viewDefinitionId: "vd-123" }),
        undefined,
        expect.any(Object),
      );
    });
  });

  describe("error handling", () => {
    it("handles missing FHIR base URL", async () => {
      vi.mocked(config).fhirBaseUrl = undefined as unknown as string;

      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.execute({
          mode: "stored",
          viewDefinitionId: "vd-123",
        });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("error");
      });

      expect(result.current.error?.message).toBe("FHIR base URL is not configured");

      // Restore config.
      vi.mocked(config).fhirBaseUrl = "http://localhost:8080/fhir";
    });

    it("handles invalid request without view definition", async () => {
      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.execute({
          mode: "stored",
          // Missing viewDefinitionId
        });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("error");
      });

      expect(result.current.error?.message).toBe(
        "Invalid request: missing view definition ID or JSON",
      );
    });

    it("handles invalid inline request without JSON", async () => {
      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.execute({
          mode: "inline",
          // Missing viewDefinitionJson
        });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("error");
      });

      expect(result.current.error?.message).toBe(
        "Invalid request: missing view definition ID or JSON",
      );
    });

    it("handles JSON parse errors", async () => {
      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.execute({
          mode: "inline",
          viewDefinitionJson: "not valid json",
        });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("error");
      });

      // The error will be a JSON parse error.
      expect(result.current.error).toBeTruthy();
    });
  });

  describe("reset function", () => {
    it("resets all state", async () => {
      vi.mocked(viewRunStored).mockResolvedValue(mockStream);

      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      await act(async () => {
        result.current.execute({
          mode: "stored",
          viewDefinitionId: "vd-123",
        });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("success");
      });

      expect(result.current.result).toBeTruthy();
      expect(result.current.lastRequest).toBeTruthy();

      await act(async () => {
        result.current.reset();
      });

      expect(result.current.status).toBe("idle");
      expect(result.current.result).toBeUndefined();
      expect(result.current.lastRequest).toBeUndefined();
    });
  });

  describe("pending state", () => {
    it("shows pending state while executing", async () => {
      let resolveViewRun: (value: ReadableStream) => void;
      const viewRunPromise = new Promise<ReadableStream>((resolve) => {
        resolveViewRun = resolve;
      });
      vi.mocked(viewRunStored).mockReturnValue(viewRunPromise);

      const { result } = renderHook(() => useViewRun(), {
        wrapper: createWrapper(),
      });

      act(() => {
        result.current.execute({
          mode: "stored",
          viewDefinitionId: "vd-123",
        });
      });

      await waitFor(() => {
        expect(result.current.isPending).toBe(true);
      });

      expect(result.current.status).toBe("pending");

      await act(async () => {
        resolveViewRun!(mockStream);
      });

      await waitFor(() => {
        expect(result.current.status).toBe("success");
      });

      expect(result.current.isPending).toBe(false);
    });
  });
});

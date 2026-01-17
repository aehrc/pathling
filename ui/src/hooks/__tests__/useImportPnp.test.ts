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
 * Tests for the useImportPnp hook.
 *
 * This test suite verifies that the useImportPnp hook correctly wraps the async
 * job execution for passthrough (PnP) import operations, including kick-off,
 * status polling, and cancellation with bulk export passthrough parameters.
 *
 * @author John Grimes
 */

import { renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mockStartWith = vi.fn();
const mockCancel = vi.fn();
const mockDeleteJob = vi.fn();
const mockReset = vi.fn();

// Mock the useAsyncJob hook.
vi.mock("../useAsyncJob", () => ({
  useAsyncJob: vi.fn(() => ({
    startWith: mockStartWith,
    cancel: mockCancel,
    deleteJob: mockDeleteJob,
    reset: mockReset,
    status: "idle",
    result: undefined,
    error: undefined,
    progress: undefined,
    request: undefined,
  })),
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

// Mock the useAsyncJobCallbacks hook.
vi.mock("../useAsyncJobCallbacks", () => ({
  useAsyncJobCallbacks: vi.fn((opts) => opts),
}));

import { useAsyncJob } from "../useAsyncJob";
import { useImportPnp } from "../useImportPnp";

import type { ImportPnpJobRequest } from "../useImportPnp";

describe("useImportPnp", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("hook initialisation", () => {
    it("calls useAsyncJob on mount", () => {
      renderHook(() => useImportPnp());

      expect(useAsyncJob).toHaveBeenCalled();
    });

    it("returns all expected properties from useAsyncJob", () => {
      const { result } = renderHook(() => useImportPnp());

      expect(result.current).toHaveProperty("startWith");
      expect(result.current).toHaveProperty("cancel");
      expect(result.current).toHaveProperty("deleteJob");
      expect(result.current).toHaveProperty("reset");
      expect(result.current).toHaveProperty("status");
      expect(result.current).toHaveProperty("result");
      expect(result.current).toHaveProperty("error");
    });

    it("starts with idle status", () => {
      const { result } = renderHook(() => useImportPnp());

      expect(result.current.status).toBe("idle");
    });
  });

  describe("startWith function", () => {
    it("exposes startWith function from useAsyncJob", () => {
      const { result } = renderHook(() => useImportPnp());

      expect(result.current.startWith).toBe(mockStartWith);
    });

    it("can be called with basic PnP import request", () => {
      const { result } = renderHook(() => useImportPnp());

      const request: ImportPnpJobRequest = {
        exportUrl: "http://remote-server.com/fhir/$export",
        saveMode: "overwrite",
        inputFormat: "application/fhir+ndjson",
      };

      result.current.startWith(request);

      expect(mockStartWith).toHaveBeenCalledWith(request);
    });

    it("can be called with bulk export passthrough parameters", () => {
      const { result } = renderHook(() => useImportPnp());

      const request: ImportPnpJobRequest = {
        exportUrl: "http://remote-server.com/fhir/$export",
        saveMode: "merge",
        inputFormat: "application/fhir+ndjson",
        types: ["Patient", "Observation", "Condition"],
        since: "2024-01-01T00:00:00Z",
        until: "2024-12-31T23:59:59Z",
        elements: "id,name,birthDate",
        typeFilters: ["Patient?active=true", "Observation?status=final"],
        includeAssociatedData: ["LatestProvenanceResources"],
      };

      result.current.startWith(request);

      expect(mockStartWith).toHaveBeenCalledWith(request);
    });

    it("can be called with partial passthrough parameters", () => {
      const { result } = renderHook(() => useImportPnp());

      const request: ImportPnpJobRequest = {
        exportUrl: "http://remote-server.com/fhir/$export",
        saveMode: "overwrite",
        inputFormat: "application/fhir+ndjson",
        types: ["Patient"],
        since: "2024-06-01T00:00:00Z",
      };

      result.current.startWith(request);

      expect(mockStartWith).toHaveBeenCalledWith(request);
    });
  });

  describe("cancel function", () => {
    it("exposes cancel function from useAsyncJob", () => {
      const { result } = renderHook(() => useImportPnp());

      expect(result.current.cancel).toBe(mockCancel);
    });

    it("calls cancel when invoked", async () => {
      mockCancel.mockResolvedValue(undefined);

      const { result } = renderHook(() => useImportPnp());

      await result.current.cancel();

      expect(mockCancel).toHaveBeenCalledTimes(1);
    });
  });

  describe("deleteJob function", () => {
    it("exposes deleteJob function from useAsyncJob", () => {
      const { result } = renderHook(() => useImportPnp());

      expect(result.current.deleteJob).toBe(mockDeleteJob);
    });

    it("calls deleteJob when invoked", async () => {
      mockDeleteJob.mockResolvedValue(undefined);

      const { result } = renderHook(() => useImportPnp());

      await result.current.deleteJob();

      expect(mockDeleteJob).toHaveBeenCalledTimes(1);
    });
  });

  describe("reset function", () => {
    it("exposes reset function from useAsyncJob", () => {
      const { result } = renderHook(() => useImportPnp());

      expect(result.current.reset).toBe(mockReset);
    });

    it("calls reset when invoked", () => {
      const { result } = renderHook(() => useImportPnp());

      result.current.reset();

      expect(mockReset).toHaveBeenCalledTimes(1);
    });
  });

  describe("callbacks", () => {
    it("passes callbacks to useAsyncJob", () => {
      const onProgress = vi.fn();
      const onComplete = vi.fn();
      const onError = vi.fn();

      renderHook(() =>
        useImportPnp({
          onProgress,
          onComplete,
          onError,
        }),
      );

      expect(useAsyncJob).toHaveBeenCalledWith(
        expect.any(Function),
        expect.objectContaining({
          onProgress,
          onComplete,
          onError,
        }),
      );
    });

    it("works without callbacks", () => {
      renderHook(() => useImportPnp());

      expect(useAsyncJob).toHaveBeenCalled();
    });
  });

  describe("status states", () => {
    it("reflects in-progress status from useAsyncJob", () => {
      vi.mocked(useAsyncJob).mockReturnValue({
        startWith: mockStartWith,
        cancel: mockCancel,
        deleteJob: mockDeleteJob,
        reset: mockReset,
        status: "in-progress",
        result: undefined,
        error: undefined,
        progress: 25,
        request: undefined,
      });

      const { result } = renderHook(() => useImportPnp());

      expect(result.current.status).toBe("in-progress");
      expect(result.current.progress).toBe(25);
    });

    it("reflects complete status from useAsyncJob", () => {
      vi.mocked(useAsyncJob).mockReturnValue({
        startWith: mockStartWith,
        cancel: mockCancel,
        deleteJob: mockDeleteJob,
        reset: mockReset,
        status: "complete",
        result: undefined,
        error: undefined,
        progress: 100,
        request: undefined,
      });

      const { result } = renderHook(() => useImportPnp());

      expect(result.current.status).toBe("complete");
    });

    it("reflects error status from useAsyncJob", () => {
      const testError = new Error("PnP import failed: remote server error");
      vi.mocked(useAsyncJob).mockReturnValue({
        startWith: mockStartWith,
        cancel: mockCancel,
        deleteJob: mockDeleteJob,
        reset: mockReset,
        status: "error",
        result: undefined,
        error: testError,
        progress: undefined,
        request: undefined,
      });

      const { result } = renderHook(() => useImportPnp());

      expect(result.current.status).toBe("error");
      expect(result.current.error).toBe(testError);
    });

    it("reflects cancelled status from useAsyncJob", () => {
      vi.mocked(useAsyncJob).mockReturnValue({
        startWith: mockStartWith,
        cancel: mockCancel,
        deleteJob: mockDeleteJob,
        reset: mockReset,
        status: "cancelled",
        result: undefined,
        error: undefined,
        progress: undefined,
        request: undefined,
      });

      const { result } = renderHook(() => useImportPnp());

      expect(result.current.status).toBe("cancelled");
    });
  });
});

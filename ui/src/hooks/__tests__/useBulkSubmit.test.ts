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
 * Tests for the useBulkSubmit hook.
 *
 * This test suite verifies that the useBulkSubmit hook correctly wraps the async
 * job execution for bulk submit operations, including submit mode, monitor mode,
 * download functionality, and cancellation.
 *
 * @author John Grimes
 */

import { renderHook, waitFor } from "@testing-library/react";
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

// Mock the API module.
vi.mock("../../api", () => ({
  bulkSubmitDownload: vi.fn(),
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

import { bulkSubmitDownload } from "../../api";
import { useAsyncJob } from "../useAsyncJob";
import { useBulkSubmit } from "../useBulkSubmit";

import type {
  BulkSubmitRequest,
  BulkSubmitMonitorRequest,
  BulkSubmitManifest,
} from "../useBulkSubmit";

describe("useBulkSubmit", () => {
  const mockManifest: BulkSubmitManifest = {
    transactionTime: "2024-01-15T10:00:00Z",
    request: "http://localhost:8080/fhir/$bulk-submit",
    output: [
      {
        type: "Patient",
        url: "http://localhost:8080/Patient.ndjson",
        count: 100,
      },
      {
        type: "Observation",
        url: "http://localhost:8080/Observation.ndjson",
        count: 500,
      },
    ],
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("hook initialisation", () => {
    it("calls useAsyncJob on mount", () => {
      renderHook(() => useBulkSubmit());

      expect(useAsyncJob).toHaveBeenCalled();
    });

    it("returns all expected properties", () => {
      const { result } = renderHook(() => useBulkSubmit());

      expect(result.current).toHaveProperty("startWith");
      expect(result.current).toHaveProperty("cancel");
      expect(result.current).toHaveProperty("deleteJob");
      expect(result.current).toHaveProperty("reset");
      expect(result.current).toHaveProperty("status");
      expect(result.current).toHaveProperty("result");
      expect(result.current).toHaveProperty("error");
      expect(result.current).toHaveProperty("download");
    });

    it("starts with idle status", () => {
      const { result } = renderHook(() => useBulkSubmit());

      expect(result.current.status).toBe("idle");
    });
  });

  describe("startWith function - submit mode", () => {
    it("can be called with submit mode request", () => {
      const { result } = renderHook(() => useBulkSubmit());

      const request: BulkSubmitRequest = {
        mode: "submit",
        submissionId: "sub-123",
        submitter: { system: "http://example.org", value: "submitter-1" },
        manifestUrl: "http://example.com/manifest.json",
      };

      result.current.startWith(request);

      expect(mockStartWith).toHaveBeenCalledWith(request);
    });

    it("can be called with full submit mode parameters", () => {
      const { result } = renderHook(() => useBulkSubmit());

      const request: BulkSubmitRequest = {
        mode: "submit",
        submissionId: "sub-456",
        submitter: { system: "http://example.org", value: "submitter-2" },
        manifestUrl: "http://example.com/manifest.json",
        fhirBaseUrl: "http://source-fhir.example.com/fhir",
        replacesManifestUrl: "http://example.com/old-manifest.json",
        oauthMetadataUrl: "http://auth.example.com/.well-known/oauth",
        metadata: { key1: "value1", key2: "value2" },
        fileRequestHeaders: { "X-Custom-Header": "custom-value" },
      };

      result.current.startWith(request);

      expect(mockStartWith).toHaveBeenCalledWith(request);
    });
  });

  describe("startWith function - monitor mode", () => {
    it("can be called with monitor mode request", () => {
      const { result } = renderHook(() => useBulkSubmit());

      const request: BulkSubmitMonitorRequest = {
        mode: "monitor",
        submissionId: "sub-789",
        submitter: { system: "http://example.org", value: "submitter-1" },
      };

      result.current.startWith(request);

      expect(mockStartWith).toHaveBeenCalledWith(request);
    });
  });

  describe("download function", () => {
    it("provides a download function", () => {
      const { result } = renderHook(() => useBulkSubmit());

      expect(typeof result.current.download).toBe("function");
    });

    it("throws error when no submission ID is available", async () => {
      const { result } = renderHook(() => useBulkSubmit());

      await expect(result.current.download("output.ndjson")).rejects.toThrow(
        "No submission ID available",
      );
    });

    it("calls bulkSubmitDownload with correct parameters after job starts", async () => {
      // Create a mock stream.
      const mockStream = new ReadableStream();
      vi.mocked(bulkSubmitDownload).mockResolvedValue(mockStream);

      // We need to simulate that startWith was called and stored the submission ID.
      // The actual implementation stores submissionId in a ref when startWith is called.
      // Since we mock useAsyncJob, we need to verify the download logic independently.

      // First, let's capture the buildOptions function passed to useAsyncJob.
      let capturedBuildOptions: (request: unknown) => unknown;
      vi.mocked(useAsyncJob).mockImplementation((buildOptions) => {
        capturedBuildOptions = buildOptions as (request: unknown) => unknown;
        return {
          startWith: mockStartWith,
          cancel: mockCancel,
          deleteJob: mockDeleteJob,
          reset: mockReset,
          status: "idle",
          result: undefined,
          error: undefined,
          progress: undefined,
          request: undefined,
        };
      });

      const { result } = renderHook(() => useBulkSubmit());

      // Call the buildOptions with a submit request to set the submission ID.
      const submitRequest: BulkSubmitRequest = {
        mode: "submit",
        submissionId: "test-sub-id",
        submitter: { system: "http://example.org", value: "test" },
        manifestUrl: "http://example.com/manifest.json",
      };

      capturedBuildOptions!(submitRequest);

      // Now the download function should work.
      await result.current.download("Patient.ndjson");

      await waitFor(() => {
        expect(bulkSubmitDownload).toHaveBeenCalledWith(
          "http://localhost:8080/fhir",
          expect.objectContaining({
            submissionId: "test-sub-id",
            fileName: "Patient.ndjson",
            accessToken: "test-token",
          }),
        );
      });
    });
  });

  describe("cancel function", () => {
    it("exposes cancel function from useAsyncJob", () => {
      const { result } = renderHook(() => useBulkSubmit());

      expect(result.current.cancel).toBe(mockCancel);
    });
  });

  describe("deleteJob function", () => {
    it("exposes deleteJob function from useAsyncJob", () => {
      const { result } = renderHook(() => useBulkSubmit());

      expect(result.current.deleteJob).toBe(mockDeleteJob);
    });
  });

  describe("reset function", () => {
    it("exposes reset function from useAsyncJob", () => {
      const { result } = renderHook(() => useBulkSubmit());

      expect(result.current.reset).toBe(mockReset);
    });
  });

  describe("callbacks", () => {
    it("passes callbacks to useAsyncJob", () => {
      const onProgress = vi.fn();
      const onComplete = vi.fn();
      const onError = vi.fn();

      renderHook(() =>
        useBulkSubmit({
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
  });

  describe("status states", () => {
    it("reflects complete status with manifest result", () => {
      vi.mocked(useAsyncJob).mockReturnValue({
        startWith: mockStartWith,
        cancel: mockCancel,
        deleteJob: mockDeleteJob,
        reset: mockReset,
        status: "complete",
        result: mockManifest,
        error: undefined,
        progress: 100,
        request: undefined,
      });

      const { result } = renderHook(() => useBulkSubmit());

      expect(result.current.status).toBe("complete");
      expect(result.current.result).toEqual(mockManifest);
    });

    it("reflects error status", () => {
      const testError = new Error("Bulk submit failed");
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

      const { result } = renderHook(() => useBulkSubmit());

      expect(result.current.status).toBe("error");
      expect(result.current.error).toBe(testError);
    });
  });
});

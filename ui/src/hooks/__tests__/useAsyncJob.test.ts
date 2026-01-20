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
 * Tests for the useAsyncJob hook.
 *
 * @author John Grimes
 */

import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Mock the API module.
vi.mock("../../api", () => ({
  executeAsyncJob: vi.fn(),
  parseProgressHeader: vi.fn((value: string) => {
    const num = parseInt(value, 10);
    return isNaN(num) ? undefined : num;
  }),
}));

// Import the mock to control it.
import { executeAsyncJob } from "../../api";
import { useAsyncJob } from "../useAsyncJob";

import type { AsyncJobExecutorOptions } from "../../api";

interface TestRequest {
  data: string;
}

interface TestKickOffResult {
  jobId: string;
}

interface TestStatusResult {
  status: string;
  progress?: string;
  result?: string;
}

type TestResult = string;

describe("useAsyncJob", () => {
  const mockCancel = vi.fn().mockResolvedValue(undefined);

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // Helper to create mock executeAsyncJob return value.
  function setupExecuteAsyncJobMock(options: {
    result?: Promise<TestResult>;
    cancel?: typeof mockCancel;
  }) {
    const cancel = options.cancel ?? mockCancel;
    const result = options.result ?? Promise.resolve("success");

    vi.mocked(executeAsyncJob).mockReturnValue({
      cancel,
      result,
    });

    return { cancel, result };
  }

  // Helper to build a standard test hook.
  function buildTestHook() {
    const buildOptions = vi.fn(
      (): Omit<
        AsyncJobExecutorOptions<
          TestKickOffResult,
          TestStatusResult,
          TestResult
        >,
        "onProgress"
      > => ({
        kickOff: vi.fn().mockResolvedValue({ jobId: "job-123" }),
        getJobId: (r) => r.jobId,
        checkStatus: vi.fn().mockResolvedValue({
          status: "complete",
          result: "success",
        }),
        isComplete: (s) => s.status === "complete",
        getResult: (s) => s.result!,
        cancel: mockCancel,
        pollingInterval: 1000,
      }),
    );

    return renderHook(() =>
      useAsyncJob<TestRequest, TestKickOffResult, TestStatusResult, TestResult>(
        buildOptions,
      ),
    );
  }

  it("starts with idle status", () => {
    setupExecuteAsyncJobMock({});
    const { result } = buildTestHook();
    expect(result.current.status).toBe("idle");
  });

  it("provides a cancel function that sets status to cancelled", async () => {
    setupExecuteAsyncJobMock({});
    const { result } = buildTestHook();

    await act(async () => {
      result.current.startWith({ data: "test" });
    });

    await act(async () => {
      await result.current.cancel();
    });

    expect(mockCancel).toHaveBeenCalledTimes(1);
    expect(result.current.status).toBe("cancelled");
  });

  describe("deleteJob", () => {
    it("calls the cancel function without changing status", async () => {
      // Set up a completed job scenario.
      setupExecuteAsyncJobMock({ result: Promise.resolve("success") });

      const { result } = buildTestHook();

      await act(async () => {
        result.current.startWith({ data: "test" });
      });

      // Wait for the job to complete.
      await waitFor(() => {
        expect(result.current.status).toBe("complete");
      });

      // Call deleteJob and verify it calls cancel but does NOT change status.
      await act(async () => {
        await result.current.deleteJob();
      });

      expect(mockCancel).toHaveBeenCalledTimes(1);
      // Status should remain "complete" - deleteJob does not change it.
      expect(result.current.status).toBe("complete");
    });

    it("resolves after DELETE completes", async () => {
      let resolveCancel: () => void;
      const cancelPromise = new Promise<void>((resolve) => {
        resolveCancel = resolve;
      });
      const delayedCancel = vi.fn().mockImplementation(() => cancelPromise);

      setupExecuteAsyncJobMock({
        result: Promise.resolve("success"),
        cancel: delayedCancel,
      });

      const { result } = buildTestHook();

      await act(async () => {
        result.current.startWith({ data: "test" });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("complete");
      });

      // Start deleteJob but don't await yet.
      let deleteResolved = false;
      const deletePromise = act(async () => {
        await result.current.deleteJob();
        deleteResolved = true;
      });

      // The delete should not have resolved yet.
      expect(deleteResolved).toBe(false);

      // Now resolve the cancel.
      await act(async () => {
        resolveCancel!();
      });

      await deletePromise;

      // Now it should have resolved.
      expect(deleteResolved).toBe(true);
      expect(delayedCancel).toHaveBeenCalledTimes(1);
    });

    it("can be called even when job is in error state", async () => {
      // Set up an error scenario.
      const errorResult = Promise.reject(new Error("Test error"));
      // Prevent unhandled rejection warning.
      errorResult.catch(() => {});

      setupExecuteAsyncJobMock({ result: errorResult });

      const { result } = buildTestHook();

      await act(async () => {
        result.current.startWith({ data: "test" });
      });

      await waitFor(() => {
        expect(result.current.status).toBe("error");
      });

      // Call deleteJob and verify it calls cancel.
      await act(async () => {
        await result.current.deleteJob();
      });

      expect(mockCancel).toHaveBeenCalledTimes(1);
      // Status should remain "error".
      expect(result.current.status).toBe("error");
    });

    it("does nothing when no job has been started", async () => {
      setupExecuteAsyncJobMock({});
      const { result } = buildTestHook();

      // Call deleteJob before starting any job.
      await act(async () => {
        await result.current.deleteJob();
      });

      // Cancel should not have been called.
      expect(mockCancel).not.toHaveBeenCalled();
      expect(result.current.status).toBe("idle");
    });
  });
});

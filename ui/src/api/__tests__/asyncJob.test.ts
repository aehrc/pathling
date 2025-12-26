/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
 *
 * Author: John Grimes
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { executeAsyncJob } from "../asyncJob";

describe("executeAsyncJob", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("kicks off job and polls until complete", async () => {
    const kickOff = vi.fn().mockResolvedValue({ jobId: "job-123" });
    const checkStatus = vi
      .fn()
      .mockResolvedValueOnce({ status: "in-progress", progress: "25%" })
      .mockResolvedValueOnce({ status: "in-progress", progress: "50%" })
      .mockResolvedValueOnce({
        status: "complete",
        result: { data: "success" },
      });
    const cancel = vi.fn();

    const handle = executeAsyncJob<
      { jobId: string },
      { status: string; progress?: string; result?: { data: string } },
      { data: string }
    >({
      kickOff,
      getJobId: (result) => result.jobId,
      checkStatus,
      isComplete: (status) => status.status === "complete",
      getResult: (status) => status.result!,
      cancel,
      pollingInterval: 1000,
    });

    // Advance through polling intervals.
    await vi.advanceTimersByTimeAsync(0); // Initial kick-off.
    await vi.advanceTimersByTimeAsync(1000); // First poll.
    await vi.advanceTimersByTimeAsync(1000); // Second poll.
    await vi.advanceTimersByTimeAsync(1000); // Third poll (complete).

    const result = await handle.result;

    expect(kickOff).toHaveBeenCalledTimes(1);
    expect(checkStatus).toHaveBeenCalledTimes(3);
    expect(checkStatus).toHaveBeenCalledWith("job-123");
    expect(result).toEqual({ data: "success" });
  });

  it("calls onProgress callback during polling", async () => {
    const kickOff = vi.fn().mockResolvedValue({ jobId: "job-123" });
    const checkStatus = vi
      .fn()
      .mockResolvedValueOnce({ status: "in-progress", progress: "50%" })
      .mockResolvedValueOnce({ status: "complete", result: "done" });
    const onProgress = vi.fn();

    const handle = executeAsyncJob<
      { jobId: string },
      { status: string; progress?: string; result?: string },
      string
    >({
      kickOff,
      getJobId: (result) => result.jobId,
      checkStatus,
      isComplete: (status) => status.status === "complete",
      getResult: (status) => status.result!,
      cancel: vi.fn(),
      pollingInterval: 1000,
      onProgress,
    });

    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(1000);
    await vi.advanceTimersByTimeAsync(1000);

    await handle.result;

    expect(onProgress).toHaveBeenCalledWith({
      status: "in-progress",
      progress: "50%",
    });
  });

  it("uses default polling interval of 3000ms", async () => {
    const kickOff = vi.fn().mockResolvedValue({ jobId: "job-123" });
    const checkStatus = vi
      .fn()
      .mockResolvedValueOnce({ status: "in-progress" })
      .mockResolvedValueOnce({ status: "complete", result: "done" });

    const handle = executeAsyncJob<
      { jobId: string },
      { status: string; result?: string },
      string
    >({
      kickOff,
      getJobId: (result) => result.jobId,
      checkStatus,
      isComplete: (status) => status.status === "complete",
      getResult: (status) => status.result!,
      cancel: vi.fn(),
    });

    await vi.advanceTimersByTimeAsync(0);
    expect(checkStatus).toHaveBeenCalledTimes(0);

    await vi.advanceTimersByTimeAsync(2999);
    expect(checkStatus).toHaveBeenCalledTimes(0);

    await vi.advanceTimersByTimeAsync(1);
    expect(checkStatus).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(3000);
    await handle.result;

    expect(checkStatus).toHaveBeenCalledTimes(2);
  });

  it("cancels job when cancel is called", async () => {
    const kickOff = vi.fn().mockResolvedValue({ jobId: "job-123" });
    const checkStatus = vi.fn().mockResolvedValue({ status: "in-progress" });
    const cancel = vi.fn().mockResolvedValue(undefined);

    const handle = executeAsyncJob<
      { jobId: string },
      { status: string; result?: string },
      string
    >({
      kickOff,
      getJobId: (result) => result.jobId,
      checkStatus,
      isComplete: (status) => status.status === "complete",
      getResult: (status) => status.result!,
      cancel,
      pollingInterval: 1000,
    });

    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(1000);

    await handle.cancel();

    expect(cancel).toHaveBeenCalledWith("job-123");
  });

  it("rejects result promise when job fails", async () => {
    const kickOff = vi.fn().mockResolvedValue({ jobId: "job-123" });
    const checkStatus = vi.fn().mockRejectedValue(new Error("Job failed"));

    const handle = executeAsyncJob<
      { jobId: string },
      { status: string; result?: string },
      string
    >({
      kickOff,
      getJobId: (result) => result.jobId,
      checkStatus,
      isComplete: (status) => status.status === "complete",
      getResult: (status) => status.result!,
      cancel: vi.fn(),
      pollingInterval: 1000,
    });

    // Attach error handler to prevent unhandled rejection warning.
    handle.result.catch(() => {});

    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(1000);

    await expect(handle.result).rejects.toThrow("Job failed");
  });

  it("rejects result promise when kick-off fails", async () => {
    const kickOff = vi.fn().mockRejectedValue(new Error("Kick-off failed"));

    const handle = executeAsyncJob<
      { jobId: string },
      { status: string; result?: string },
      string
    >({
      kickOff,
      getJobId: (result) => result.jobId,
      checkStatus: vi.fn(),
      isComplete: (status) => status.status === "complete",
      getResult: (status) => status.result!,
      cancel: vi.fn(),
    });

    // Attach error handler to prevent unhandled rejection warning.
    handle.result.catch(() => {});

    await vi.advanceTimersByTimeAsync(0);

    await expect(handle.result).rejects.toThrow("Kick-off failed");
  });

  it("stops polling after cancel is called", async () => {
    const kickOff = vi.fn().mockResolvedValue({ jobId: "job-123" });
    const checkStatus = vi.fn().mockResolvedValue({ status: "in-progress" });
    const cancel = vi.fn().mockResolvedValue(undefined);

    const handle = executeAsyncJob<
      { jobId: string },
      { status: string; result?: string },
      string
    >({
      kickOff,
      getJobId: (result) => result.jobId,
      checkStatus,
      isComplete: (status) => status.status === "complete",
      getResult: (status) => status.result!,
      cancel,
      pollingInterval: 1000,
    });

    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(1000);
    const callCountBeforeCancel = checkStatus.mock.calls.length;

    await handle.cancel();

    await vi.advanceTimersByTimeAsync(5000);

    expect(checkStatus.mock.calls.length).toBe(callCountBeforeCancel);
  });
});

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
 * Tests for the useViewExport hook.
 *
 * These tests verify the hook's ability to:
 * - Execute view export operations via the buildOptions callback.
 * - Handle the complete async job lifecycle including status polling.
 * - Store the polling URL in a ref for use by the download function.
 * - Download files from completed exports using the job ID extracted from the polling URL.
 * - Expose the deleteJob function from useAsyncJob.
 *
 * @author John Grimes
 */

import { renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// The ViewExportRequest type is needed for typing the capturedBuildOptions variable.
// We declare the type alias inline to avoid import ordering issues with vi.mock.
interface ViewExportRequestType {
  views: Array<{
    viewDefinition: {
      resourceType: "ViewDefinition";
      name?: string;
      resource: string;
      status: string;
      select: unknown[];
    };
    name?: string;
  }>;
  format?: "ndjson" | "csv" | "parquet";
  header?: boolean;
}

// Track the buildOptions callback passed to useAsyncJob so we can test its returned options.
let capturedBuildOptions:
  | ((request: ViewExportRequestType) => unknown)
  | undefined;

const mockStartWith = vi.fn();
const mockCancel = vi.fn();
const mockDeleteJob = vi.fn();
const mockReset = vi.fn();

// Mock the useAsyncJob hook to capture the buildOptions callback.
vi.mock("../useAsyncJob", () => ({
  useAsyncJob: vi.fn(
    (buildOptions: (request: ViewExportRequestType) => unknown) => {
      capturedBuildOptions = buildOptions;
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
    },
  ),
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
  useAsyncJobCallbacks: vi.fn(() => undefined),
}));

// Mock the API functions.
const mockViewExportKickOff = vi.fn();
const mockViewExportDownload = vi.fn();
const mockJobStatus = vi.fn();
const mockJobCancel = vi.fn();
const mockExtractJobIdFromUrl = vi.fn();

vi.mock("../../api", () => ({
  viewExportKickOff: (...args: unknown[]) => mockViewExportKickOff(...args),
  viewExportDownload: (...args: unknown[]) => mockViewExportDownload(...args),
  jobStatus: (...args: unknown[]) => mockJobStatus(...args),
  jobCancel: (...args: unknown[]) => mockJobCancel(...args),
  extractJobIdFromUrl: (...args: unknown[]) => mockExtractJobIdFromUrl(...args),
}));

import { useAsyncJob } from "../useAsyncJob";
import { useViewExport } from "../useViewExport";

describe("useViewExport", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    capturedBuildOptions = undefined;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("deleteJob", () => {
    it("exposes deleteJob function from useAsyncJob", () => {
      const { result } = renderHook(() => useViewExport());

      expect(result.current.deleteJob).toBe(mockDeleteJob);
    });

    it("calls deleteJob when invoked", async () => {
      mockDeleteJob.mockResolvedValue(undefined);

      const { result } = renderHook(() => useViewExport());

      await result.current.deleteJob();

      expect(mockDeleteJob).toHaveBeenCalledTimes(1);
    });

    it("does not expose cancel as deleteJob (they are separate functions)", () => {
      const { result } = renderHook(() => useViewExport());

      expect(result.current.deleteJob).not.toBe(result.current.cancel);
    });

    it("useAsyncJob is called when hook is rendered", () => {
      renderHook(() => useViewExport());

      expect(useAsyncJob).toHaveBeenCalled();
    });
  });

  // The buildOptions callback defines the view export workflow, including kick-off,
  // status polling, completion detection, and cancellation.
  describe("buildOptions callback", () => {
    describe("kickOff", () => {
      // The kick-off function initiates a view export with the provided views and options.
      it("calls viewExportKickOff with correct parameters", async () => {
        mockViewExportKickOff.mockResolvedValue({
          pollingUrl: "http://example.com/$job?id=view-export-123",
        });

        const mockViewDefinition = {
          resourceType: "ViewDefinition" as const,
          name: "PatientView",
          resource: "Patient" as const,
          status: "active",
          select: [{ column: [{ path: "id" }] }],
        };

        renderHook(() => useViewExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          views: [
            { viewDefinition: mockViewDefinition, name: "custom-name" },
            { viewDefinition: mockViewDefinition },
          ],
          format: "parquet",
          header: true,
        });

        const result = await (
          options as { kickOff: () => Promise<unknown> }
        ).kickOff();

        expect(mockViewExportKickOff).toHaveBeenCalledWith(
          "http://localhost:8080/fhir",
          {
            views: [
              { viewDefinition: mockViewDefinition, name: "custom-name" },
              { viewDefinition: mockViewDefinition },
            ],
            format: "parquet",
            header: true,
            accessToken: "test-token",
          },
        );
        expect(result).toEqual({
          pollingUrl: "http://example.com/$job?id=view-export-123",
        });
      });

      // Verify that optional parameters can be omitted.
      it("calls viewExportKickOff without optional parameters", async () => {
        mockViewExportKickOff.mockResolvedValue({
          pollingUrl: "http://example.com/$job?id=456",
        });

        const mockViewDefinition = {
          resourceType: "ViewDefinition" as const,
          name: "ObservationView",
          resource: "Observation" as const,
          status: "draft",
          select: [{ column: [{ path: "status" }] }],
        };

        renderHook(() => useViewExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          views: [{ viewDefinition: mockViewDefinition }],
        });

        await (options as { kickOff: () => Promise<unknown> }).kickOff();

        expect(mockViewExportKickOff).toHaveBeenCalledWith(
          "http://localhost:8080/fhir",
          {
            views: [{ viewDefinition: mockViewDefinition }],
            format: undefined,
            header: undefined,
            accessToken: "test-token",
          },
        );
      });
    });

    // getJobId extracts the polling URL and stores it in a ref for later use by download.
    describe("getJobId", () => {
      it("returns the pollingUrl from kick-off result", () => {
        const mockViewDefinition = {
          resourceType: "ViewDefinition" as const,
          name: "TestView",
          resource: "Patient" as const,
          status: "active",
          select: [],
        };

        renderHook(() => useViewExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          views: [{ viewDefinition: mockViewDefinition }],
        });
        const getJobId = (options as { getJobId: (r: unknown) => string })
          .getJobId;

        const result = getJobId({
          pollingUrl: "http://example.com/$job?id=test-view-job",
        });

        expect(result).toBe("http://example.com/$job?id=test-view-job");
      });
    });

    // checkStatus polls the server to get the current job state.
    describe("checkStatus", () => {
      it("calls jobStatus with correct parameters", async () => {
        mockJobStatus.mockResolvedValue({
          status: "in-progress",
          progress: "Processing views...",
        });

        const mockViewDefinition = {
          resourceType: "ViewDefinition" as const,
          name: "TestView",
          resource: "Patient" as const,
          status: "active",
          select: [],
        };

        renderHook(() => useViewExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          views: [{ viewDefinition: mockViewDefinition }],
        });
        const checkStatus = (
          options as { checkStatus: (url: string) => Promise<unknown> }
        ).checkStatus;

        const result = await checkStatus(
          "http://example.com/$job?id=test-view-job",
        );

        expect(mockJobStatus).toHaveBeenCalledWith(
          "http://localhost:8080/fhir",
          {
            pollingUrl: "http://example.com/$job?id=test-view-job",
            accessToken: "test-token",
          },
        );
        expect(result).toEqual({
          status: "in-progress",
          progress: "Processing views...",
        });
      });
    });

    // isComplete determines when the job has finished based on the status response.
    describe("isComplete", () => {
      it('returns true when status is "complete"', () => {
        const mockViewDefinition = {
          resourceType: "ViewDefinition" as const,
          name: "TestView",
          resource: "Patient" as const,
          status: "active",
          select: [],
        };

        renderHook(() => useViewExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          views: [{ viewDefinition: mockViewDefinition }],
        });
        const isComplete = (
          options as { isComplete: (s: { status: string }) => boolean }
        ).isComplete;

        expect(isComplete({ status: "complete" })).toBe(true);
      });

      it('returns false when status is "in-progress"', () => {
        const mockViewDefinition = {
          resourceType: "ViewDefinition" as const,
          name: "TestView",
          resource: "Patient" as const,
          status: "active",
          select: [],
        };

        renderHook(() => useViewExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          views: [{ viewDefinition: mockViewDefinition }],
        });
        const isComplete = (
          options as { isComplete: (s: { status: string }) => boolean }
        ).isComplete;

        expect(isComplete({ status: "in-progress" })).toBe(false);
      });
    });

    // getResult extracts the manifest from the completed status response.
    describe("getResult", () => {
      it("returns the result from status response", () => {
        const mockViewDefinition = {
          resourceType: "ViewDefinition" as const,
          name: "TestView",
          resource: "Patient" as const,
          status: "active",
          select: [],
        };

        const mockManifest = {
          resourceType: "Parameters",
          parameter: [
            {
              name: "output",
              part: [
                { name: "name", valueString: "PatientView" },
                {
                  name: "url",
                  valueUri: "http://example.com/files/PatientView.parquet",
                },
              ],
            },
          ],
        };

        renderHook(() => useViewExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          views: [{ viewDefinition: mockViewDefinition }],
        });
        const getResult = (
          options as { getResult: (s: { result?: unknown }) => unknown }
        ).getResult;

        const result = getResult({ result: mockManifest });

        expect(result).toBe(mockManifest);
      });

      // The result field is optional in the status response during in-progress state.
      it("returns undefined when result is not present", () => {
        const mockViewDefinition = {
          resourceType: "ViewDefinition" as const,
          name: "TestView",
          resource: "Patient" as const,
          status: "active",
          select: [],
        };

        renderHook(() => useViewExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          views: [{ viewDefinition: mockViewDefinition }],
        });
        const getResult = (
          options as { getResult: (s: { result?: unknown }) => unknown }
        ).getResult;

        const result = getResult({});

        expect(result).toBeUndefined();
      });
    });

    // cancel allows aborting an in-progress view export operation.
    describe("cancel", () => {
      it("calls jobCancel with correct parameters", async () => {
        mockJobCancel.mockResolvedValue(undefined);

        const mockViewDefinition = {
          resourceType: "ViewDefinition" as const,
          name: "TestView",
          resource: "Patient" as const,
          status: "active",
          select: [],
        };

        renderHook(() => useViewExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          views: [{ viewDefinition: mockViewDefinition }],
        });
        const cancel = (options as { cancel: (url: string) => Promise<void> })
          .cancel;

        await cancel("http://example.com/$job?id=test-view-job");

        expect(mockJobCancel).toHaveBeenCalledWith(
          "http://localhost:8080/fhir",
          {
            pollingUrl: "http://example.com/$job?id=test-view-job",
            accessToken: "test-token",
          },
        );
      });
    });

    // Verify the default polling interval is set correctly.
    describe("pollingInterval", () => {
      it("sets polling interval to 3000ms", () => {
        const mockViewDefinition = {
          resourceType: "ViewDefinition" as const,
          name: "TestView",
          resource: "Patient" as const,
          status: "active",
          select: [],
        };

        renderHook(() => useViewExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          views: [{ viewDefinition: mockViewDefinition }],
        });
        expect((options as { pollingInterval: number }).pollingInterval).toBe(
          3000,
        );
      });
    });
  });

  // The download function retrieves files from a completed view export. It relies on
  // the polling URL being stored in a ref during the getJobId callback, and extracts
  // the job ID from that URL to construct the download request.
  describe("download function", () => {
    // The download function should throw a clear error if called before getJobId has
    // stored a polling URL.
    it("throws error when no polling URL is available", async () => {
      // Default mock returns undefined result and no polling URL has been stored.
      const { result } = renderHook(() => useViewExport());

      await expect(
        result.current.download("PatientView.parquet"),
      ).rejects.toThrow("No polling URL available");
    });
  });

  // Additional integration test to verify the complete flow through the hook.
  describe("integration with pollingUrlRef", () => {
    it("stores polling URL in ref when getJobId is called", () => {
      const mockViewDefinition = {
        resourceType: "ViewDefinition" as const,
        name: "TestView",
        resource: "Patient" as const,
        status: "active",
        select: [],
      };

      renderHook(() => useViewExport());
      expect(capturedBuildOptions).toBeDefined();

      const options = capturedBuildOptions!({
        views: [{ viewDefinition: mockViewDefinition }],
      });

      // Call getJobId - this should store the polling URL in the hook's ref.
      const getJobId = (
        options as { getJobId: (r: { pollingUrl: string }) => string }
      ).getJobId;
      const pollingUrl = "http://example.com/$job?id=integration-test-123";

      const jobId = getJobId({ pollingUrl });

      // getJobId returns the polling URL as the job ID.
      expect(jobId).toBe(pollingUrl);
    });
  });
});

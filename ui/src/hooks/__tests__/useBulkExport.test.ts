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
 * Tests for the useBulkExport hook.
 *
 * These tests verify the hook's ability to:
 * - Execute different types of bulk export operations (system, all-patients, patient, group).
 * - Handle the complete async job lifecycle via buildOptions callbacks.
 * - Download files from completed export manifests.
 * - Expose the deleteJob function from useAsyncJob.
 *
 * @author John Grimes
 */

import { renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Track the buildOptions callback passed to useAsyncJob so we can test its returned options.
let capturedBuildOptions: ((request: BulkExportRequest) => unknown) | undefined;

const mockStartWith = vi.fn();
const mockCancel = vi.fn();
const mockDeleteJob = vi.fn();
const mockReset = vi.fn();

// Mock the useAsyncJob hook to capture the buildOptions callback.
vi.mock("../useAsyncJob", () => ({
  useAsyncJob: vi.fn(
    (buildOptions: (request: BulkExportRequest) => unknown) => {
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
const mockSystemExportKickOff = vi.fn();
const mockAllPatientsExportKickOff = vi.fn();
const mockPatientExportKickOff = vi.fn();
const mockGroupExportKickOff = vi.fn();
const mockBulkExportStatus = vi.fn();
const mockBulkExportDownload = vi.fn();
const mockJobCancel = vi.fn();

vi.mock("../../api", () => ({
  systemExportKickOff: (...args: unknown[]) => mockSystemExportKickOff(...args),
  allPatientsExportKickOff: (...args: unknown[]) =>
    mockAllPatientsExportKickOff(...args),
  patientExportKickOff: (...args: unknown[]) =>
    mockPatientExportKickOff(...args),
  groupExportKickOff: (...args: unknown[]) => mockGroupExportKickOff(...args),
  bulkExportStatus: (...args: unknown[]) => mockBulkExportStatus(...args),
  bulkExportDownload: (...args: unknown[]) => mockBulkExportDownload(...args),
  jobCancel: (...args: unknown[]) => mockJobCancel(...args),
}));

// Mock getExportOutputFiles from the types module.
const mockGetExportOutputFiles = vi.fn();
vi.mock("../../types/export", () => ({
  getExportOutputFiles: (...args: unknown[]) =>
    mockGetExportOutputFiles(...args),
}));

import { useAsyncJob } from "../useAsyncJob";
import { useBulkExport } from "../useBulkExport";

import type { BulkExportRequest } from "../useBulkExport";

describe("useBulkExport", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    capturedBuildOptions = undefined;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("deleteJob", () => {
    it("exposes deleteJob function from useAsyncJob", () => {
      const { result } = renderHook(() => useBulkExport());

      expect(result.current.deleteJob).toBe(mockDeleteJob);
    });

    it("calls deleteJob when invoked", async () => {
      mockDeleteJob.mockResolvedValue(undefined);

      const { result } = renderHook(() => useBulkExport());

      await result.current.deleteJob();

      expect(mockDeleteJob).toHaveBeenCalledTimes(1);
    });

    it("does not expose cancel as deleteJob (they are separate functions)", () => {
      const { result } = renderHook(() => useBulkExport());

      expect(result.current.deleteJob).not.toBe(result.current.cancel);
    });

    it("useAsyncJob is called when hook is rendered", () => {
      renderHook(() => useBulkExport());

      expect(useAsyncJob).toHaveBeenCalled();
    });
  });

  // The buildOptions callback is the core of the hook's functionality. It defines how
  // each export type is initiated and how the async job lifecycle is managed.
  describe("buildOptions callback", () => {
    describe("kickOff", () => {
      // System export is the simplest case - it exports all resources on the server.
      it("calls systemExportKickOff for system export type", async () => {
        mockSystemExportKickOff.mockResolvedValue({
          pollingUrl: "http://example.com/$job?id=123",
        });

        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          type: "system",
          resourceTypes: ["Patient", "Observation"],
          since: "2023-01-01",
          until: "2023-12-31",
          elements: "id,name",
          outputFormat: "ndjson",
        });

        const result = await (
          options as { kickOff: () => Promise<unknown> }
        ).kickOff();

        expect(mockSystemExportKickOff).toHaveBeenCalledWith(
          "http://localhost:8080/fhir",
          {
            types: ["Patient", "Observation"],
            since: "2023-01-01",
            until: "2023-12-31",
            elements: "id,name",
            outputFormat: "ndjson",
            accessToken: "test-token",
          },
        );
        expect(result).toEqual({
          pollingUrl: "http://example.com/$job?id=123",
        });
      });

      // All-patients export targets the Patient compartment across all patients.
      it("calls allPatientsExportKickOff for all-patients export type", async () => {
        mockAllPatientsExportKickOff.mockResolvedValue({
          pollingUrl: "http://example.com/$job?id=456",
        });

        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          type: "all-patients",
        });

        const result = await (
          options as { kickOff: () => Promise<unknown> }
        ).kickOff();

        expect(mockAllPatientsExportKickOff).toHaveBeenCalledWith(
          "http://localhost:8080/fhir",
          {
            types: undefined,
            since: undefined,
            until: undefined,
            elements: undefined,
            outputFormat: undefined,
            accessToken: "test-token",
          },
        );
        expect(result).toEqual({
          pollingUrl: "http://example.com/$job?id=456",
        });
      });

      // Patient export requires a patient ID and exports only that patient's compartment.
      it("calls patientExportKickOff for patient export type with patientId", async () => {
        mockPatientExportKickOff.mockResolvedValue({
          pollingUrl: "http://example.com/$job?id=789",
        });

        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          type: "patient",
          patientId: "patient-123",
        });

        const result = await (
          options as { kickOff: () => Promise<unknown> }
        ).kickOff();

        expect(mockPatientExportKickOff).toHaveBeenCalledWith(
          "http://localhost:8080/fhir",
          {
            types: undefined,
            since: undefined,
            until: undefined,
            elements: undefined,
            outputFormat: undefined,
            accessToken: "test-token",
            patientId: "patient-123",
          },
        );
        expect(result).toEqual({
          pollingUrl: "http://example.com/$job?id=789",
        });
      });

      // The hook should throw a clear error if patient export is attempted without an ID.
      it("throws error for patient export type when patientId is missing", async () => {
        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          type: "patient",
        });

        await expect(
          (options as { kickOff: () => Promise<unknown> }).kickOff(),
        ).rejects.toThrow("Patient ID required");
      });

      // Group export requires a group ID and exports resources for all group members.
      it("calls groupExportKickOff for group export type with groupId", async () => {
        mockGroupExportKickOff.mockResolvedValue({
          pollingUrl: "http://example.com/$job?id=abc",
        });

        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          type: "group",
          groupId: "group-456",
        });

        const result = await (
          options as { kickOff: () => Promise<unknown> }
        ).kickOff();

        expect(mockGroupExportKickOff).toHaveBeenCalledWith(
          "http://localhost:8080/fhir",
          {
            types: undefined,
            since: undefined,
            until: undefined,
            elements: undefined,
            outputFormat: undefined,
            accessToken: "test-token",
            groupId: "group-456",
          },
        );
        expect(result).toEqual({
          pollingUrl: "http://example.com/$job?id=abc",
        });
      });

      // The hook should throw a clear error if group export is attempted without an ID.
      it("throws error for group export type when groupId is missing", async () => {
        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          type: "group",
        });

        await expect(
          (options as { kickOff: () => Promise<unknown> }).kickOff(),
        ).rejects.toThrow("Group ID required");
      });

      // Guard against unexpected export types to catch programming errors early.
      it("throws error for unknown export type", async () => {
        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({
          type: "unknown" as BulkExportRequest["type"],
        });

        await expect(
          (options as { kickOff: () => Promise<unknown> }).kickOff(),
        ).rejects.toThrow("Unknown export type: unknown");
      });
    });

    // getJobId extracts the polling URL from the kick-off result to use as the job identifier.
    describe("getJobId", () => {
      it("returns the pollingUrl from kick-off result", () => {
        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({ type: "system" });
        const getJobId = (options as { getJobId: (r: unknown) => string })
          .getJobId;

        const result = getJobId({
          pollingUrl: "http://example.com/$job?id=test-job",
        });

        expect(result).toBe("http://example.com/$job?id=test-job");
      });
    });

    // checkStatus polls the server for the current job state.
    describe("checkStatus", () => {
      it("calls bulkExportStatus with correct parameters", async () => {
        mockBulkExportStatus.mockResolvedValue({
          status: "in-progress",
          progress: "50%",
        });

        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({ type: "system" });
        const checkStatus = (
          options as { checkStatus: (url: string) => Promise<unknown> }
        ).checkStatus;

        const result = await checkStatus("http://example.com/$job?id=test-job");

        expect(mockBulkExportStatus).toHaveBeenCalledWith(
          "http://localhost:8080/fhir",
          {
            pollingUrl: "http://example.com/$job?id=test-job",
            accessToken: "test-token",
          },
        );
        expect(result).toEqual({ status: "in-progress", progress: "50%" });
      });
    });

    // isComplete determines when polling should stop based on the status response.
    describe("isComplete", () => {
      it('returns true when status is "complete"', () => {
        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({ type: "system" });
        const isComplete = (
          options as { isComplete: (s: { status: string }) => boolean }
        ).isComplete;

        expect(isComplete({ status: "complete" })).toBe(true);
      });

      it('returns false when status is "in-progress"', () => {
        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({ type: "system" });
        const isComplete = (
          options as { isComplete: (s: { status: string }) => boolean }
        ).isComplete;

        expect(isComplete({ status: "in-progress" })).toBe(false);
      });
    });

    // getResult extracts the manifest from the completed status response.
    describe("getResult", () => {
      it("returns the manifest from status result", () => {
        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const mockManifest = {
          resourceType: "Parameters",
          parameter: [
            {
              name: "output",
              part: [
                { name: "type", valueCode: "Patient" },
                {
                  name: "url",
                  valueUri: "http://example.com/files/Patient.ndjson",
                },
              ],
            },
          ],
        };

        const options = capturedBuildOptions!({ type: "system" });
        const getResult = (
          options as { getResult: (s: { manifest: unknown }) => unknown }
        ).getResult;

        const result = getResult({ manifest: mockManifest });

        expect(result).toBe(mockManifest);
      });
    });

    // cancel allows aborting an in-progress export operation.
    describe("cancel", () => {
      it("calls jobCancel with correct parameters", async () => {
        mockJobCancel.mockResolvedValue(undefined);

        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({ type: "system" });
        const cancel = (options as { cancel: (url: string) => Promise<void> })
          .cancel;

        await cancel("http://example.com/$job?id=test-job");

        expect(mockJobCancel).toHaveBeenCalledWith(
          "http://localhost:8080/fhir",
          {
            pollingUrl: "http://example.com/$job?id=test-job",
            accessToken: "test-token",
          },
        );
      });
    });

    // Verify the default polling interval is set correctly.
    describe("pollingInterval", () => {
      it("sets polling interval to 3000ms", () => {
        renderHook(() => useBulkExport());
        expect(capturedBuildOptions).toBeDefined();

        const options = capturedBuildOptions!({ type: "system" });
        expect((options as { pollingInterval: number }).pollingInterval).toBe(
          3000,
        );
      });
    });
  });

  // The download function enables downloading individual files from a completed export.
  describe("download function", () => {
    it("downloads a file when result is available and file is found", async () => {
      const mockStream = new ReadableStream();
      mockBulkExportDownload.mockResolvedValue(mockStream);

      const mockManifest = {
        resourceType: "Parameters",
        parameter: [
          {
            name: "output",
            part: [
              { name: "type", valueCode: "Patient" },
              {
                name: "url",
                valueUri: "http://example.com/files/Patient.ndjson",
              },
            ],
          },
        ],
      };

      mockGetExportOutputFiles.mockReturnValue([
        { type: "Patient", url: "http://example.com/files/Patient.ndjson" },
      ]);

      // We need to provide a result from useAsyncJob to test the download function.
      vi.mocked(useAsyncJob).mockImplementationOnce(
        (buildOptions: (request: BulkExportRequest) => unknown) => {
          capturedBuildOptions = buildOptions;
          return {
            startWith: mockStartWith,
            cancel: mockCancel,
            deleteJob: mockDeleteJob,
            reset: mockReset,
            status: "complete",
            result: mockManifest,
            error: undefined,
            progress: undefined,
            request: undefined,
          };
        },
      );

      const { result } = renderHook(() => useBulkExport());

      const stream = await result.current.download("Patient.ndjson");

      expect(mockGetExportOutputFiles).toHaveBeenCalledWith(mockManifest);
      expect(mockBulkExportDownload).toHaveBeenCalledWith(
        "http://localhost:8080/fhir",
        {
          fileUrl: "http://example.com/files/Patient.ndjson",
          accessToken: "test-token",
        },
      );
      expect(stream).toBe(mockStream);
    });

    it("throws error when no export result is available", async () => {
      // Default mock returns undefined result.
      const { result } = renderHook(() => useBulkExport());

      await expect(result.current.download("Patient.ndjson")).rejects.toThrow(
        "No export result available",
      );
    });

    it("throws error when file is not found in manifest", async () => {
      const mockManifest = {
        resourceType: "Parameters",
        parameter: [
          {
            name: "output",
            part: [
              { name: "type", valueCode: "Patient" },
              {
                name: "url",
                valueUri: "http://example.com/files/Patient.ndjson",
              },
            ],
          },
        ],
      };

      mockGetExportOutputFiles.mockReturnValue([
        { type: "Patient", url: "http://example.com/files/Patient.ndjson" },
      ]);

      vi.mocked(useAsyncJob).mockImplementationOnce(
        (buildOptions: (request: BulkExportRequest) => unknown) => {
          capturedBuildOptions = buildOptions;
          return {
            startWith: mockStartWith,
            cancel: mockCancel,
            deleteJob: mockDeleteJob,
            reset: mockReset,
            status: "complete",
            result: mockManifest,
            error: undefined,
            progress: undefined,
            request: undefined,
          };
        },
      );

      const { result } = renderHook(() => useBulkExport());

      await expect(
        result.current.download("NonExistent.ndjson"),
      ).rejects.toThrow("File not found: NonExistent.ndjson");
    });
  });
});

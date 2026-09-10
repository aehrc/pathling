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
 * Tests for the useSqlExport hook: that it composes the async-job machinery
 * with the `$sql-export` kick-off, status, cancel and download calls, and that
 * a rejected kick-off surfaces as a failure rather than a started job.
 *
 * @author John Grimes
 */

import { renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mockStartWith = vi.fn();
const mockCancel = vi.fn();
const mockDeleteJob = vi.fn();

/** Captures the options the hook builds for one request. */
let capturedBuildOptions:
  | ((request: unknown) => Record<string, unknown>)
  | undefined;

vi.mock("../useAsyncJob", () => ({
  useAsyncJob: vi.fn(
    (buildOptions: (request: unknown) => Record<string, unknown>) => {
      capturedBuildOptions = buildOptions;
      return {
        startWith: mockStartWith,
        cancel: mockCancel,
        deleteJob: mockDeleteJob,
        reset: vi.fn(),
        status: "idle",
        result: undefined,
        error: undefined,
        progress: undefined,
        request: undefined,
      };
    },
  ),
}));

vi.mock("../useAsyncJobCallbacks", () => ({
  useAsyncJobCallbacks: vi.fn((opts) => opts),
}));

vi.mock("../../config", () => ({
  config: { fhirBaseUrl: "http://localhost:8080/fhir" },
}));

vi.mock("../../contexts/AuthContext", () => ({
  useAuth: vi.fn(() => ({
    client: { state: { tokenResponse: { access_token: "test-token" } } },
  })),
}));

const mockKickOff = vi.fn();
const mockStatus = vi.fn();
const mockJobCancel = vi.fn();
const mockDownload = vi.fn();

vi.mock("../../api", () => ({
  sqlExportKickOff: (...args: unknown[]) => mockKickOff(...args),
  sqlExportDownload: (...args: unknown[]) => mockDownload(...args),
  jobStatus: (...args: unknown[]) => mockStatus(...args),
  jobCancel: (...args: unknown[]) => mockJobCancel(...args),
}));

import { useSqlExport } from "../useSqlExport";

import type { SqlExportRequest } from "../useSqlExport";

const REQUEST: SqlExportRequest = {
  subjects: [
    {
      name: "demographics",
      subject: { kind: "reference", reference: "ViewDefinition/v" },
    },
    {
      name: "smiths",
      subject: { kind: "reference", reference: "Library/by-family" },
    },
  ],
  format: "csv",
  header: false,
  patientIds: ["p1"],
  since: "2026-01-01T00:00:00Z",
};

describe("useSqlExport", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    capturedBuildOptions = undefined;
  });

  it("exposes the async-job controls and a download function", () => {
    const { result } = renderHook(() => useSqlExport());

    expect(result.current.startWith).toBe(mockStartWith);
    expect(result.current.cancel).toBe(mockCancel);
    expect(result.current.status).toBe("idle");
    expect(typeof result.current.download).toBe("function");
  });

  // The whole request reaches the kick-off in one call, since one job carries
  // every subject and the filters that apply to all of them.
  it("kicks off one job carrying every subject and the job-wide settings", async () => {
    mockKickOff.mockResolvedValue({
      pollingUrl: "http://localhost/$job?id=abc",
    });
    renderHook(() => useSqlExport());

    const options = capturedBuildOptions!(REQUEST);
    await (options.kickOff as () => Promise<unknown>)();

    expect(mockKickOff).toHaveBeenCalledWith("http://localhost:8080/fhir", {
      subjects: REQUEST.subjects,
      format: "csv",
      header: false,
      patientIds: ["p1"],
      groupIds: undefined,
      since: "2026-01-01T00:00:00Z",
      accessToken: "test-token",
    });
  });

  it("tracks the job by the status URL the kick-off returned", () => {
    renderHook(() => useSqlExport());
    const options = capturedBuildOptions!(REQUEST);

    const jobId = (options.getJobId as (r: { pollingUrl: string }) => string)({
      pollingUrl: "http://localhost/$job?id=abc",
    });

    expect(jobId).toBe("http://localhost/$job?id=abc");
  });

  it("polls the status URL and recognises completion", () => {
    renderHook(() => useSqlExport());
    const options = capturedBuildOptions!(REQUEST);

    (options.checkStatus as (url: string) => unknown)(
      "http://localhost/$job?id=abc",
    );
    expect(mockStatus).toHaveBeenCalledWith("http://localhost:8080/fhir", {
      pollingUrl: "http://localhost/$job?id=abc",
      accessToken: "test-token",
    });

    const isComplete = options.isComplete as (s: { status: string }) => boolean;
    expect(isComplete({ status: "complete" })).toBe(true);
    expect(isComplete({ status: "in-progress" })).toBe(false);
  });

  // A completed job's manifest can name several outputs, one per subject, and
  // the hook passes it through untouched for the card to parse.
  it("returns the multi-output manifest as the job result", () => {
    renderHook(() => useSqlExport());
    const options = capturedBuildOptions!(REQUEST);
    const manifest = {
      resourceType: "Parameters",
      parameter: [
        {
          name: "output",
          part: [{ name: "name", valueString: "demographics" }],
        },
        { name: "output", part: [{ name: "name", valueString: "smiths" }] },
      ],
    };

    const returned = (
      options.getResult as (s: { result?: unknown }) => unknown
    )({
      result: manifest,
    });

    expect(returned).toBe(manifest);
  });

  it("cancels through the shared job endpoint", () => {
    renderHook(() => useSqlExport());
    const options = capturedBuildOptions!(REQUEST);

    (options.cancel as (url: string) => unknown)(
      "http://localhost/$job?id=abc",
    );

    expect(mockJobCancel).toHaveBeenCalledWith("http://localhost:8080/fhir", {
      pollingUrl: "http://localhost/$job?id=abc",
      accessToken: "test-token",
    });
  });

  it("downloads an output by its manifest location", async () => {
    mockDownload.mockResolvedValue(new ReadableStream());
    const { result } = renderHook(() => useSqlExport());

    await result.current.download("http://localhost/$result?file=a.csv");

    expect(mockDownload).toHaveBeenCalledWith({
      location: "http://localhost/$result?file=a.csv",
      accessToken: "test-token",
    });
  });

  // A kick-off the server rejects must surface as a failure the export card
  // can display, not as a job that silently never starts.
  it("propagates a rejected kick-off as a failure", async () => {
    mockKickOff.mockRejectedValue(
      new Error("At least one 'subject' must be supplied"),
    );
    renderHook(() => useSqlExport());
    const options = capturedBuildOptions!(REQUEST);

    await expect((options.kickOff as () => Promise<unknown>)()).rejects.toThrow(
      "At least one 'subject' must be supplied",
    );
  });
});

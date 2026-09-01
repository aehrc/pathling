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

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { NotFoundError, UnauthorizedError } from "../../types/errors";
import { listJobs } from "../jobs";

import type { Parameters } from "fhir/r4";

const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetAllMocks();
});

/**
 * Builds a $jobs Parameters response body with the given job parts.
 *
 * @param jobs - The jobs to encode as repeating job parameters.
 * @returns A FHIR Parameters resource.
 */
function jobsResponse(jobs: Parameters["parameter"]): Parameters {
  return { resourceType: "Parameters", parameter: jobs };
}

describe("listJobs", () => {
  it("makes a GET request to the $jobs operation", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(jobsResponse([])), { status: 200 }),
    );

    await listJobs("https://example.com/fhir");

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$jobs",
      expect.objectContaining({
        method: "GET",
        headers: expect.objectContaining({ Accept: "application/fhir+json" }),
      }),
    );
  });

  it("includes the Authorization header when an access token is provided", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(jobsResponse([])), { status: 200 }),
    );

    await listJobs("https://example.com/fhir", { accessToken: "token123" });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$jobs",
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: "Bearer token123" }),
      }),
    );
  });

  it("parses a populated job list into job summaries", async () => {
    const body = jobsResponse([
      {
        name: "job",
        part: [
          { name: "id", valueString: "abc-123" },
          { name: "operation", valueCode: "export" },
          { name: "status", valueCode: "in-progress" },
          { name: "progress", valueInteger: 62 },
          { name: "startTime", valueInstant: "2026-07-24T00:42:11.000Z" },
          {
            name: "url",
            valueUri: "https://example.com/fhir/$job?id=abc-123",
          },
        ],
      },
    ]);
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(body), { status: 200 }),
    );

    const jobs = await listJobs("https://example.com/fhir");

    expect(jobs).toEqual([
      {
        id: "abc-123",
        operation: "export",
        status: "in-progress",
        progress: 62,
        startTime: "2026-07-24T00:42:11.000Z",
        url: "https://example.com/fhir/$job?id=abc-123",
      },
    ]);
  });

  it("returns an empty array when the Parameters resource has no jobs", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ resourceType: "Parameters" }), {
        status: 200,
      }),
    );

    expect(await listJobs("https://example.com/fhir")).toEqual([]);
  });

  it("omits progress when the part is absent", async () => {
    const body = jobsResponse([
      {
        name: "job",
        part: [
          { name: "id", valueString: "done-1" },
          { name: "operation", valueCode: "import" },
          { name: "status", valueCode: "completed" },
          { name: "startTime", valueInstant: "2026-07-24T09:03:00.000Z" },
          { name: "url", valueUri: "https://example.com/fhir/$job?id=done-1" },
        ],
      },
    ]);
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(body), { status: 200 }),
    );

    const jobs = await listJobs("https://example.com/fhir");

    expect(jobs).toHaveLength(1);
    expect(jobs[0].progress).toBeUndefined();
    expect(jobs[0].status).toBe("completed");
  });

  it("skips malformed job entries missing required parts", async () => {
    const body = jobsResponse([
      {
        name: "job",
        part: [{ name: "operation", valueCode: "export" }],
      },
      {
        name: "job",
        part: [
          { name: "id", valueString: "good-1" },
          { name: "operation", valueCode: "export" },
          { name: "status", valueCode: "failed" },
          { name: "startTime", valueInstant: "2026-07-23T16:17:00.000Z" },
          { name: "url", valueUri: "https://example.com/fhir/$job?id=good-1" },
        ],
      },
    ]);
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(body), { status: 200 }),
    );

    const jobs = await listJobs("https://example.com/fhir");

    expect(jobs.map((job) => job.id)).toEqual(["good-1"]);
  });

  it("throws UnauthorizedError on a 401 response", async () => {
    mockFetch.mockResolvedValueOnce(new Response("", { status: 401 }));

    await expect(listJobs("https://example.com/fhir")).rejects.toBeInstanceOf(
      UnauthorizedError,
    );
  });

  it("throws on a 403 response", async () => {
    mockFetch.mockResolvedValueOnce(new Response("Forbidden", { status: 403 }));

    await expect(listJobs("https://example.com/fhir")).rejects.toThrow();
  });

  it("throws NotFoundError on a 404 response", async () => {
    mockFetch.mockResolvedValueOnce(new Response("", { status: 404 }));

    await expect(listJobs("https://example.com/fhir")).rejects.toBeInstanceOf(
      NotFoundError,
    );
  });

  it("throws on a 500 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Server error", { status: 500 }),
    );

    await expect(listJobs("https://example.com/fhir")).rejects.toThrow();
  });
});

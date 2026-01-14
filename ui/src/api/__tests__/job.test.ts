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

import { UnauthorizedError } from "../../types/errors";
import { jobCancel, jobStatus } from "../job";

const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetAllMocks();
});

describe("jobStatus", () => {
  it("makes GET request to job status URL", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ status: "complete" }), { status: 200 }),
    );

    await jobStatus("https://example.com/fhir", {
      pollingUrl: "https://example.com/fhir/$job?id=abc-123",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$job?id=abc-123",
      expect.objectContaining({
        method: "GET",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
        }),
      }),
    );
  });

  it("includes Authorization header when access token provided", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ status: "complete" }), { status: 200 }),
    );

    await jobStatus("https://example.com/fhir", {
      pollingUrl: "https://example.com/fhir/$job?id=abc-123",
      accessToken: "test-token",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer test-token",
        }),
      }),
    );
  });

  it("returns in-progress status with progress from 202 response", async () => {
    const headers = new Headers();
    headers.set("X-Progress", "45%");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    const result = await jobStatus("https://example.com/fhir", {
      pollingUrl: "https://example.com/fhir/$job?id=abc-123",
    });

    expect(result.status).toBe("in-progress");
    expect(result.progress).toBe("45%");
  });

  it("returns complete status with result from 200 response", async () => {
    const resultData = { transactionTime: "2024-01-01T00:00:00Z" };

    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(resultData), { status: 200 }),
    );

    const result = await jobStatus("https://example.com/fhir", {
      pollingUrl: "https://example.com/fhir/$job?id=abc-123",
    });

    expect(result.status).toBe("complete");
    expect(result.result).toEqual(resultData);
  });

  it("returns undefined progress when header not present", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202 }));

    const result = await jobStatus("https://example.com/fhir", {
      pollingUrl: "https://example.com/fhir/$job?id=abc-123",
    });

    expect(result.status).toBe("in-progress");
    expect(result.progress).toBeUndefined();
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      jobStatus("https://example.com/fhir", {
        pollingUrl: "https://example.com/fhir/$job?id=abc-123",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });

  it("throws Error for other error responses", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Server error", { status: 500 }),
    );

    await expect(
      jobStatus("https://example.com/fhir", {
        pollingUrl: "https://example.com/fhir/$job?id=abc-123",
      }),
    ).rejects.toThrow("Job status failed: 500");
  });
});

describe("jobCancel", () => {
  it("makes DELETE request to job status URL", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202 }));

    await jobCancel("https://example.com/fhir", {
      pollingUrl: "https://example.com/fhir/$job?id=abc-123",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$job?id=abc-123",
      expect.objectContaining({
        method: "DELETE",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
        }),
      }),
    );
  });

  it("includes Authorization header when access token provided", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202 }));

    await jobCancel("https://example.com/fhir", {
      pollingUrl: "https://example.com/fhir/$job?id=abc-123",
      accessToken: "test-token",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer test-token",
        }),
      }),
    );
  });

  it("resolves without value on success", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202 }));

    const result = await jobCancel("https://example.com/fhir", {
      pollingUrl: "https://example.com/fhir/$job?id=abc-123",
    });

    expect(result).toBeUndefined();
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      jobCancel("https://example.com/fhir", {
        pollingUrl: "https://example.com/fhir/$job?id=abc-123",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });

  it("throws Error for other error responses", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Server error", { status: 500 }),
    );

    await expect(
      jobCancel("https://example.com/fhir", {
        pollingUrl: "https://example.com/fhir/$job?id=abc-123",
      }),
    ).rejects.toThrow("Job cancel failed: 500");
  });
});

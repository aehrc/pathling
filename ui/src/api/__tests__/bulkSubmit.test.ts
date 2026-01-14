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
import {
  bulkSubmit,
  bulkSubmitDownload,
  bulkSubmitStatus,
} from "../bulkSubmit";

const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetAllMocks();
});

describe("bulkSubmit", () => {
  const baseSubmitOptions = {
    submitter: { system: "http://example.org", value: "org-123" },
    submissionId: "sub-456",
    submissionStatus: "in-progress" as const,
    manifestUrl: "https://source.com/manifest.json",
  };

  it("makes POST request to /$bulk-submit endpoint", async () => {
    const responseParams = {
      resourceType: "Parameters",
      parameter: [
        { name: "submissionId", valueString: "sub-456" },
        { name: "status", valueString: "accepted" },
      ],
    };

    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(responseParams), { status: 200 }),
    );

    await bulkSubmit("https://example.com/fhir", baseSubmitOptions);

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$bulk-submit",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
          "Content-Type": "application/fhir+json",
        }),
      }),
    );
  });

  it("builds FHIR Parameters resource with required fields", async () => {
    const responseParams = {
      resourceType: "Parameters",
      parameter: [
        { name: "submissionId", valueString: "sub-456" },
        { name: "status", valueString: "accepted" },
      ],
    };

    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(responseParams), { status: 200 }),
    );

    await bulkSubmit("https://example.com/fhir", baseSubmitOptions);

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.resourceType).toBe("Parameters");
    expect(body.parameter).toContainEqual({
      name: "submissionId",
      valueString: "sub-456",
    });
    expect(body.parameter).toContainEqual({
      name: "submitter",
      valueIdentifier: { system: "http://example.org", value: "org-123" },
    });
  });

  it("includes optional fields when provided", async () => {
    const responseParams = {
      resourceType: "Parameters",
      parameter: [
        { name: "submissionId", valueString: "sub-456" },
        { name: "status", valueString: "accepted" },
      ],
    };

    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(responseParams), { status: 200 }),
    );

    await bulkSubmit("https://example.com/fhir", {
      ...baseSubmitOptions,
      fhirBaseUrl: "https://source.com/fhir",
      replacesManifestUrl: "https://source.com/old-manifest.json",
      metadata: { key1: "value1" },
    });

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.parameter).toContainEqual({
      name: "fhirBaseUrl",
      valueUrl: "https://source.com/fhir",
    });
    expect(body.parameter).toContainEqual({
      name: "replacesManifestUrl",
      valueUrl: "https://source.com/old-manifest.json",
    });
  });

  it("returns submission result from response", async () => {
    const responseParams = {
      resourceType: "Parameters",
      parameter: [
        { name: "submissionId", valueString: "sub-456" },
        { name: "status", valueString: "accepted" },
      ],
    };

    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(responseParams), { status: 200 }),
    );

    const result = await bulkSubmit(
      "https://example.com/fhir",
      baseSubmitOptions,
    );

    expect(result.submissionId).toBe("sub-456");
    expect(result.status).toBe("accepted");
  });

  it("includes Authorization header when access token provided", async () => {
    const responseParams = {
      resourceType: "Parameters",
      parameter: [
        { name: "submissionId", valueString: "sub-456" },
        { name: "status", valueString: "accepted" },
      ],
    };

    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(responseParams), { status: 200 }),
    );

    await bulkSubmit("https://example.com/fhir", {
      ...baseSubmitOptions,
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

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      bulkSubmit("https://example.com/fhir", baseSubmitOptions),
    ).rejects.toThrow(UnauthorizedError);
  });
});

describe("bulkSubmitStatus", () => {
  const baseStatusOptions = {
    submitter: { system: "http://example.org", value: "org-123" },
    submissionId: "sub-456",
  };

  it("makes POST request to /$bulk-submit-status endpoint", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=job-123");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    await bulkSubmitStatus("https://example.com/fhir", baseStatusOptions);

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$bulk-submit-status",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
          "Content-Type": "application/fhir+json",
          Prefer: "respond-async",
        }),
      }),
    );
  });

  it("returns pending status with job info from 202 with Content-Location", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=job-123");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    const result = await bulkSubmitStatus(
      "https://example.com/fhir",
      baseStatusOptions,
    );

    expect(result.status).toBe("in-progress");
    expect(result.jobId).toBe("job-123");
  });

  it("returns completed status with manifest from 200 response", async () => {
    const manifest = {
      transactionTime: "2024-01-01T00:00:00Z",
      request: "https://example.com/$bulk-submit",
      requiresAccessToken: false,
      output: [],
      error: [],
    };

    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(manifest), { status: 200 }),
    );

    const result = await bulkSubmitStatus(
      "https://example.com/fhir",
      baseStatusOptions,
    );

    expect(result.status).toBe("completed");
    expect(result.manifest).toEqual(manifest);
  });

  it("returns progress from X-Progress header on 202", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=job-123");
    headers.set("X-Progress", "50%");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    const result = await bulkSubmitStatus(
      "https://example.com/fhir",
      baseStatusOptions,
    );

    expect(result.progress).toBe("50%");
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      bulkSubmitStatus("https://example.com/fhir", baseStatusOptions),
    ).rejects.toThrow(UnauthorizedError);
  });
});

describe("bulkSubmitDownload", () => {
  it("makes GET request to download URL", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await bulkSubmitDownload("https://example.com/fhir", {
      submissionId: "sub-456",
      fileName: "output.ndjson",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$bulk-submit-download/sub-456/output.ndjson",
      expect.objectContaining({
        method: "GET",
      }),
    );
  });

  it("includes Authorization header when access token provided", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await bulkSubmitDownload("https://example.com/fhir", {
      submissionId: "sub-456",
      fileName: "output.ndjson",
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

  it("returns ReadableStream on success", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    const result = await bulkSubmitDownload("https://example.com/fhir", {
      submissionId: "sub-456",
      fileName: "output.ndjson",
    });

    expect(result).toBeInstanceOf(ReadableStream);
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      bulkSubmitDownload("https://example.com/fhir", {
        submissionId: "sub-456",
        fileName: "output.ndjson",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });
});

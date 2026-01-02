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

import type { Parameters } from "fhir/r4";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { UnauthorizedError } from "../../types/errors";
import {
  allPatientsExportKickOff,
  bulkExportDownload,
  bulkExportStatus,
  groupExportKickOff,
  patientExportKickOff,
  systemExportKickOff,
} from "../bulkExport";

const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetAllMocks();
});

describe("systemExportKickOff", () => {
  it("makes GET request to /$export endpoint", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=abc");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    await systemExportKickOff("https://example.com/fhir", {});

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$export",
      expect.objectContaining({
        method: "GET",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
          Prefer: "respond-async",
        }),
      }),
    );
  });

  it("includes _type parameter when types provided", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=abc");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    await systemExportKickOff("https://example.com/fhir", {
      types: ["Patient", "Observation"],
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining("_type=Patient%2CObservation"),
      expect.any(Object),
    );
  });

  it("includes _since parameter when provided", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=abc");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    await systemExportKickOff("https://example.com/fhir", {
      since: "2024-01-01T00:00:00Z",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining("_since="),
      expect.any(Object),
    );
  });

  it("includes Authorization header when access token provided", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=abc");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    await systemExportKickOff("https://example.com/fhir", {
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

  it("returns polling URL from Content-Location header", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=abc-123");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    const result = await systemExportKickOff("https://example.com/fhir", {});

    expect(result.pollingUrl).toBe("https://example.com/$job?id=abc-123");
  });

  it("throws error when Content-Location header missing", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202 }));

    await expect(
      systemExportKickOff("https://example.com/fhir", {}),
    ).rejects.toThrow("No Content-Location header");
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      systemExportKickOff("https://example.com/fhir", {}),
    ).rejects.toThrow(UnauthorizedError);
  });

  it("throws error on non-202 response", async () => {
    mockFetch.mockResolvedValueOnce(new Response("Error", { status: 500 }));

    await expect(
      systemExportKickOff("https://example.com/fhir", {}),
    ).rejects.toThrow("500");
  });
});

describe("allPatientsExportKickOff", () => {
  it("makes GET request to /Patient/$export endpoint", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=abc");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    await allPatientsExportKickOff("https://example.com/fhir", {});

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining("/Patient/$export"),
      expect.any(Object),
    );
  });
});

describe("patientExportKickOff", () => {
  it("makes GET request to /Patient/{id}/$export endpoint", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=abc");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    await patientExportKickOff("https://example.com/fhir", {
      patientId: "patient-123",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining("/Patient/patient-123/$export"),
      expect.any(Object),
    );
  });
});

describe("groupExportKickOff", () => {
  it("makes GET request to /Group/{id}/$export endpoint", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=abc");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    await groupExportKickOff("https://example.com/fhir", {
      groupId: "group-456",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining("/Group/group-456/$export"),
      expect.any(Object),
    );
  });
});

describe("bulkExportStatus", () => {
  it("makes GET request to polling URL", async () => {
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        { name: "transactionTime", valueInstant: "2024-01-01T00:00:00Z" },
        { name: "request", valueString: "https://example.com/$export" },
        { name: "requiresAccessToken", valueBoolean: false },
      ],
    };

    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(manifest), { status: 200 }),
    );

    await bulkExportStatus("https://example.com/fhir", {
      pollingUrl: "https://example.com/$job?id=abc",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/$job?id=abc",
      expect.objectContaining({
        method: "GET",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
        }),
      }),
    );
  });

  it("resolves relative polling URLs against base URL", async () => {
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        { name: "transactionTime", valueInstant: "2024-01-01T00:00:00Z" },
        { name: "request", valueString: "https://example.com/$export" },
        { name: "requiresAccessToken", valueBoolean: false },
      ],
    };

    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(manifest), { status: 200 }),
    );

    await bulkExportStatus("https://example.com/fhir", {
      pollingUrl: "/$job?id=abc",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$job?id=abc",
      expect.any(Object),
    );
  });

  it("returns in-progress status with progress from 202 response", async () => {
    const headers = new Headers();
    headers.set("X-Progress", "50%");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    const result = await bulkExportStatus("https://example.com/fhir", {
      pollingUrl: "https://example.com/$job?id=abc",
    });

    expect(result.status).toBe("in-progress");
    expect(result.progress).toBe("50%");
  });

  it("returns complete status with manifest from 200 response", async () => {
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        { name: "transactionTime", valueInstant: "2024-01-01T00:00:00Z" },
        { name: "request", valueString: "https://example.com/$export" },
        { name: "requiresAccessToken", valueBoolean: true },
        {
          name: "output",
          part: [
            { name: "type", valueCode: "Patient" },
            {
              name: "url",
              valueUri: "https://example.com/files/patient.ndjson",
            },
          ],
        },
      ],
    };

    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(manifest), { status: 200 }),
    );

    const result = await bulkExportStatus("https://example.com/fhir", {
      pollingUrl: "https://example.com/$job?id=abc",
    });

    expect(result.status).toBe("complete");
    expect(result.manifest).toEqual(manifest);
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      bulkExportStatus("https://example.com/fhir", {
        pollingUrl: "https://example.com/$job?id=abc",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });
});

describe("bulkExportDownload", () => {
  it("makes GET request to file URL", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await bulkExportDownload("https://example.com/fhir", {
      fileUrl: "https://example.com/files/patient.ndjson",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/files/patient.ndjson",
      expect.objectContaining({
        method: "GET",
        headers: expect.objectContaining({
          Accept: "application/fhir+ndjson",
        }),
      }),
    );
  });

  it("includes Authorization header when access token provided", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await bulkExportDownload("https://example.com/fhir", {
      fileUrl: "https://example.com/files/patient.ndjson",
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

    const result = await bulkExportDownload("https://example.com/fhir", {
      fileUrl: "https://example.com/files/patient.ndjson",
    });

    expect(result).toBeInstanceOf(ReadableStream);
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      bulkExportDownload("https://example.com/fhir", {
        fileUrl: "https://example.com/files/patient.ndjson",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });
});

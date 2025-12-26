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
import { importKickOff, importPnpKickOff } from "../import";
import { UnauthorizedError } from "../../types/errors";

const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetAllMocks();
});

describe("importKickOff", () => {
  it("makes POST request to /$import endpoint", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job-status?id=abc");

    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202, headers }));

    await importKickOff("https://example.com/fhir", {
      input: [{ type: "Patient", url: "s3://bucket/patient.ndjson" }],
      inputFormat: "application/fhir+ndjson",
      mode: "overwrite",
      inputSource: "s3://bucket/",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$import",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
          "Content-Type": "application/json",
          Prefer: "respond-async",
        }),
      }),
    );
  });

  it("includes input array in request body", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job-status?id=abc");

    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202, headers }));

    const input = [
      { type: "Patient", url: "s3://bucket/patient.ndjson" },
      { type: "Observation", url: "s3://bucket/observation.ndjson" },
    ];

    await importKickOff("https://example.com/fhir", {
      input,
      inputFormat: "application/fhir+ndjson",
      mode: "merge",
      inputSource: "s3://bucket/",
    });

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.input).toEqual(input);
    expect(body.inputFormat).toBe("application/fhir+ndjson");
    expect(body.mode).toBe("merge");
    expect(body.inputSource).toBe("s3://bucket/");
  });

  it("includes Authorization header when access token provided", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job-status?id=abc");

    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202, headers }));

    await importKickOff("https://example.com/fhir", {
      input: [{ type: "Patient", url: "s3://bucket/patient.ndjson" }],
      inputFormat: "application/fhir+ndjson",
      mode: "overwrite",
      inputSource: "s3://bucket/",
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

  it("returns job ID from Content-Location header", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job-status?id=import-job-123");

    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202, headers }));

    const result = await importKickOff("https://example.com/fhir", {
      input: [{ type: "Patient", url: "s3://bucket/patient.ndjson" }],
      inputFormat: "application/fhir+ndjson",
      mode: "overwrite",
      inputSource: "s3://bucket/",
    });

    expect(result.jobId).toBe("import-job-123");
  });

  it("throws error when Content-Location header missing", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202 }));

    await expect(
      importKickOff("https://example.com/fhir", {
        input: [{ type: "Patient", url: "s3://bucket/patient.ndjson" }],
        inputFormat: "application/fhir+ndjson",
        mode: "overwrite",
        inputSource: "s3://bucket/",
      }),
    ).rejects.toThrow("No Content-Location header");
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(new Response("Unauthorized", { status: 401 }));

    await expect(
      importKickOff("https://example.com/fhir", {
        input: [{ type: "Patient", url: "s3://bucket/patient.ndjson" }],
        inputFormat: "application/fhir+ndjson",
        mode: "overwrite",
        inputSource: "s3://bucket/",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });

  it("throws error on non-202 response", async () => {
    mockFetch.mockResolvedValueOnce(new Response("Error", { status: 500 }));

    await expect(
      importKickOff("https://example.com/fhir", {
        input: [{ type: "Patient", url: "s3://bucket/patient.ndjson" }],
        inputFormat: "application/fhir+ndjson",
        mode: "overwrite",
        inputSource: "s3://bucket/",
      }),
    ).rejects.toThrow("500");
  });
});

describe("importPnpKickOff", () => {
  it("makes POST request to /$import-pnp endpoint", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job-status?id=abc");

    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202, headers }));

    await importPnpKickOff("https://example.com/fhir", {
      exportUrl: "https://source.com/$export",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$import-pnp",
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

  it("builds FHIR Parameters resource in request body", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job-status?id=abc");

    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202, headers }));

    await importPnpKickOff("https://example.com/fhir", {
      exportUrl: "https://source.com/$export",
      exportType: "dynamic",
      saveMode: "merge",
    });

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.resourceType).toBe("Parameters");
    expect(body.parameter).toContainEqual({
      name: "exportUrl",
      valueUrl: "https://source.com/$export",
    });
  });

  it("includes optional parameters when provided", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job-status?id=abc");

    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202, headers }));

    await importPnpKickOff("https://example.com/fhir", {
      exportUrl: "https://source.com/$export",
      exportType: "static",
      saveMode: "overwrite",
    });

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.parameter).toContainEqual({
      name: "exportType",
      valueCoding: { code: "static" },
    });
    expect(body.parameter).toContainEqual({
      name: "mode",
      valueCoding: { code: "overwrite" },
    });
  });

  it("includes inputSource parameter when provided", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job-status?id=abc");

    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202, headers }));

    await importPnpKickOff("https://example.com/fhir", {
      exportUrl: "https://source.com/$export",
      inputSource: "https://source.com/fhir",
    });

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.parameter).toContainEqual({
      name: "inputSource",
      valueUrl: "https://source.com/fhir",
    });
  });

  it("returns job ID from Content-Location header", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job-status?id=pnp-job-456");

    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202, headers }));

    const result = await importPnpKickOff("https://example.com/fhir", {
      exportUrl: "https://source.com/$export",
    });

    expect(result.jobId).toBe("pnp-job-456");
  });

  it("includes Authorization header when access token provided", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job-status?id=abc");

    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202, headers }));

    await importPnpKickOff("https://example.com/fhir", {
      exportUrl: "https://source.com/$export",
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
    mockFetch.mockResolvedValueOnce(new Response("Unauthorized", { status: 401 }));

    await expect(
      importPnpKickOff("https://example.com/fhir", {
        exportUrl: "https://source.com/$export",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });
});

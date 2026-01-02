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

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ViewDefinition } from "../../types/api";
import { UnauthorizedError } from "../../types/errors";
import {
  viewExportDownload,
  viewExportKickOff,
  viewRun,
  viewRunStored,
} from "../view";

const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetAllMocks();
});

const sampleViewDefinition: ViewDefinition = {
  resourceType: "ViewDefinition",
  name: "patient-view",
  resource: "Patient",
  status: "active",
  select: [{ column: [{ path: "id", name: "patient_id" }] }],
};

describe("viewRun", () => {
  it("makes POST request to /ViewDefinition/$run endpoint", async () => {
    const responseStream = new ReadableStream();
    mockFetch.mockResolvedValueOnce(
      new Response(responseStream, { status: 200 }),
    );

    await viewRun("https://example.com/fhir", {
      viewDefinition: sampleViewDefinition,
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/ViewDefinition/$run",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          Accept: "application/x-ndjson",
          "Content-Type": "application/fhir+json",
        }),
      }),
    );
  });

  it("includes view definition in request body as Parameters", async () => {
    const responseStream = new ReadableStream();
    mockFetch.mockResolvedValueOnce(
      new Response(responseStream, { status: 200 }),
    );

    await viewRun("https://example.com/fhir", {
      viewDefinition: sampleViewDefinition,
    });

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.resourceType).toBe("Parameters");
    expect(body.parameter).toContainEqual(
      expect.objectContaining({
        name: "viewResource",
      }),
    );
  });

  it("includes format parameter when specified", async () => {
    const responseStream = new ReadableStream();
    mockFetch.mockResolvedValueOnce(
      new Response(responseStream, { status: 200 }),
    );

    await viewRun("https://example.com/fhir", {
      viewDefinition: sampleViewDefinition,
      format: "csv",
    });

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.parameter).toContainEqual({
      name: "_format",
      valueString: "csv",
    });
  });

  it("includes limit parameter when specified", async () => {
    const responseStream = new ReadableStream();
    mockFetch.mockResolvedValueOnce(
      new Response(responseStream, { status: 200 }),
    );

    await viewRun("https://example.com/fhir", {
      viewDefinition: sampleViewDefinition,
      limit: 100,
    });

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.parameter).toContainEqual({
      name: "_limit",
      valueInteger: 100,
    });
  });

  it("includes Authorization header when access token provided", async () => {
    const responseStream = new ReadableStream();
    mockFetch.mockResolvedValueOnce(
      new Response(responseStream, { status: 200 }),
    );

    await viewRun("https://example.com/fhir", {
      viewDefinition: sampleViewDefinition,
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
    const responseStream = new ReadableStream();
    mockFetch.mockResolvedValueOnce(
      new Response(responseStream, { status: 200 }),
    );

    const result = await viewRun("https://example.com/fhir", {
      viewDefinition: sampleViewDefinition,
    });

    expect(result).toBeInstanceOf(ReadableStream);
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      viewRun("https://example.com/fhir", {
        viewDefinition: sampleViewDefinition,
      }),
    ).rejects.toThrow(UnauthorizedError);
  });
});

describe("viewRunStored", () => {
  it("makes GET request to /ViewDefinition/{id}/$run endpoint", async () => {
    const responseStream = new ReadableStream();
    mockFetch.mockResolvedValueOnce(
      new Response(responseStream, { status: 200 }),
    );

    await viewRunStored("https://example.com/fhir", {
      viewDefinitionId: "view-123",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining("/ViewDefinition/view-123/$run"),
      expect.objectContaining({
        method: "GET",
        headers: expect.objectContaining({
          Accept: "application/x-ndjson",
        }),
      }),
    );
  });

  it("includes query parameters when specified", async () => {
    const responseStream = new ReadableStream();
    mockFetch.mockResolvedValueOnce(
      new Response(responseStream, { status: 200 }),
    );

    await viewRunStored("https://example.com/fhir", {
      viewDefinitionId: "view-123",
      format: "csv",
      limit: 50,
      header: true,
    });

    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("_format=csv");
    expect(url).toContain("_limit=50");
    expect(url).toContain("_header=true");
  });

  it("returns ReadableStream on success", async () => {
    const responseStream = new ReadableStream();
    mockFetch.mockResolvedValueOnce(
      new Response(responseStream, { status: 200 }),
    );

    const result = await viewRunStored("https://example.com/fhir", {
      viewDefinitionId: "view-123",
    });

    expect(result).toBeInstanceOf(ReadableStream);
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      viewRunStored("https://example.com/fhir", {
        viewDefinitionId: "view-123",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });
});

describe("viewExportKickOff", () => {
  it("makes POST request to /$viewdefinition-export endpoint", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=abc");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    await viewExportKickOff("https://example.com/fhir", {
      views: [{ viewDefinition: sampleViewDefinition }],
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$viewdefinition-export",
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

  it("includes views in request body as Parameters", async () => {
    const headers = new Headers();
    headers.set("Content-Location", "https://example.com/$job?id=abc");

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    await viewExportKickOff("https://example.com/fhir", {
      views: [{ viewDefinition: sampleViewDefinition, name: "my-view" }],
      format: "parquet",
    });

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.resourceType).toBe("Parameters");
    expect(body.parameter).toContainEqual(
      expect.objectContaining({ name: "view" }),
    );
    expect(body.parameter).toContainEqual({
      name: "_format",
      valueString: "parquet",
    });
  });

  it("returns job ID from Content-Location header", async () => {
    const headers = new Headers();
    headers.set(
      "Content-Location",
      "https://example.com/$job?id=view-export-123",
    );

    mockFetch.mockResolvedValueOnce(
      new Response(null, { status: 202, headers }),
    );

    const result = await viewExportKickOff("https://example.com/fhir", {
      views: [{ viewDefinition: sampleViewDefinition }],
    });

    expect(result.jobId).toBe("view-export-123");
  });

  it("throws error when Content-Location header missing", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202 }));

    await expect(
      viewExportKickOff("https://example.com/fhir", {
        views: [{ viewDefinition: sampleViewDefinition }],
      }),
    ).rejects.toThrow("No Content-Location header");
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      viewExportKickOff("https://example.com/fhir", {
        views: [{ viewDefinition: sampleViewDefinition }],
      }),
    ).rejects.toThrow(UnauthorizedError);
  });
});

describe("viewExportDownload", () => {
  it("makes GET request to job download URL", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await viewExportDownload("https://example.com/fhir", {
      jobId: "view-export-123",
      fileName: "patient-view.parquet",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$job-output/view-export-123/patient-view.parquet",
      expect.objectContaining({
        method: "GET",
      }),
    );
  });

  it("includes Authorization header when access token provided", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await viewExportDownload("https://example.com/fhir", {
      jobId: "view-export-123",
      fileName: "patient-view.parquet",
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

    const result = await viewExportDownload("https://example.com/fhir", {
      jobId: "view-export-123",
      fileName: "patient-view.parquet",
    });

    expect(result).toBeInstanceOf(ReadableStream);
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      viewExportDownload("https://example.com/fhir", {
        jobId: "view-export-123",
        fileName: "patient-view.parquet",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });
});

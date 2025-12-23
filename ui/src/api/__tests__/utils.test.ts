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

import { describe, it, expect } from "vitest";
import {
  buildHeaders,
  buildUrl,
  resolveUrl,
  checkResponse,
  extractJobIdFromUrl,
  parseProgressHeader,
} from "../utils";
import { UnauthorizedError, NotFoundError } from "../../types/errors";

describe("buildHeaders", () => {
  it("returns Accept header with FHIR JSON by default", () => {
    const headers = buildHeaders();
    expect(headers).toEqual({
      Accept: "application/fhir+json",
    });
  });

  it("includes Authorization header when access token provided", () => {
    const headers = buildHeaders({ accessToken: "test-token" });
    expect(headers).toEqual({
      Accept: "application/fhir+json",
      Authorization: "Bearer test-token",
    });
  });

  it("omits Authorization header when access token is undefined", () => {
    const headers = buildHeaders({ accessToken: undefined });
    expect(headers).not.toHaveProperty("Authorization");
  });

  it("includes Content-Type header when specified", () => {
    const headers = buildHeaders({ contentType: "application/json" });
    expect(headers).toEqual({
      Accept: "application/fhir+json",
      "Content-Type": "application/json",
    });
  });

  it("includes custom Accept header when specified", () => {
    const headers = buildHeaders({ accept: "application/x-ndjson" });
    expect(headers).toEqual({
      Accept: "application/x-ndjson",
    });
  });

  it("includes Prefer header when specified", () => {
    const headers = buildHeaders({ prefer: "respond-async" });
    expect(headers).toEqual({
      Accept: "application/fhir+json",
      Prefer: "respond-async",
    });
  });

  it("combines all header options", () => {
    const headers = buildHeaders({
      accessToken: "my-token",
      contentType: "application/fhir+json",
      accept: "application/fhir+json",
      prefer: "respond-async",
    });
    expect(headers).toEqual({
      Accept: "application/fhir+json",
      Authorization: "Bearer my-token",
      "Content-Type": "application/fhir+json",
      Prefer: "respond-async",
    });
  });
});

describe("buildUrl", () => {
  it("joins base and path with no params", () => {
    const url = buildUrl("https://example.com/fhir", "/Patient");
    expect(url).toBe("https://example.com/fhir/Patient");
  });

  it("handles base URL with trailing slash", () => {
    const url = buildUrl("https://example.com/fhir/", "/Patient");
    expect(url).toBe("https://example.com/fhir/Patient");
  });

  it("handles path without leading slash", () => {
    const url = buildUrl("https://example.com/fhir", "Patient");
    expect(url).toBe("https://example.com/fhir/Patient");
  });

  it("appends query parameters", () => {
    const url = buildUrl("https://example.com/fhir", "/Patient", {
      _count: "10",
      _sort: "name",
    });
    expect(url).toBe("https://example.com/fhir/Patient?_count=10&_sort=name");
  });

  it("handles empty params object", () => {
    const url = buildUrl("https://example.com/fhir", "/Patient", {});
    expect(url).toBe("https://example.com/fhir/Patient");
  });

  it("URL-encodes parameter values", () => {
    const url = buildUrl("https://example.com/fhir", "/Patient", {
      filter: "name contains 'test'",
    });
    expect(url).toBe(
      "https://example.com/fhir/Patient?filter=name+contains+%27test%27",
    );
  });
});

describe("resolveUrl", () => {
  it("returns absolute URLs unchanged", () => {
    const resolved = resolveUrl(
      "https://example.com/fhir",
      "https://other.com/path",
    );
    expect(resolved).toBe("https://other.com/path");
  });

  it("resolves relative URLs against base URL", () => {
    const resolved = resolveUrl(
      "https://example.com/fhir",
      "/$job-status?id=123",
    );
    expect(resolved).toBe("https://example.com/fhir/$job-status?id=123");
  });

  it("handles relative URLs without leading slash", () => {
    const resolved = resolveUrl("https://example.com/fhir", "$job-status");
    expect(resolved).toBe("https://example.com/fhir/$job-status");
  });

  it("preserves query parameters in relative URLs", () => {
    const resolved = resolveUrl(
      "https://example.com/fhir",
      "/jobs/abc?format=json",
    );
    expect(resolved).toBe("https://example.com/fhir/jobs/abc?format=json");
  });

  it("handles http URLs as absolute", () => {
    const resolved = resolveUrl(
      "https://example.com/fhir",
      "http://insecure.com/path",
    );
    expect(resolved).toBe("http://insecure.com/path");
  });
});

describe("checkResponse", () => {
  it("does not throw for successful responses", async () => {
    const response = new Response(JSON.stringify({ status: "ok" }), {
      status: 200,
    });
    await expect(checkResponse(response)).resolves.toBeUndefined();
  });

  it("throws UnauthorizedError for 401 responses", async () => {
    const response = new Response("Unauthorized", { status: 401 });
    await expect(checkResponse(response)).rejects.toThrow(UnauthorizedError);
  });

  it("throws NotFoundError for 404 responses", async () => {
    const response = new Response("Not found", { status: 404 });
    await expect(checkResponse(response)).rejects.toThrow(NotFoundError);
  });

  it("throws Error with status and body for other error responses", async () => {
    const response = new Response("Internal server error", { status: 500 });
    await expect(checkResponse(response)).rejects.toThrow(
      "Request failed: 500 - Internal server error",
    );
  });

  it("includes custom context in error message when provided", async () => {
    const response = new Response("Bad request", { status: 400 });
    await expect(checkResponse(response, "Import kick-off")).rejects.toThrow(
      "Import kick-off failed: 400 - Bad request",
    );
  });

  it("handles empty response body in errors", async () => {
    const response = new Response("", { status: 503 });
    await expect(checkResponse(response)).rejects.toThrow(
      "Request failed: 503 - ",
    );
  });
});

describe("extractJobIdFromUrl", () => {
  it("extracts job ID from query parameter", () => {
    const jobId = extractJobIdFromUrl(
      "https://example.com/$job-status?id=abc-123",
    );
    expect(jobId).toBe("abc-123");
  });

  it("extracts job ID from path segment with /jobs/", () => {
    const jobId = extractJobIdFromUrl(
      "https://example.com/jobs/abc-123-def-456",
    );
    expect(jobId).toBe("abc-123-def-456");
  });

  it("extracts job ID from path segment with /job/", () => {
    const jobId = extractJobIdFromUrl("https://example.com/job/xyz-789");
    expect(jobId).toBe("xyz-789");
  });

  it("throws error when job ID cannot be extracted", () => {
    expect(() => extractJobIdFromUrl("https://example.com/status")).toThrow(
      "Could not extract job ID from URL",
    );
  });

  it("handles relative URLs with query parameters", () => {
    const jobId = extractJobIdFromUrl("/$job-status?id=relative-id");
    expect(jobId).toBe("relative-id");
  });
});

describe("parseProgressHeader", () => {
  it("parses percentage format", () => {
    expect(parseProgressHeader("45%")).toBe(45);
  });

  it("parses plain number format", () => {
    expect(parseProgressHeader("75")).toBe(75);
  });

  it("extracts number from descriptive text", () => {
    expect(parseProgressHeader("Processing: 30% complete")).toBe(30);
  });

  it("returns 0 for non-numeric strings", () => {
    expect(parseProgressHeader("in progress")).toBe(0);
  });

  it("returns 0 for empty string", () => {
    expect(parseProgressHeader("")).toBe(0);
  });

  it("handles decimal numbers by taking integer part", () => {
    expect(parseProgressHeader("50.5%")).toBe(50);
  });
});

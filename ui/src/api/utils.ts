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

import { NotFoundError, UnauthorizedError } from "../types/errors";

/**
 * Options for building HTTP headers.
 */
export interface BuildHeadersOptions {
  /** OAuth2 access token for Bearer authentication. */
  accessToken?: string;
  /** Content-Type header value (e.g., "application/json"). */
  contentType?: string;
  /** Accept header value. Defaults to "application/fhir+json". */
  accept?: string;
  /** Prefer header value (e.g., "respond-async"). */
  prefer?: string;
}

/**
 * Builds HTTP headers for API requests.
 *
 * @param options - Header configuration options.
 * @returns A HeadersInit object ready for use with fetch.
 *
 * @example
 * // Basic FHIR request headers.
 * buildHeaders() // { Accept: "application/fhir+json" }
 *
 * @example
 * // Authenticated async request.
 * buildHeaders({
 *   accessToken: "token123",
 *   prefer: "respond-async"
 * })
 */
export function buildHeaders(options: BuildHeadersOptions = {}): HeadersInit {
  const headers: HeadersInit = {
    Accept: options.accept ?? "application/fhir+json",
  };

  if (options.accessToken) {
    headers.Authorization = `Bearer ${options.accessToken}`;
  }

  if (options.contentType) {
    headers["Content-Type"] = options.contentType;
  }

  if (options.prefer) {
    headers.Prefer = options.prefer;
  }

  return headers;
}

/**
 * Builds a URL from base, path, and optional query parameters.
 *
 * @param base - The base URL (e.g., "https://example.com/fhir").
 * @param path - The path to append (e.g., "/Patient" or "Patient").
 * @param params - Optional query parameters as key-value pairs.
 * @returns The constructed URL string.
 *
 * @example
 * buildUrl("https://example.com/fhir", "/Patient", { _count: "10" })
 * // "https://example.com/fhir/Patient?_count=10"
 */
export function buildUrl(
  base: string,
  path: string,
  params?: Record<string, string>,
): string {
  // Normalise base URL by removing trailing slash.
  const normalisedBase = base.endsWith("/") ? base.slice(0, -1) : base;

  // Normalise path by ensuring leading slash.
  const normalisedPath = path.startsWith("/") ? path : `/${path}`;

  let url = `${normalisedBase}${normalisedPath}`;

  if (params && Object.keys(params).length > 0) {
    const searchParams = new URLSearchParams(params);
    url += `?${searchParams.toString()}`;
  }

  return url;
}

/**
 * Resolves a URL against a base URL. Returns absolute URLs unchanged.
 *
 * @param baseUrl - The base URL to resolve against.
 * @param url - The URL to resolve (absolute or relative).
 * @returns The resolved absolute URL.
 *
 * @example
 * // Absolute URL returned unchanged.
 * resolveUrl("https://example.com/fhir", "https://other.com/path")
 * // "https://other.com/path"
 *
 * @example
 * // Relative URL resolved against base.
 * resolveUrl("https://example.com/fhir", "/$job?id=123")
 * // "https://example.com/fhir/$job?id=123"
 */
export function resolveUrl(baseUrl: string, url: string): string {
  // Check if URL is already absolute.
  if (url.startsWith("http://") || url.startsWith("https://")) {
    return url;
  }

  // Normalise base URL by removing trailing slash.
  const normalisedBase = baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;

  // Normalise path by ensuring leading slash.
  const normalisedPath = url.startsWith("/") ? url : `/${url}`;

  return `${normalisedBase}${normalisedPath}`;
}

/**
 * Checks response status and throws appropriate errors for non-successful responses.
 *
 * @param response - The fetch Response object to check.
 * @param context - Optional context string for error messages (e.g., "Import kick-off").
 * @throws {UnauthorizedError} When response status is 401.
 * @throws {NotFoundError} When response status is 404.
 * @throws {Error} For other non-successful responses with status and body.
 *
 * @example
 * const response = await fetch(url);
 * await checkResponse(response, "Search");
 * const data = await response.json();
 */
export async function checkResponse(
  response: Response,
  context?: string,
): Promise<void> {
  if (response.ok) {
    return;
  }

  if (response.status === 401) {
    throw new UnauthorizedError();
  }

  if (response.status === 404) {
    throw new NotFoundError();
  }

  const errorBody = await response.text();
  const prefix = context ? `${context} failed` : "Request failed";
  throw new Error(`${prefix}: ${response.status} - ${errorBody}`);
}

/**
 * Extracts a job ID from a poll URL.
 *
 * Supports two URL formats:
 * - Query parameter: `/$job?id=abc-123`
 * - Path segment: `/jobs/abc-123` or `/job/abc-123`
 *
 * @param url - The URL to extract the job ID from.
 * @returns The extracted job ID.
 * @throws {Error} When job ID cannot be extracted from the URL.
 *
 * @example
 * extractJobIdFromUrl("https://example.com/$job?id=abc-123")
 * // "abc-123"
 *
 * @example
 * extractJobIdFromUrl("https://example.com/jobs/abc-123")
 * // "abc-123"
 */
export function extractJobIdFromUrl(url: string): string {
  // Try to extract from query parameter.
  const queryMatch = /[?&]id=([^&]+)/.exec(url);
  if (queryMatch) {
    return queryMatch[1];
  }

  // Try to extract from path segment (/job/ or /jobs/).
  const pathMatch = /\/jobs?\/([a-z0-9-]+)/i.exec(url);
  if (pathMatch) {
    return pathMatch[1];
  }

  throw new Error("Could not extract job ID from URL");
}

/**
 * Parses the X-Progress header value into a numeric percentage.
 *
 * @param progressHeader - The X-Progress header value (e.g., "45%", "75", "Processing: 30% complete").
 * @returns The numeric percentage value, or 0 if no number is found.
 *
 * @example
 * parseProgressHeader("45%") // 45
 * parseProgressHeader("Processing: 30% complete") // 30
 * parseProgressHeader("in progress") // 0
 */
export function parseProgressHeader(progressHeader: string): number {
  const match = /(\d+)/.exec(progressHeader);
  if (match) {
    return Number.parseInt(match[1], 10);
  }
  return 0;
}

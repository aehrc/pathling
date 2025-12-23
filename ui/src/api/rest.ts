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

import type { Bundle, Resource } from "fhir/r4";
import type {
  SearchOptions,
  SearchResult,
  ReadOptions,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
} from "../types/api";
import { buildHeaders, buildUrl, checkResponse } from "./utils";

/**
 * Searches for FHIR resources of a given type.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Search options including resource type, count, and filters.
 * @returns A FHIR Bundle containing matching resources.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const bundle = await search("https://example.com/fhir", {
 *   resourceType: "Patient",
 *   count: 10,
 *   filters: ["name.family = 'Smith'"],
 *   accessToken: "token123"
 * });
 */
export async function search(
  baseUrl: string,
  options: SearchOptions,
): Promise<SearchResult> {
  // Start with any explicitly provided params.
  const searchParams = new URLSearchParams();

  if (options.count !== undefined) {
    searchParams.set("_count", String(options.count));
  }

  // Add generic params (can have multiple values per key).
  if (options.params) {
    for (const [key, value] of Object.entries(options.params)) {
      if (Array.isArray(value)) {
        for (const v of value) {
          searchParams.append(key, v);
        }
      } else {
        searchParams.append(key, value);
      }
    }
  }

  // Add filter parameters (can have multiple with same key).
  if (options.filters && options.filters.length > 0) {
    for (const filter of options.filters) {
      const trimmed = filter.trim();
      if (trimmed) {
        searchParams.append("filter", trimmed);
      }
    }
  }

  const queryString = searchParams.toString();
  const url = buildUrl(baseUrl, `/${options.resourceType}`) +
    (queryString ? `?${queryString}` : "");

  const headers = buildHeaders({ accessToken: options.accessToken });

  const response = await fetch(url, {
    method: "GET",
    headers,
  });

  await checkResponse(response, "Search");

  return (await response.json()) as Bundle;
}

/**
 * Reads a single FHIR resource by type and ID.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Read options including resource type and ID.
 * @returns The requested FHIR resource.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {NotFoundError} When the resource does not exist.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const patient = await read("https://example.com/fhir", {
 *   resourceType: "Patient",
 *   id: "123",
 *   accessToken: "token123"
 * });
 */
export async function read(
  baseUrl: string,
  options: ReadOptions,
): Promise<Resource> {
  const url = buildUrl(baseUrl, `/${options.resourceType}/${options.id}`);
  const headers = buildHeaders({ accessToken: options.accessToken });

  const response = await fetch(url, {
    method: "GET",
    headers,
  });

  await checkResponse(response, "Read");

  return (await response.json()) as Resource;
}

/**
 * Creates a new FHIR resource.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Create options including resource type and the resource to create.
 * @returns The created FHIR resource with server-assigned ID.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const patient = await create("https://example.com/fhir", {
 *   resourceType: "Patient",
 *   resource: { resourceType: "Patient", name: [{ family: "Smith" }] },
 *   accessToken: "token123"
 * });
 */
export async function create(
  baseUrl: string,
  options: CreateOptions,
): Promise<Resource> {
  const url = buildUrl(baseUrl, `/${options.resourceType}`);
  const headers = buildHeaders({
    accessToken: options.accessToken,
    contentType: "application/fhir+json",
  });

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(options.resource),
  });

  await checkResponse(response, "Create");

  return (await response.json()) as Resource;
}

/**
 * Updates an existing FHIR resource.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Update options including resource type, ID, and the updated resource.
 * @returns The updated FHIR resource.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {NotFoundError} When the resource does not exist.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const patient = await update("https://example.com/fhir", {
 *   resourceType: "Patient",
 *   id: "123",
 *   resource: { resourceType: "Patient", id: "123", name: [{ family: "Jones" }] },
 *   accessToken: "token123"
 * });
 */
export async function update(
  baseUrl: string,
  options: UpdateOptions,
): Promise<Resource> {
  const url = buildUrl(baseUrl, `/${options.resourceType}/${options.id}`);
  const headers = buildHeaders({
    accessToken: options.accessToken,
    contentType: "application/fhir+json",
  });

  const response = await fetch(url, {
    method: "PUT",
    headers,
    body: JSON.stringify(options.resource),
  });

  await checkResponse(response, "Update");

  return (await response.json()) as Resource;
}

/**
 * Deletes a FHIR resource by type and ID.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Delete options including resource type and ID.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {NotFoundError} When the resource does not exist.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * await deleteResource("https://example.com/fhir", {
 *   resourceType: "Patient",
 *   id: "123",
 *   accessToken: "token123"
 * });
 */
export async function deleteResource(
  baseUrl: string,
  options: DeleteOptions,
): Promise<void> {
  const url = buildUrl(baseUrl, `/${options.resourceType}/${options.id}`);
  const headers = buildHeaders({ accessToken: options.accessToken });

  const response = await fetch(url, {
    method: "DELETE",
    headers,
  });

  await checkResponse(response, "Delete");
}

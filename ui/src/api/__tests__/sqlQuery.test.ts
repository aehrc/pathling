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
  listSqlQueryLibraries,
  listStoredLibraries,
  SQL_QUERY_LIBRARY_TYPE_FILTER,
  SQL_VIEW_LIBRARY_TYPE_FILTER,
} from "../sqlQuery";

const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetAllMocks();
});

describe("listSqlQueryLibraries", () => {
  // The search is scoped by the SQLQuery type code so unrelated Library
  // resources are excluded.
  it("queries Library with the SQLQuery type filter", async () => {
    const bundle = {
      resourceType: "Bundle",
      entry: [],
    };
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(bundle), {
        status: 200,
        headers: { "Content-Type": "application/fhir+json" },
      }),
    );

    await listSqlQueryLibraries("https://example.com/fhir");

    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("/Library?");
    expect(decodeURIComponent(url)).toContain(
      `type=${SQL_QUERY_LIBRARY_TYPE_FILTER}`,
    );
  });

  // Bundle entries are returned as-is so the hook layer can decide how to
  // shape them for the picker.
  it("returns the FHIR Bundle on success", async () => {
    const bundle = {
      resourceType: "Bundle",
      entry: [{ resource: { resourceType: "Library", id: "lib-1" } }],
    };
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(bundle), {
        status: 200,
        headers: { "Content-Type": "application/fhir+json" },
      }),
    );

    const result = await listSqlQueryLibraries("https://example.com/fhir");
    expect(result.entry?.[0]?.resource?.resourceType).toBe("Library");
  });

  // The Authorization header is forwarded for authenticated servers.
  it("attaches a bearer token when an access token is provided", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ resourceType: "Bundle" }), {
        status: 200,
        headers: { "Content-Type": "application/fhir+json" },
      }),
    );
    await listSqlQueryLibraries("https://example.com/fhir", {
      accessToken: "secret",
    });
    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer secret",
        }),
      }),
    );
  });

  // A 401 propagates as UnauthorizedError, consistent with the rest of
  // the API layer.
  it("throws UnauthorizedError on a 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );
    await expect(
      listSqlQueryLibraries("https://example.com/fhir"),
    ).rejects.toThrow(UnauthorizedError);
  });
});

describe("SQL_VIEW_LIBRARY_TYPE_FILTER", () => {
  // The SQLView filter shares the SQL on FHIR Library code system with the
  // SQLQuery filter; only the type code differs.
  it("is the SQL on FHIR Library code system scoped to sql-view", () => {
    expect(SQL_VIEW_LIBRARY_TYPE_FILTER).toBe(
      "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes|sql-view",
    );
  });
});

describe("listStoredLibraries", () => {
  // The generalised list function scopes its search by the requested type
  // code, issuing the SQLView token for sql-view requests.
  it("queries Library with the sql-view type filter", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ resourceType: "Bundle", entry: [] }), {
        status: 200,
        headers: { "Content-Type": "application/fhir+json" },
      }),
    );

    await listStoredLibraries("https://example.com/fhir", {
      typeCode: "sql-view",
    });

    const url = decodeURIComponent(mockFetch.mock.calls[0][0] as string);
    expect(url).toContain("/Library?");
    expect(url).toContain(`type=${SQL_VIEW_LIBRARY_TYPE_FILTER}`);
  });

  // The same function issues the SQLQuery token for sql-query requests, so
  // both Library kinds share one code path.
  it("queries Library with the sql-query type filter", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ resourceType: "Bundle", entry: [] }), {
        status: 200,
        headers: { "Content-Type": "application/fhir+json" },
      }),
    );

    await listStoredLibraries("https://example.com/fhir", {
      typeCode: "sql-query",
    });

    const url = decodeURIComponent(mockFetch.mock.calls[0][0] as string);
    expect(url).toContain(`type=${SQL_QUERY_LIBRARY_TYPE_FILTER}`);
  });

  // The Authorization header is forwarded for authenticated servers.
  it("attaches a bearer token when an access token is provided", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ resourceType: "Bundle" }), {
        status: 200,
        headers: { "Content-Type": "application/fhir+json" },
      }),
    );

    await listStoredLibraries("https://example.com/fhir", {
      typeCode: "sql-view",
      accessToken: "secret",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: "Bearer secret" }),
      }),
    );
  });

  // A 401 propagates as UnauthorizedError, consistent with the rest of the
  // API layer.
  it("throws UnauthorizedError on a 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );
    await expect(
      listStoredLibraries("https://example.com/fhir", { typeCode: "sql-view" }),
    ).rejects.toThrow(UnauthorizedError);
  });
});

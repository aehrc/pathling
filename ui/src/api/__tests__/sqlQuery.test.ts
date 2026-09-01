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

import {
  NotFoundError,
  OperationOutcomeError,
  UnauthorizedError,
} from "../../types/errors";
import {
  listSqlQueryLibraries,
  listStoredLibraries,
  SQL_QUERY_LIBRARY_TYPE_FILTER,
  SQL_VIEW_LIBRARY_TYPE_FILTER,
  sqlQueryExportDownload,
  sqlQueryExportKickOff,
  sqlQueryRun,
} from "../sqlQuery";

import type { SqlQueryLibrary } from "../../types/sqlQuery";

const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetAllMocks();
});

const sampleLibrary: SqlQueryLibrary = {
  resourceType: "Library",
  status: "active",
  type: {
    coding: [
      {
        system: "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
        code: "sql-query",
      },
    ],
  },
  content: [
    {
      contentType: "application/sql",
      data: "U0VMRUNUIDE=",
    },
  ],
  relatedArtifact: [
    {
      type: "depends-on",
      label: "patients",
      resource: "ViewDefinition/patients",
    },
  ],
};

describe("sqlQueryRun", () => {
  // The inline mode posts a Parameters body containing a `queryResource`
  // entry whose value is the assembled Library.
  it("posts queryResource in inline mode", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await sqlQueryRun("https://example.com/fhir", {
      mode: "inline",
      library: sampleLibrary,
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$sqlquery-run",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          "Content-Type": "application/fhir+json",
        }),
      }),
    );
    const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(requestBody.resourceType).toBe("Parameters");
    expect(requestBody.parameter).toContainEqual(
      expect.objectContaining({
        name: "queryResource",
        resource: expect.objectContaining({ resourceType: "Library" }),
      }),
    );
  });

  // The stored mode submits a `queryReference` parameter with a relative
  // `Library/<id>` reference.
  it("posts queryReference in stored mode", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await sqlQueryRun("https://example.com/fhir", {
      mode: "stored",
      libraryId: "lib-123",
    });

    const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(requestBody.parameter).toContainEqual({
      name: "queryReference",
      valueReference: { reference: "Library/lib-123" },
    });
  });

  // The format, limit and header options are translated into Parameters
  // entries with the appropriate value[x] slots.
  it("includes _format, _limit and _header parameters when supplied", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await sqlQueryRun("https://example.com/fhir", {
      mode: "stored",
      libraryId: "lib-123",
      format: "csv",
      limit: 50,
      header: true,
    });

    const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(requestBody.parameter).toContainEqual({
      name: "_format",
      valueString: "csv",
    });
    expect(requestBody.parameter).toContainEqual({
      name: "_limit",
      valueInteger: 50,
    });
    expect(requestBody.parameter).toContainEqual({
      name: "_header",
      valueBoolean: true,
    });
  });

  // Empty bindings are omitted; non-empty bindings appear inside a nested
  // Parameters resource on the `parameters` parameter.
  it("nests runtime bindings as a Parameters resource", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await sqlQueryRun("https://example.com/fhir", {
      mode: "stored",
      libraryId: "lib-123",
      bindings: { patient_id: "Patient/pat-1", empty: "" },
      parameterTypes: { patient_id: "string", empty: "string" },
    });

    const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
    const paramsParam = requestBody.parameter.find(
      (p: { name: string }) => p.name === "parameters",
    );
    expect(paramsParam).toBeDefined();
    expect(paramsParam.resource.resourceType).toBe("Parameters");
    expect(paramsParam.resource.parameter).toEqual([
      { name: "patient_id", valueString: "Patient/pat-1" },
    ]);
  });

  // Bindings of different declared types map to the corresponding
  // value[x] slot.
  it("maps bindings to typed value[x] slots", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await sqlQueryRun("https://example.com/fhir", {
      mode: "stored",
      libraryId: "lib-123",
      bindings: {
        s: "abc",
        c: "approved",
        i: "42",
        d: "1.5",
        b: "true",
        date: "2025-01-01",
        ts: "2025-01-01T12:00:00Z",
      },
      parameterTypes: {
        s: "string",
        c: "code",
        i: "integer",
        d: "decimal",
        b: "boolean",
        date: "date",
        ts: "dateTime",
      },
    });

    const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
    const paramsParam = requestBody.parameter.find(
      (p: { name: string }) => p.name === "parameters",
    );
    expect(paramsParam.resource.parameter).toEqual([
      { name: "s", valueString: "abc" },
      { name: "c", valueCode: "approved" },
      { name: "i", valueInteger: 42 },
      { name: "d", valueDecimal: 1.5 },
      { name: "b", valueBoolean: true },
      { name: "date", valueDate: "2025-01-01" },
      { name: "ts", valueDateTime: "2025-01-01T12:00:00Z" },
    ]);
  });

  // The Authorization header is included when an access token is supplied.
  it("attaches a bearer token when an access token is provided", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await sqlQueryRun("https://example.com/fhir", {
      mode: "stored",
      libraryId: "lib-123",
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

  // The Accept header tracks the requested output format so the server
  // can pick the right streaming branch.
  it("sets the Accept header to match the requested format", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    await sqlQueryRun("https://example.com/fhir", {
      mode: "stored",
      libraryId: "lib-123",
      format: "csv",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({ Accept: "text/csv" }),
      }),
    );
  });

  // A 401 response should surface as an UnauthorizedError so the global
  // session-expired handler can react to it.
  it("throws UnauthorizedError on a 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );
    await expect(
      sqlQueryRun("https://example.com/fhir", {
        mode: "stored",
        libraryId: "lib-123",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });

  // A 400 response carrying an OperationOutcome surfaces as a structured
  // error that preserves the diagnostics text.
  it("throws OperationOutcomeError on a 400 with an OperationOutcome body", async () => {
    const outcome = {
      resourceType: "OperationOutcome",
      issue: [
        {
          severity: "error",
          code: "invalid",
          diagnostics: "SQL contains a disallowed operation",
        },
      ],
    };
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(outcome), {
        status: 400,
        headers: { "Content-Type": "application/fhir+json" },
      }),
    );
    await expect(
      sqlQueryRun("https://example.com/fhir", {
        mode: "stored",
        libraryId: "lib-123",
      }),
    ).rejects.toBeInstanceOf(OperationOutcomeError);
  });
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

describe("sqlQueryExportKickOff", () => {
  // The kick-off must request asynchronous processing and return the polling
  // URL carried by the Content-Location header.
  it("sets Prefer: respond-async and returns the polling URL", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(null, {
        status: 202,
        headers: { "Content-Location": "https://example.com/fhir/$job?id=abc" },
      }),
    );

    const result = await sqlQueryExportKickOff("https://example.com/fhir", {
      request: {
        mode: "stored",
        libraryId: "patient-bp-query",
        format: "ndjson",
      },
    });

    expect(result.pollingUrl).toBe("https://example.com/fhir/$job?id=abc");
    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$sqlquery-export",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({ Prefer: "respond-async" }),
      }),
    );
  });

  // Without a Content-Location header the kick-off cannot be polled, so it
  // fails rather than returning an undefined URL.
  it("throws when no Content-Location header is present", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202 }));
    await expect(
      sqlQueryExportKickOff("https://example.com/fhir", {
        request: { mode: "stored", libraryId: "q", format: "ndjson" },
      }),
    ).rejects.toThrow("No Content-Location");
  });
});

describe("sqlQueryExportDownload", () => {
  it("returns the response body stream on success", async () => {
    const body = new ReadableStream();
    mockFetch.mockResolvedValueOnce(new Response(body, { status: 200 }));

    const stream = await sqlQueryExportDownload({
      location: "https://example.com/fhir/$result?job=j&file=people.ndjson",
    });

    expect(stream).toBe(body);
    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/$result?job=j&file=people.ndjson",
      expect.objectContaining({ method: "GET" }),
    );
  });

  it("throws NotFoundError on a 404 response", async () => {
    mockFetch.mockResolvedValueOnce(new Response("Not found", { status: 404 }));
    await expect(
      sqlQueryExportDownload({
        location: "https://example.com/fhir/$result?job=j&file=x",
      }),
    ).rejects.toThrow(NotFoundError);
  });

  it("throws UnauthorizedError on a 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );
    await expect(
      sqlQueryExportDownload({
        location: "https://example.com/fhir/$result?job=j&file=x",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });
});

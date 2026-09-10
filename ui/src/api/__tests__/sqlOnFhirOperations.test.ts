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

import { NotFoundError, UnauthorizedError } from "../../types/errors";
import {
  parseSqlExportManifest,
  sqlExportKickOff,
  sqlRun,
  sqlRunStored,
} from "../sqlOnFhirOperations";

import type { Parameters, ParametersParameter } from "fhir/r4";

const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetAllMocks();
});

const BASE = "https://example.com/fhir";

const viewDefinition = {
  resourceType: "ViewDefinition",
  name: "patient_view",
  resource: "Patient",
  status: "active",
  select: [{ column: [{ path: "id", name: "id" }] }],
};

/**
 * Returns the parsed Parameters body of the single recorded fetch call.
 *
 * @returns The body that was sent.
 */
function sentBody(): Parameters {
  const init = mockFetch.mock.calls[0][1] as RequestInit;
  return JSON.parse(init.body as string) as Parameters;
}

/**
 * Returns the parts of the sent body with the given name.
 *
 * @param body - The Parameters body that was sent.
 * @param name - The parameter name to match.
 * @returns The matching parts, in order.
 */
function partsNamed(body: Parameters, name: string): ParametersParameter[] {
  return (body.parameter ?? []).filter((p) => p.name === name);
}

describe("sqlRun", () => {
  it("posts an inline subject to /$sql-run as subjectResource", async () => {
    mockFetch.mockResolvedValueOnce(new Response("{}", { status: 200 }));

    await sqlRun(BASE, {
      subject: { kind: "resource", resource: viewDefinition },
    });

    expect(mockFetch).toHaveBeenCalledWith(
      `${BASE}/$sql-run`,
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          "Content-Type": "application/fhir+json",
          Accept: "application/x-ndjson",
        }),
      }),
    );
    const subject = partsNamed(sentBody(), "subjectResource");
    expect(subject).toHaveLength(1);
    expect(subject[0].resource).toEqual(viewDefinition);
  });

  it("posts a stored subject as a typed subjectReference", async () => {
    mockFetch.mockResolvedValueOnce(new Response("{}", { status: 200 }));

    await sqlRun(BASE, {
      subject: { kind: "reference", reference: "Library/bp-query" },
    });

    expect(
      partsNamed(sentBody(), "subjectReference")[0].valueReference,
    ).toEqual({ reference: "Library/bp-query" });
  });

  it("posts a canonical subject as subjectCanonical", async () => {
    mockFetch.mockResolvedValueOnce(new Response("{}", { status: 200 }));

    await sqlRun(BASE, {
      subject: { kind: "canonical", canonical: "https://example.org/v" },
    });

    expect(partsNamed(sentBody(), "subjectCanonical")[0].valueCanonical).toBe(
      "https://example.org/v",
    );
  });

  it("carries runtime bindings as a nested parameters resource", async () => {
    mockFetch.mockResolvedValueOnce(new Response("{}", { status: 200 }));
    const bindings: Parameters = {
      resourceType: "Parameters",
      parameter: [{ name: "family", valueString: "Smith" }],
    };

    await sqlRun(BASE, {
      subject: { kind: "reference", reference: "Library/bp-query" },
      parameters: bindings,
    });

    expect(partsNamed(sentBody(), "parameters")[0].resource).toEqual(bindings);
  });

  it("sends the output settings and the filters", async () => {
    mockFetch.mockResolvedValueOnce(new Response("{}", { status: 200 }));

    await sqlRun(BASE, {
      subject: { kind: "reference", reference: "ViewDefinition/v" },
      format: "csv",
      limit: 25,
      header: false,
      patientIds: ["p1", "p2"],
      groupIds: ["g1"],
      since: "2026-01-01T00:00:00Z",
    });

    const body = sentBody();
    expect(partsNamed(body, "_format")[0].valueString).toBe("csv");
    expect(partsNamed(body, "_limit")[0].valueInteger).toBe(25);
    // The CSV header flag is named `header`, not `_header`.
    expect(partsNamed(body, "header")[0].valueBoolean).toBe(false);
    expect(partsNamed(body, "_header")).toHaveLength(0);
    expect(partsNamed(body, "patient").map((p) => p.valueReference)).toEqual([
      { reference: "Patient/p1" },
      { reference: "Patient/p2" },
    ]);
    expect(partsNamed(body, "group")[0].valueReference).toEqual({
      reference: "Group/g1",
    });
    expect(partsNamed(body, "_since")[0].valueInstant).toBe(
      "2026-01-01T00:00:00Z",
    );
  });

  it("negotiates the Accept header from the requested format", async () => {
    mockFetch.mockResolvedValueOnce(new Response("{}", { status: 200 }));

    await sqlRun(BASE, {
      subject: { kind: "reference", reference: "Library/q" },
      format: "fhir",
    });

    const init = mockFetch.mock.calls[0][1] as RequestInit;
    expect((init.headers as Record<string, string>).Accept).toBe(
      "application/fhir+json",
    );
  });

  it.each([
    [401, UnauthorizedError],
    [404, NotFoundError],
  ])("surfaces a %s response as a failure", async (status, errorType) => {
    mockFetch.mockResolvedValueOnce(new Response("nope", { status }));

    await expect(
      sqlRun(BASE, { subject: { kind: "reference", reference: "Library/q" } }),
    ).rejects.toBeInstanceOf(errorType);
  });

  it("surfaces a 400 OperationOutcome as a failure", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          resourceType: "OperationOutcome",
          issue: [{ severity: "error", code: "invalid" }],
        }),
        { status: 400 },
      ),
    );

    await expect(
      sqlRun(BASE, { subject: { kind: "reference", reference: "Library/q" } }),
    ).rejects.toThrow();
  });

  it("surfaces a 500 response as a failure", async () => {
    mockFetch.mockResolvedValueOnce(new Response("boom", { status: 500 }));

    await expect(
      sqlRun(BASE, { subject: { kind: "reference", reference: "Library/q" } }),
    ).rejects.toThrow();
  });
});

describe("sqlRunStored", () => {
  it("gets /$sql-run with the subject reference and output settings", async () => {
    mockFetch.mockResolvedValueOnce(new Response("{}", { status: 200 }));

    await sqlRunStored(BASE, {
      reference: "ViewDefinition/demographics",
      format: "csv",
      limit: 10,
      header: true,
    });

    const [url, init] = mockFetch.mock.calls[0] as [string, RequestInit];
    expect(init.method).toBe("GET");
    const query = new URL(url).searchParams;
    expect(new URL(url).pathname).toContain("/$sql-run");
    expect(query.get("subjectReference")).toBe("ViewDefinition/demographics");
    expect(query.get("_format")).toBe("csv");
    expect(query.get("_limit")).toBe("10");
    // The CSV header flag is named `header`, not `_header`.
    expect(query.get("header")).toBe("true");
    expect(query.get("_header")).toBeNull();
  });

  it("repeats the patient and group filters in the query string", async () => {
    mockFetch.mockResolvedValueOnce(new Response("{}", { status: 200 }));

    await sqlRunStored(BASE, {
      reference: "ViewDefinition/demographics",
      patientIds: ["p1", "p2"],
      groupIds: ["g1"],
      since: "2026-01-01T00:00:00Z",
    });

    const query = new URL(mockFetch.mock.calls[0][0] as string).searchParams;
    expect(query.getAll("patient")).toEqual(["Patient/p1", "Patient/p2"]);
    expect(query.getAll("group")).toEqual(["Group/g1"]);
    expect(query.get("_since")).toBe("2026-01-01T00:00:00Z");
  });

  it("surfaces an error response as a failure", async () => {
    mockFetch.mockResolvedValueOnce(new Response("nope", { status: 401 }));

    await expect(
      sqlRunStored(BASE, { reference: "ViewDefinition/v" }),
    ).rejects.toBeInstanceOf(UnauthorizedError);
  });
});

describe("sqlExportKickOff", () => {
  /**
   * Builds a 202 response carrying a status URL.
   *
   * @returns The accepted response.
   */
  function accepted(): Response {
    return new Response(null, {
      status: 202,
      headers: { "Content-Location": `${BASE}/$job?id=abc` },
    });
  }

  it("posts one subject repetition per entry, with its name and bindings", async () => {
    mockFetch.mockResolvedValueOnce(accepted());
    const bindings: Parameters = {
      resourceType: "Parameters",
      parameter: [{ name: "family", valueString: "Smith" }],
    };

    const { pollingUrl } = await sqlExportKickOff(BASE, {
      subjects: [
        {
          name: "demographics",
          subject: { kind: "canonical", canonical: "https://example.org/v" },
        },
        {
          name: "smiths",
          subject: { kind: "reference", reference: "Library/by-family" },
          parameters: bindings,
        },
      ],
    });

    expect(pollingUrl).toBe(`${BASE}/$job?id=abc`);
    expect(mockFetch).toHaveBeenCalledWith(
      `${BASE}/$sql-export`,
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({ Prefer: "respond-async" }),
      }),
    );

    const subjects = partsNamed(sentBody(), "subject");
    expect(subjects).toHaveLength(2);
    expect(subjects[0].part).toEqual([
      { name: "name", valueString: "demographics" },
      { name: "subjectCanonical", valueCanonical: "https://example.org/v" },
    ]);
    expect(subjects[1].part).toEqual([
      { name: "name", valueString: "smiths" },
      {
        name: "subjectReference",
        valueReference: { reference: "Library/by-family" },
      },
      { name: "parameters", resource: bindings },
    ]);
  });

  it("sends the job-level output settings and filters", async () => {
    mockFetch.mockResolvedValueOnce(accepted());

    await sqlExportKickOff(BASE, {
      subjects: [
        { subject: { kind: "reference", reference: "ViewDefinition/v" } },
      ],
      format: "parquet",
      header: false,
      clientTrackingId: "track-1",
      patientIds: ["p1"],
      groupIds: ["g1"],
      since: "2026-01-01T00:00:00Z",
    });

    const body = sentBody();
    expect(partsNamed(body, "_format")[0].valueCode).toBe("parquet");
    expect(partsNamed(body, "header")[0].valueBoolean).toBe(false);
    expect(partsNamed(body, "clientTrackingId")[0].valueString).toBe("track-1");
    expect(partsNamed(body, "patient")[0].valueReference).toEqual({
      reference: "Patient/p1",
    });
    expect(partsNamed(body, "group")[0].valueReference).toEqual({
      reference: "Group/g1",
    });
    expect(partsNamed(body, "_since")[0].valueInstant).toBe(
      "2026-01-01T00:00:00Z",
    );
  });

  it("fails when the kick-off is rejected", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          resourceType: "OperationOutcome",
          issue: [{ severity: "error", code: "required" }],
        }),
        { status: 400 },
      ),
    );

    await expect(
      sqlExportKickOff(BASE, {
        subjects: [
          { subject: { kind: "reference", reference: "ViewDefinition/v" } },
        ],
      }),
    ).rejects.toThrow();
  });

  it("fails when the accepted response carries no status URL", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 202 }));

    await expect(
      sqlExportKickOff(BASE, {
        subjects: [
          { subject: { kind: "reference", reference: "ViewDefinition/v" } },
        ],
      }),
    ).rejects.toThrow("No Content-Location header");
  });
});

describe("parseSqlExportManifest", () => {
  it("emits one entry per file, all sharing the output name", () => {
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        {
          name: "output",
          part: [
            { name: "name", valueString: "demographics" },
            { name: "location", valueUri: "https://example.com/a.ndjson" },
            { name: "location", valueUri: "https://example.com/b.ndjson" },
          ],
        },
        {
          name: "output",
          part: [
            { name: "name", valueString: "smiths" },
            { name: "location", valueUri: "https://example.com/c.ndjson" },
          ],
        },
      ],
    };

    expect(parseSqlExportManifest(manifest)).toEqual([
      { name: "demographics", url: "https://example.com/a.ndjson" },
      { name: "demographics", url: "https://example.com/b.ndjson" },
      { name: "smiths", url: "https://example.com/c.ndjson" },
    ]);
  });

  it("returns nothing for an absent or empty manifest", () => {
    expect(parseSqlExportManifest(null)).toEqual([]);
    expect(parseSqlExportManifest({ resourceType: "Parameters" })).toEqual([]);
  });
});

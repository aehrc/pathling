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

import { describe, expect, it } from "vitest";

import {
  findSourceByUrl,
  libraryToSummary,
  mapLibraryBundle,
  parseTabularBody,
  readSqlQueryResponse,
  storedReferencesToViewRows,
} from "../sqlQueryHelpers";

import type { SourceOption } from "../../types/sqlQuery";
import type { Bundle, Library } from "fhir/r4";

const libraryFixture: Library = {
  resourceType: "Library",
  id: "lib-1",
  name: "patients-by-condition",
  title: "Patients by condition",
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
      // "SELECT 1".
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
  parameter: [
    { name: "patient_id", use: "in", type: "string" },
    { name: "_unsupported", use: "in", type: "Quantity" },
  ],
};

describe("libraryToSummary", () => {
  // The summary surfaces decoded SQL and known parameter types, with
  // unsupported declared types coerced to `string` so the form can still
  // render an input.
  it("decodes SQL and maps parameters and related artefacts", () => {
    const summary = libraryToSummary(libraryFixture);
    expect(summary).toBeDefined();
    expect(summary?.id).toBe("lib-1");
    expect(summary?.title).toBe("Patients by condition");
    expect(summary?.sql).toBe("SELECT 1");
    expect(summary?.relatedArtifacts).toEqual([
      { label: "patients", reference: "ViewDefinition/patients" },
    ]);
    expect(summary?.parameters).toEqual([
      { name: "patient_id", type: "string" },
      { name: "_unsupported", type: "string" },
    ]);
  });

  // Resources without an ID or SQL content cannot be rendered, so they
  // are skipped at the summary layer.
  it("returns undefined when the resource lacks an ID", () => {
    const noId: Library = { ...libraryFixture, id: undefined };
    expect(libraryToSummary(noId)).toBeUndefined();
  });

  it("returns undefined when no application/sql content exists", () => {
    const noSql: Library = { ...libraryFixture, content: [] };
    expect(libraryToSummary(noSql)).toBeUndefined();
  });

  // The summary falls back to `name` then `id` when no `title` is set.
  it("falls back to name then id for the title", () => {
    const noTitle: Library = { ...libraryFixture, title: undefined };
    expect(libraryToSummary(noTitle)?.title).toBe("patients-by-condition");
    const onlyId: Library = {
      ...libraryFixture,
      title: undefined,
      name: undefined,
    };
    expect(libraryToSummary(onlyId)?.title).toBe("lib-1");
  });
});

describe("mapLibraryBundle", () => {
  // Empty bundles produce no summaries.
  it("returns an empty array for an empty bundle", () => {
    const bundle: Bundle = { resourceType: "Bundle", type: "searchset" };
    expect(mapLibraryBundle(bundle)).toEqual([]);
  });

  // Non-Library entries and Libraries that fail to summarise are skipped.
  it("skips non-Library entries and unsummarisable Libraries", () => {
    const bundle: Bundle = {
      resourceType: "Bundle",
      type: "searchset",
      entry: [
        { resource: { resourceType: "Patient", id: "x" } },
        {
          resource: {
            resourceType: "Library",
            // Missing id and content so libraryToSummary returns undefined.
            status: "active",
          } as Library,
        },
        { resource: libraryFixture },
      ],
    };
    const summaries = mapLibraryBundle(bundle);
    expect(summaries).toHaveLength(1);
    expect(summaries[0].id).toBe("lib-1");
  });
});

describe("parseTabularBody", () => {
  // CSV bodies are parsed using the dedicated CSV utility.
  it("parses a CSV body", () => {
    const body = "id,name\n1,Alice\n2,Bob";
    const { columns, rows } = parseTabularBody(body, "csv");
    expect(columns).toEqual(["id", "name"]);
    expect(rows).toEqual([
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ]);
  });

  // NDJSON bodies are parsed line by line.
  it("parses an NDJSON body", () => {
    const body = '{"id":1,"name":"Alice"}\n{"id":2,"name":"Bob"}';
    const { columns, rows } = parseTabularBody(body, "ndjson");
    expect(columns).toEqual(["id", "name"]);
    expect(rows).toEqual([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);
  });

  // JSON bodies parse into a single array.
  it("parses a JSON body", () => {
    const body = '[{"id":1},{"id":2}]';
    const { columns, rows } = parseTabularBody(body, "json");
    expect(columns).toEqual(["id"]);
    expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
  });

  // FHIR-format bodies are flattened from a Parameters resource.
  it("flattens a FHIR-format body", () => {
    const body = JSON.stringify({
      resourceType: "Parameters",
      parameter: [
        {
          name: "row",
          part: [{ name: "id", valueInteger: 1 }],
        },
      ],
    });
    const { columns, rows } = parseTabularBody(body, "fhir");
    expect(columns).toEqual(["id"]);
    expect(rows).toEqual([{ id: "1" }]);
  });

  // An empty body yields zero rows for any tabular format.
  it("returns no rows for an empty JSON body", () => {
    expect(parseTabularBody("", "json")).toEqual({
      columns: [],
      rows: [],
    });
  });
});

describe("readSqlQueryResponse", () => {
  // CSV branch uses the response text and returns a tabular result with
  // a Blob for verbatim download.
  it("returns a tabular result for csv", async () => {
    const response = new Response("id,name\n1,Alice", {
      status: 200,
      headers: { "Content-Type": "text/csv" },
    });
    const result = await readSqlQueryResponse(response, "csv");
    expect(result.kind).toBe("tabular");
    if (result.kind !== "tabular") return;
    expect(result.columns).toEqual(["id", "name"]);
    expect(result.rows).toEqual([{ id: "1", name: "Alice" }]);
    expect(result.rawBody).toBeInstanceOf(Blob);
  });

  // Parquet branch returns a binary blob and does not attempt to parse.
  it("returns a binary result for parquet", async () => {
    const data = new Uint8Array([1, 2, 3]);
    const response = new Response(data, {
      status: 200,
      headers: { "Content-Type": "application/vnd.apache.parquet" },
    });
    const result = await readSqlQueryResponse(response, "parquet");
    expect(result.kind).toBe("binary");
    if (result.kind !== "binary") return;
    expect(result.blob.size).toBe(3);
  });
});

describe("storedReferencesToViewRows", () => {
  // Each stored dependency reference becomes a view row carrying the canonical
  // URL verbatim, in reference order, with deterministic row ids.
  it("maps stored references to view rows by canonical url", () => {
    const rows = storedReferencesToViewRows([
      { label: "patients", reference: "https://example.org/Patients" },
      { label: "active", reference: "https://example.org/Library/Active" },
    ]);

    expect(rows).toEqual([
      {
        rowId: "stored-row-0",
        label: "patients",
        referenceUrl: "https://example.org/Patients",
      },
      {
        rowId: "stored-row-1",
        label: "active",
        referenceUrl: "https://example.org/Library/Active",
      },
    ]);
  });
});

describe("findSourceByUrl", () => {
  const sources: SourceOption[] = [
    { id: "vd1", name: "Patients", url: "https://example.org/Patients" },
    { id: "vd2", name: "Draft", url: undefined },
    { id: "lib1", name: "Active", url: "https://example.org/Library/Active" },
  ];

  // A stored canonical URL repopulates the picker by matching a known source.
  it("matches a known source by its canonical url", () => {
    expect(findSourceByUrl(sources, "https://example.org/Patients")).toEqual({
      id: "vd1",
      name: "Patients",
      url: "https://example.org/Patients",
    });
  });

  // An unmatched URL is surfaced verbatim (no source carries it), so the
  // picker can show a "source not found" note instead of a selection.
  it("returns undefined for a url no source carries", () => {
    expect(
      findSourceByUrl(sources, "https://example.org/Unknown"),
    ).toBeUndefined();
  });

  // A url-less source can never be matched.
  it("never matches a url-less source", () => {
    expect(findSourceByUrl(sources, "")).toBeUndefined();
  });
});

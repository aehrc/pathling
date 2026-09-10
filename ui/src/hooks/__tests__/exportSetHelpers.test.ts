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

/**
 * Tests for the export set helpers: capture, naming, collision detection and
 * request building.
 *
 * @author John Grimes
 */

import { describe, expect, it } from "vitest";

import {
  buildExportSetRequest,
  captureQueryEntry,
  captureViewEntry,
  findNameCollisions,
  parseIdList,
  removeEntry,
  renameEntry,
} from "../exportSetHelpers";

import type { SqlQueryLibrary } from "../../types/sqlQuery";
import type { ExportSetEntry } from "../exportSetHelpers";

const LIBRARY: SqlQueryLibrary = {
  resourceType: "Library",
  name: "bp_summary",
  status: "active",
  type: {
    coding: [
      {
        system: "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
        code: "sql-query",
      },
    ],
  },
  content: [{ contentType: "application/sql", data: "U0VMRUNUIDE=" }],
};

/**
 * Builds an entry with the given id and name, for the naming tests.
 *
 * @param id - The entry id.
 * @param name - The output name.
 * @returns The entry.
 */
function entry(id: string, name: string): ExportSetEntry {
  return {
    id,
    name,
    kind: "view",
    subject: { kind: "reference", reference: `ViewDefinition/${id}` },
  };
}

describe("captureViewEntry", () => {
  it("captures a stored view by typed reference, named from its id", () => {
    const captured = captureViewEntry(
      "1",
      { mode: "stored", viewDefinitionId: "demographics" },
      [],
    );

    expect(captured).toEqual({
      id: "1",
      kind: "view",
      name: "demographics",
      subject: {
        kind: "reference",
        reference: "ViewDefinition/demographics",
      },
    });
  });

  // The captured subject is the parsed resource, not a live reference to the
  // editor's text, so editing the form afterwards cannot change what exports.
  it("freezes an inline view at the moment it was captured", () => {
    const json = JSON.stringify({
      resourceType: "ViewDefinition",
      name: "encounters",
      resource: "Encounter",
      status: "active",
      select: [],
    });

    const captured = captureViewEntry(
      "1",
      { mode: "inline", viewDefinitionJson: json },
      [],
    );

    expect(captured.name).toBe("encounters");
    expect(captured.subject).toEqual({
      kind: "resource",
      resource: JSON.parse(json),
    });
  });

  it("falls back to a generic name for an unnamed inline view", () => {
    const json = JSON.stringify({
      resourceType: "ViewDefinition",
      resource: "Patient",
      status: "active",
      select: [],
    });

    expect(
      captureViewEntry("1", { mode: "inline", viewDefinitionJson: json }, [])
        .name,
    ).toBe("view");
  });

  // Capturing the same subject twice must not produce two entries the manifest
  // cannot tell apart, so the second is suffixed.
  it("suffixes a name that is already used in the set", () => {
    const existing = [entry("1", "demographics")];

    const captured = captureViewEntry(
      "2",
      { mode: "stored", viewDefinitionId: "demographics" },
      existing,
    );

    expect(captured.name).toBe("demographics_2");
  });

  it("rejects a request naming no view definition", () => {
    expect(() => captureViewEntry("1", { mode: "stored" }, [])).toThrow(
      "missing view definition",
    );
  });
});

describe("captureQueryEntry", () => {
  it("captures a stored query by typed reference, named from its id", () => {
    const captured = captureQueryEntry(
      "1",
      { mode: "stored", libraryId: "bp-summary" },
      [],
    );

    expect(captured).toMatchObject({
      id: "1",
      kind: "query",
      name: "bp-summary",
      subject: { kind: "reference", reference: "Library/bp-summary" },
    });
    expect(captured.parameters).toBeUndefined();
  });

  // Bindings are part of what the user chose to export, so they are frozen
  // alongside the subject rather than re-read at export time.
  it("freezes the runtime bindings alongside the subject", () => {
    const captured = captureQueryEntry(
      "1",
      {
        mode: "stored",
        libraryId: "bp-summary",
        bindings: { family: "Smith" },
        parameterTypes: { family: "string" },
      },
      [],
    );

    expect(captured.parameters).toEqual({
      resourceType: "Parameters",
      parameter: [{ name: "family", valueString: "Smith" }],
    });
  });

  it("names an inline query from the Library's own name", () => {
    const captured = captureQueryEntry(
      "1",
      { mode: "inline", library: LIBRARY },
      [],
    );

    expect(captured.name).toBe("bp_summary");
    expect(captured.subject).toEqual({ kind: "resource", resource: LIBRARY });
  });
});

describe("renameEntry and removeEntry", () => {
  it("renames only the named entry", () => {
    const entries = [entry("1", "a"), entry("2", "b")];

    expect(renameEntry(entries, "2", "renamed").map((e) => e.name)).toEqual([
      "a",
      "renamed",
    ]);
  });

  it("removes only the named entry", () => {
    const entries = [entry("1", "a"), entry("2", "b")];

    expect(removeEntry(entries, "1").map((e) => e.id)).toEqual(["2"]);
  });
});

describe("findNameCollisions", () => {
  it("finds nothing when every name is distinct", () => {
    expect(findNameCollisions([entry("1", "a"), entry("2", "b")])).toEqual([]);
  });

  it("reports a repeated name once", () => {
    expect(
      findNameCollisions([entry("1", "a"), entry("2", "a"), entry("3", "a")]),
    ).toEqual(["a"]);
  });

  // A name is what correlates a manifest output to the subject that produced
  // it, so a blank one is as unusable as a repeated one.
  it("treats a blank name as a collision", () => {
    expect(findNameCollisions([entry("1", "   ")])).toEqual([""]);
  });

  it("ignores surrounding whitespace when comparing", () => {
    expect(findNameCollisions([entry("1", "a"), entry("2", " a ")])).toEqual([
      "a",
    ]);
  });

  it("finds nothing in an empty set", () => {
    expect(findNameCollisions([])).toEqual([]);
  });
});

describe("buildExportSetRequest", () => {
  it("builds one subject per entry, carrying its name and bindings", () => {
    const entries: ExportSetEntry[] = [
      entry("1", "demographics"),
      {
        id: "2",
        name: " smiths ",
        kind: "query",
        subject: { kind: "reference", reference: "Library/by-family" },
        parameters: {
          resourceType: "Parameters",
          parameter: [{ name: "family", valueString: "Smith" }],
        },
      },
    ];

    const request = buildExportSetRequest(entries, "csv");

    expect(request.format).toBe("csv");
    expect(request.header).toBe(true);
    expect(request.subjects).toEqual([
      {
        name: "demographics",
        subject: { kind: "reference", reference: "ViewDefinition/1" },
        parameters: undefined,
      },
      {
        name: "smiths",
        subject: { kind: "reference", reference: "Library/by-family" },
        parameters: entries[1].parameters,
      },
    ]);
  });

  it("applies the job-wide filters", () => {
    const request = buildExportSetRequest([entry("1", "a")], "ndjson", {
      patientIds: ["p1", "p2"],
      groupIds: ["g1"],
      since: " 2026-01-01T00:00:00Z ",
    });

    expect(request.patientIds).toEqual(["p1", "p2"]);
    expect(request.groupIds).toEqual(["g1"]);
    expect(request.since).toBe("2026-01-01T00:00:00Z");
  });

  // An empty filter is omitted rather than sent as an empty list, so the
  // request says nothing about a filter the user did not set.
  it("omits filters that were left empty", () => {
    const request = buildExportSetRequest([entry("1", "a")], "ndjson", {
      patientIds: [],
      since: "  ",
    });

    expect(request.patientIds).toBeUndefined();
    expect(request.groupIds).toBeUndefined();
    expect(request.since).toBeUndefined();
  });

  it("builds a single-entry set", () => {
    expect(
      buildExportSetRequest([entry("1", "only")], "parquet").subjects,
    ).toHaveLength(1);
  });

  it("builds an empty request for an empty set", () => {
    expect(buildExportSetRequest([], "ndjson").subjects).toEqual([]);
  });
});

describe("parseIdList", () => {
  it("splits on commas and whitespace, dropping blanks", () => {
    expect(parseIdList("p1, p2  p3,,")).toEqual(["p1", "p2", "p3"]);
  });

  it("returns nothing for an empty field", () => {
    expect(parseIdList("   ")).toEqual([]);
  });
});

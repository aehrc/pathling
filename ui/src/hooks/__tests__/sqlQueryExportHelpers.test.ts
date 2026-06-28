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
  buildSqlQueryExportKickOffBody,
  parseSqlQueryExportManifest,
} from "../sqlQueryExportHelpers";

import type { SqlQueryLibrary } from "../../types/sqlQuery";
import type { Parameters } from "fhir/r4";

describe("buildSqlQueryExportKickOffBody", () => {
  it("builds a query.queryReference part for a stored query", () => {
    const body = buildSqlQueryExportKickOffBody({
      mode: "stored",
      libraryId: "patient-bp-query",
      format: "ndjson",
    });

    expect(body.resourceType).toBe("Parameters");
    const query = body.parameter?.find((p) => p.name === "query");
    expect(query?.part?.[0]?.name).toBe("queryReference");
    expect(query?.part?.[0]?.valueReference?.reference).toBe(
      "Library/patient-bp-query",
    );
    expect(body.parameter?.find((p) => p.name === "_format")?.valueCode).toBe(
      "ndjson",
    );
  });

  it("builds a query.queryResource part for an inline query", () => {
    const library: SqlQueryLibrary = {
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
      content: [{ contentType: "application/sql", data: "U0VMRUNUIDE=" }],
    };

    const body = buildSqlQueryExportKickOffBody({
      mode: "inline",
      library,
      format: "csv",
      header: false,
    });

    const query = body.parameter?.find((p) => p.name === "query");
    expect(query?.part?.[0]?.name).toBe("queryResource");
    expect(query?.part?.[0]?.resource?.resourceType).toBe("Library");
    expect(body.parameter?.find((p) => p.name === "_format")?.valueCode).toBe(
      "csv",
    );
    expect(body.parameter?.find((p) => p.name === "header")?.valueBoolean).toBe(
      false,
    );
  });

  it("omits the header parameter when not supplied", () => {
    const body = buildSqlQueryExportKickOffBody({
      mode: "stored",
      libraryId: "q",
      format: "parquet",
    });
    expect(body.parameter?.find((p) => p.name === "header")).toBeUndefined();
  });
});

describe("parseSqlQueryExportManifest", () => {
  it("returns an empty array for an absent manifest or parameters", () => {
    expect(parseSqlQueryExportManifest(null)).toEqual([]);
    expect(parseSqlQueryExportManifest(undefined)).toEqual([]);
    expect(parseSqlQueryExportManifest({ resourceType: "Parameters" })).toEqual(
      [],
    );
  });

  it("extracts one entry per location, sharing the output name", () => {
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        { name: "status", valueCode: "completed" },
        {
          name: "output",
          part: [
            { name: "name", valueString: "people" },
            {
              name: "location",
              valueUri: "https://x/fhir/$result?job=j&file=people.00000.ndjson",
            },
            {
              name: "location",
              valueUri: "https://x/fhir/$result?job=j&file=people.00001.ndjson",
            },
          ],
        },
        {
          name: "output",
          part: [
            { name: "name", valueString: "observations" },
            {
              name: "location",
              valueUri:
                "https://x/fhir/$result?job=j&file=observations.00000.ndjson",
            },
          ],
        },
      ],
    };

    const outputs = parseSqlQueryExportManifest(manifest);
    expect(outputs).toHaveLength(3);
    expect(outputs[0]).toEqual({
      name: "people",
      url: "https://x/fhir/$result?job=j&file=people.00000.ndjson",
    });
    expect(outputs[1].name).toBe("people");
    expect(outputs[2].name).toBe("observations");
  });
});

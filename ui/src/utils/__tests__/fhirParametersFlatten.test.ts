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

import { flattenFhirParameters } from "../fhirParametersFlatten";

import type { Parameters } from "fhir/r4";

describe("flattenFhirParameters", () => {
  // A single row with two typed parts produces one record and the columns
  // appear in first-seen order.
  it("flattens a typed row into a record", () => {
    const params: Parameters = {
      resourceType: "Parameters",
      parameter: [
        {
          name: "row",
          part: [
            { name: "patient_id", valueString: "pat-1" },
            { name: "age", valueInteger: 42 },
          ],
        },
      ],
    };
    const result = flattenFhirParameters(params);
    expect(result.columns).toEqual(["patient_id", "age"]);
    expect(result.rows).toEqual([{ patient_id: "pat-1", age: "42" }]);
  });

  // Missing parts (representing SQL NULL) appear as empty strings in the
  // flattened row but the union of seen columns is preserved across rows.
  it("treats missing parts as empty cells and unions columns across rows", () => {
    const params: Parameters = {
      resourceType: "Parameters",
      parameter: [
        {
          name: "row",
          part: [
            { name: "id", valueInteger: 1 },
            { name: "name", valueString: "Alice" },
          ],
        },
        {
          name: "row",
          part: [{ name: "id", valueInteger: 2 }],
        },
        {
          name: "row",
          part: [
            { name: "id", valueInteger: 3 },
            { name: "name", valueString: "Charlie" },
            { name: "active", valueBoolean: true },
          ],
        },
      ],
    };
    const result = flattenFhirParameters(params);
    expect(result.columns).toEqual(["id", "name", "active"]);
    expect(result.rows).toEqual([
      { id: "1", name: "Alice", active: "" },
      { id: "2", name: "", active: "" },
      { id: "3", name: "Charlie", active: "true" },
    ]);
  });

  // A response with no `row` parameters yields an empty result (zero rows
  // and zero columns).
  it("returns empty columns and rows when there are no row parameters", () => {
    const params: Parameters = {
      resourceType: "Parameters",
      parameter: [],
    };
    const result = flattenFhirParameters(params);
    expect(result.columns).toEqual([]);
    expect(result.rows).toEqual([]);
  });

  // Decimal values are stringified using `String()`, preserving the JSON
  // numeric form returned by the server.
  it("renders decimal values as strings", () => {
    const params: Parameters = {
      resourceType: "Parameters",
      parameter: [
        {
          name: "row",
          part: [{ name: "amount", valueDecimal: 1.5 }],
        },
      ],
    };
    const result = flattenFhirParameters(params);
    expect(result.rows).toEqual([{ amount: "1.5" }]);
  });

  // Date and dateTime values pass through as-is from their string `value[x]`.
  it("preserves date and dateTime string values", () => {
    const params: Parameters = {
      resourceType: "Parameters",
      parameter: [
        {
          name: "row",
          part: [
            { name: "dob", valueDate: "1990-05-15" },
            { name: "ts", valueDateTime: "2025-10-01T12:00:00Z" },
          ],
        },
      ],
    };
    const result = flattenFhirParameters(params);
    expect(result.rows).toEqual([
      { dob: "1990-05-15", ts: "2025-10-01T12:00:00Z" },
    ]);
  });
});

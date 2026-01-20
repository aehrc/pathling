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
 * Unit tests for bulk export type utilities.
 *
 * These tests verify the extraction of output file entries from FHIR
 * Parameters resources returned by bulk export operations.
 *
 * @author John Grimes
 */

import { describe, expect, it } from "vitest";

import { getExportOutputFiles } from "../export";

import type { Parameters } from "fhir/r4";

describe("getExportOutputFiles", () => {
  it("extracts output files with type, url, and count", () => {
    // A manifest with two output files, one with count and one without.
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        {
          name: "output",
          part: [
            { name: "type", valueCode: "Patient" },
            { name: "url", valueUri: "http://example.com/patient.ndjson" },
            { name: "count", valueInteger: 100 },
          ],
        },
        {
          name: "output",
          part: [
            { name: "type", valueCode: "Observation" },
            { name: "url", valueUri: "http://example.com/observation.ndjson" },
            { name: "count", valueInteger: 500 },
          ],
        },
      ],
    };

    const outputs = getExportOutputFiles(manifest);

    expect(outputs).toHaveLength(2);
    expect(outputs[0]).toEqual({
      type: "Patient",
      url: "http://example.com/patient.ndjson",
      count: 100,
    });
    expect(outputs[1]).toEqual({
      type: "Observation",
      url: "http://example.com/observation.ndjson",
      count: 500,
    });
  });

  it("returns empty array when manifest has no parameter property", () => {
    // A manifest without the parameter array.
    const manifest: Parameters = { resourceType: "Parameters" };

    expect(getExportOutputFiles(manifest)).toEqual([]);
  });

  it("returns empty array when manifest has empty parameter array", () => {
    // A manifest with an empty parameter array.
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [],
    };

    expect(getExportOutputFiles(manifest)).toEqual([]);
  });

  it("handles output entries without count", () => {
    // Some export implementations may not include a count.
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        {
          name: "output",
          part: [
            { name: "type", valueCode: "Condition" },
            { name: "url", valueUri: "http://example.com/condition.ndjson" },
          ],
        },
      ],
    };

    const outputs = getExportOutputFiles(manifest);

    expect(outputs).toHaveLength(1);
    expect(outputs[0]).toEqual({
      type: "Condition",
      url: "http://example.com/condition.ndjson",
      count: undefined,
    });
  });

  it("filters out non-output parameters", () => {
    // The manifest includes parameters other than output entries.
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        { name: "transactionTime", valueInstant: "2024-01-01T00:00:00Z" },
        {
          name: "output",
          part: [
            { name: "type", valueCode: "Patient" },
            { name: "url", valueUri: "http://example.com/patient.ndjson" },
          ],
        },
        { name: "request", valueString: "https://example.com/$export" },
        { name: "requiresAccessToken", valueBoolean: false },
      ],
    };

    const outputs = getExportOutputFiles(manifest);

    expect(outputs).toHaveLength(1);
    expect(outputs[0].type).toBe("Patient");
  });

  it("filters out output parameters without part array", () => {
    // Malformed output entries that lack the part array should be skipped.
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        {
          name: "output",
          // Missing part array.
        },
        {
          name: "output",
          part: [
            { name: "type", valueCode: "Medication" },
            { name: "url", valueUri: "http://example.com/medication.ndjson" },
          ],
        },
      ],
    };

    const outputs = getExportOutputFiles(manifest);

    expect(outputs).toHaveLength(1);
    expect(outputs[0].type).toBe("Medication");
  });

  it("handles output entries with missing type or url parts", () => {
    // Parts may be missing; function should return empty strings for missing values.
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        {
          name: "output",
          part: [
            { name: "url", valueUri: "http://example.com/unknown.ndjson" },
            // Missing type part.
          ],
        },
        {
          name: "output",
          part: [
            { name: "type", valueCode: "Encounter" },
            // Missing url part.
          ],
        },
      ],
    };

    const outputs = getExportOutputFiles(manifest);

    expect(outputs).toHaveLength(2);
    expect(outputs[0]).toEqual({
      type: "",
      url: "http://example.com/unknown.ndjson",
      count: undefined,
    });
    expect(outputs[1]).toEqual({
      type: "Encounter",
      url: "",
      count: undefined,
    });
  });

  it("handles multiple output entries with varying formats", () => {
    // A realistic manifest with multiple resource types.
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        { name: "transactionTime", valueInstant: "2024-06-15T10:30:00Z" },
        {
          name: "output",
          part: [
            { name: "type", valueCode: "Patient" },
            { name: "url", valueUri: "http://example.com/Patient-1.ndjson" },
            { name: "count", valueInteger: 1000 },
          ],
        },
        {
          name: "output",
          part: [
            { name: "type", valueCode: "Patient" },
            { name: "url", valueUri: "http://example.com/Patient-2.ndjson" },
            { name: "count", valueInteger: 1000 },
          ],
        },
        {
          name: "output",
          part: [
            { name: "type", valueCode: "Observation" },
            {
              name: "url",
              valueUri: "http://example.com/Observation-1.ndjson",
            },
            { name: "count", valueInteger: 50000 },
          ],
        },
        { name: "request", valueString: "/$export?_type=Patient,Observation" },
      ],
    };

    const outputs = getExportOutputFiles(manifest);

    expect(outputs).toHaveLength(3);
    expect(outputs.map((o) => o.type)).toEqual([
      "Patient",
      "Patient",
      "Observation",
    ]);
    expect(outputs.map((o) => o.count)).toEqual([1000, 1000, 50000]);
  });
});

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
 * Unit tests for view export type utilities.
 *
 * @author John Grimes
 */

import { describe, expect, it } from "vitest";

import { getViewExportOutputFiles } from "../viewExport";

import type { Parameters } from "fhir/r4";

describe("getViewExportOutputFiles", () => {
  it("extracts one file per output from a spec-shaped manifest", () => {
    // A spec-shaped manifest where each output has a name and a single location part.
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        { name: "exportId", valueString: "job-abc" },
        { name: "status", valueCode: "completed" },
        { name: "_format", valueCode: "ndjson" },
        {
          name: "output",
          part: [
            { name: "name", valueString: "patients" },
            {
              name: "location",
              valueUri:
                "http://example.org/$result?job=abc&file=patients.ndjson",
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
                "http://example.org/$result?job=abc&file=observations.ndjson",
            },
          ],
        },
      ],
    };

    const outputs = getViewExportOutputFiles(manifest);

    expect(outputs).toHaveLength(2);
    expect(outputs[0]).toEqual({
      name: "patients",
      url: "http://example.org/$result?job=abc&file=patients.ndjson",
    });
    expect(outputs[1]).toEqual({
      name: "observations",
      url: "http://example.org/$result?job=abc&file=observations.ndjson",
    });
  });

  it("emits one entry per location when a view produced multiple files", () => {
    // A single output with several location parts (e.g. a Spark-partitioned view).
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        {
          name: "output",
          part: [
            { name: "name", valueString: "observations" },
            {
              name: "location",
              valueUri:
                "http://example.org/$result?job=abc&file=observations-0.ndjson",
            },
            {
              name: "location",
              valueUri:
                "http://example.org/$result?job=abc&file=observations-1.ndjson",
            },
          ],
        },
      ],
    };

    const outputs = getViewExportOutputFiles(manifest);

    expect(outputs).toHaveLength(2);
    expect(outputs[0]).toEqual({
      name: "observations",
      url: "http://example.org/$result?job=abc&file=observations-0.ndjson",
    });
    expect(outputs[1]).toEqual({
      name: "observations",
      url: "http://example.org/$result?job=abc&file=observations-1.ndjson",
    });
  });

  it("returns empty array when no parameter property", () => {
    // A manifest with no parameters.
    const manifest: Parameters = { resourceType: "Parameters" };
    expect(getViewExportOutputFiles(manifest)).toEqual([]);
  });

  it("returns empty array when no output parameters", () => {
    // A manifest with parameters but no output entries.
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [{ name: "status", valueCode: "completed" }],
    };
    expect(getViewExportOutputFiles(manifest)).toEqual([]);
  });
});

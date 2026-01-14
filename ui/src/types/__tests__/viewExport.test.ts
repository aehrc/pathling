/**
 * Unit tests for view export type utilities.
 *
 * @author John Grimes
 */

import { describe, expect, it } from "vitest";

import { getViewExportOutputFiles } from "../viewExport";

import type { Parameters } from "fhir/r4";

describe("getViewExportOutputFiles", () => {
  it("extracts output files from Parameters resource", () => {
    // A manifest with two output files.
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        { name: "transactionTime", valueInstant: "2025-01-01T00:00:00Z" },
        {
          name: "output",
          part: [
            { name: "name", valueString: "patients" },
            {
              name: "url",
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
              name: "url",
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

  it("returns empty array when no parameter property", () => {
    // A manifest with no parameters.
    const manifest: Parameters = { resourceType: "Parameters" };
    expect(getViewExportOutputFiles(manifest)).toEqual([]);
  });

  it("returns empty array when no output parameters", () => {
    // A manifest with parameters but no output entries.
    const manifest: Parameters = {
      resourceType: "Parameters",
      parameter: [
        { name: "transactionTime", valueInstant: "2025-01-01T00:00:00Z" },
      ],
    };
    expect(getViewExportOutputFiles(manifest)).toEqual([]);
  });
});

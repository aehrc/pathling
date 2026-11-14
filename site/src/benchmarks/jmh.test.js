/**
 * Tests for reading JMH JSON results.
 *
 * @author John Grimes
 */

import { describe, expect, test } from "bun:test";
import {
  iterationScores,
  paramsLabel,
  resultKey,
  shortBenchmarkName,
  summariseResults,
} from "./jmh.js";

/**
 * Builds a minimal JMH result in the shape written by `-rf json`.
 *
 * @param {object} overrides - Fields to replace on the result.
 * @param {object} metric - Fields to replace on the primary metric.
 * @returns {object} A JMH result.
 */
function jmhResult(overrides = {}, metric = {}) {
  return {
    benchmark: "au.csiro.pathling.benchmark.PathlingBenchmark.conditionFlat",
    mode: "avgt",
    params: { sourceType: "delta" },
    primaryMetric: {
      score: 12.5,
      scoreError: 0.5,
      scoreUnit: "ms/op",
      scorePercentiles: { "50.0": 12.4 },
      rawData: [
        [12, 13],
        [12.5, 12.5],
      ],
      ...metric,
    },
    ...overrides,
  };
}

describe("naming", () => {
  test("shortBenchmarkName drops the package", () => {
    expect(
      shortBenchmarkName(
        "au.csiro.pathling.benchmark.PathlingBenchmark.conditionFlat",
      ),
    ).toBe("PathlingBenchmark.conditionFlat");
  });

  test("shortBenchmarkName leaves an unqualified name unchanged", () => {
    expect(shortBenchmarkName("conditionFlat")).toBe("conditionFlat");
  });

  test("paramsLabel sorts parameters by name", () => {
    expect(paramsLabel({ view: "PatientFlat", sourceType: "delta" })).toBe(
      "sourceType=delta, view=PatientFlat",
    );
  });

  test("paramsLabel is empty when there are no parameters", () => {
    // JMH omits the params field entirely for unparameterised benchmarks.
    expect(paramsLabel(undefined)).toBe("");
    expect(paramsLabel({})).toBe("");
  });

  test("resultKey distinguishes mode and parameters", () => {
    const a = resultKey(jmhResult());
    const b = resultKey(jmhResult({ params: { sourceType: "ndjson" } }));
    const c = resultKey(jmhResult({ mode: "thrpt" }));
    expect(new Set([a, b, c]).size).toBe(3);
  });
});

describe("iterationScores", () => {
  test("flattens raw data across forks", () => {
    // Two forks with two measurement iterations each.
    expect(iterationScores(jmhResult())).toEqual([12, 13, 12.5, 12.5]);
  });

  test("uses the weighted mean of each iteration's histogram in sample mode", () => {
    // Sample mode records a histogram of [value, count] pairs per iteration.
    const result = jmhResult(
      { mode: "sample" },
      {
        rawData: undefined,
        rawDataHistogram: [
          [
            [
              [10, 1],
              [20, 3],
            ],
            [[30, 2]],
          ],
          [[[40, 1]]],
        ],
      },
    );
    expect(iterationScores(result)).toEqual([17.5, 30, 40]);
  });

  test("returns an empty list when no raw data is present", () => {
    const result = jmhResult(
      {},
      { rawData: undefined, rawDataHistogram: undefined },
    );
    expect(iterationScores(result)).toEqual([]);
  });
});

describe("summariseResults", () => {
  test("keeps only the fields needed for the timeline", () => {
    expect(summariseResults([jmhResult()])).toEqual([
      {
        benchmark: "PathlingBenchmark.conditionFlat",
        mode: "avgt",
        params: "sourceType=delta",
        score: 12.5,
        scoreError: 0.5,
        scoreUnit: "ms/op",
      },
    ]);
  });

  test("records a missing error as null", () => {
    // JMH writes "NaN" as the error when there are too few iterations.
    const [summary] = summariseResults([jmhResult({}, { scoreError: "NaN" })]);
    expect(summary.scoreError).toBeNull();
  });
});

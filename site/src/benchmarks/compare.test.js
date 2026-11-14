/**
 * Tests for comparing the results of two benchmark runs.
 *
 * @author John Grimes
 */

import { describe, expect, test } from "bun:test";
import { compareRuns } from "./compare.js";

/**
 * Builds a JMH result with the given per-iteration scores.
 *
 * @param {string} name - The unqualified benchmark method name.
 * @param {number[]} scores - Per-iteration scores, all in a single fork.
 * @param {object} overrides - Fields to replace on the result.
 * @returns {object} A JMH result.
 */
function result(name, scores, overrides = {}) {
  const score = scores.reduce((sum, value) => sum + value, 0) / scores.length;
  return {
    benchmark: `au.csiro.pathling.benchmark.PathlingBenchmark.${name}`,
    mode: "avgt",
    params: { sourceType: "delta" },
    primaryMetric: {
      score,
      scoreError: 1,
      scoreUnit: "ms/op",
      rawData: [scores],
    },
    ...overrides,
  };
}

const FAST = [10, 11, 12, 10, 11, 12];
const SLOW = [20, 21, 22, 20, 21, 22];
const NOISY = [10, 30, 12, 28, 11, 29];

describe("compareRuns", () => {
  test("a significantly lower time is an improvement", () => {
    const [row] = compareRuns([result("a", SLOW)], [result("a", FAST)]);
    expect(row.verdict).toBe("improvement");
    expect(row.changePercent).toBeCloseTo(-47.619, 2);
    expect(row.p).toBeLessThan(0.05);
  });

  test("a significantly higher time is a regression", () => {
    const [row] = compareRuns([result("a", FAST)], [result("a", SLOW)]);
    expect(row.verdict).toBe("regression");
  });

  test("higher throughput is an improvement", () => {
    // In throughput mode a higher score is better.
    const [row] = compareRuns(
      [result("a", FAST, { mode: "thrpt" })],
      [result("a", SLOW, { mode: "thrpt" })],
    );
    expect(row.verdict).toBe("improvement");
  });

  test("a difference within the noise is not significant", () => {
    const [row] = compareRuns([result("a", FAST)], [result("a", NOISY)]);
    expect(row.p).toBeGreaterThanOrEqual(0.05);
    expect(row.verdict).toBe("no-change");
  });

  test("the significance level can be changed", () => {
    // p is about 0.09 for these samples, so it is significant at 0.1 only.
    const base = result("a", [10, 11, 12, 13, 14, 15]);
    const target = result("a", [12, 13, 14, 15, 16, 17]);
    expect(compareRuns([base], [target], 0.05)[0].verdict).toBe("no-change");
    expect(compareRuns([base], [target], 0.1)[0].verdict).toBe("regression");
  });

  test("results without enough iterations are untestable", () => {
    const [row] = compareRuns([result("a", [10])], [result("a", [20])]);
    expect(row.verdict).toBe("untestable");
    expect(row.p).toBeNull();
    // The change is still reported.
    expect(row.changePercent).toBeCloseTo(100, 6);
  });

  test("results matched on parameters, not position", () => {
    const base = [
      result("a", FAST, { params: { sourceType: "delta" } }),
      result("a", SLOW, { params: { sourceType: "ndjson" } }),
    ];
    const target = [...base].reverse();
    const rows = compareRuns(base, target);
    expect(rows.map((row) => row.verdict)).toEqual(["no-change", "no-change"]);
  });

  test("results present in only one run are reported", () => {
    const rows = compareRuns([result("a", FAST)], [result("b", FAST)]);
    expect(
      rows.map((row) => [row.benchmark, row.verdict, row.base, row.target]),
    ).toEqual([
      [
        "PathlingBenchmark.a",
        "base-only",
        { score: 11, scoreError: 1, scoreUnit: "ms/op" },
        null,
      ],
      [
        "PathlingBenchmark.b",
        "target-only",
        null,
        { score: 11, scoreError: 1, scoreUnit: "ms/op" },
      ],
    ]);
  });

  test("rows are ordered by benchmark then parameters", () => {
    const rows = compareRuns(
      [
        result("b", FAST),
        result("a", FAST, { params: { sourceType: "ndjson" } }),
        result("a", FAST),
      ],
      [],
    );
    expect(rows.map((row) => `${row.benchmark} ${row.params}`)).toEqual([
      "PathlingBenchmark.a sourceType=delta",
      "PathlingBenchmark.a sourceType=ndjson",
      "PathlingBenchmark.b sourceType=delta",
    ]);
  });
});

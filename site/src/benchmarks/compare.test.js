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

/**
 * Builds a run whose reference results all have the given score, so that a
 * reference score of 1 leaves the relative scores equal to the raw scores.
 *
 * @param {object[]} results - The JMH results of the run.
 * @param {number} [referenceScore=1] - The score of every reference result.
 * @returns {{results: object[], referenceResults: object[]}} The run.
 */
function run(results, referenceScore = 1) {
  return {
    results,
    referenceResults: results.map((entry) => ({
      ...entry,
      primaryMetric: {
        ...entry.primaryMetric,
        score: referenceScore,
        scoreError: 0,
      },
    })),
  };
}

const FAST = [10, 11, 12, 10, 11, 12];
const SLOW = [20, 21, 22, 20, 21, 22];
const NOISY = [10, 30, 12, 28, 11, 29];

describe("compareRuns", () => {
  test("a significantly lower relative time is an improvement", () => {
    const [row] = compareRuns(
      run([result("a", SLOW)]),
      run([result("a", FAST)]),
    );
    expect(row.verdict).toBe("improvement");
    expect(row.changePercent).toBeCloseTo(-47.619, 2);
    expect(row.p).toBeLessThan(0.05);
  });

  test("a significantly higher relative time is a regression", () => {
    const [row] = compareRuns(
      run([result("a", FAST)]),
      run([result("a", SLOW)]),
    );
    expect(row.verdict).toBe("regression");
  });

  test("a runner that is slower for both the code and the reference is not a regression", () => {
    // The target run took twice as long, but so did the reference on its
    // runner, so the relative scores are the same.
    const [row] = compareRuns(
      run([result("a", FAST)], 10),
      run(
        [
          result(
            "a",
            FAST.map((score) => score * 2),
          ),
        ],
        20,
      ),
    );
    expect(row.changePercent).toBeCloseTo(0, 10);
    expect(row.verdict).toBe("no-change");
  });

  test("a faster reference reveals a regression hidden by a faster runner", () => {
    // The raw scores are identical, but the reference ran twice as fast in the
    // target run, so the code under test is relatively twice as slow.
    const [row] = compareRuns(
      run([result("a", FAST)], 10),
      run([result("a", FAST)], 5),
    );
    expect(row.changePercent).toBeCloseTo(100, 10);
    expect(row.verdict).toBe("regression");
  });

  test("rows report relative scores", () => {
    const [row] = compareRuns(
      run([result("a", FAST)], 10),
      run([result("a", SLOW)], 10),
    );
    expect(row.base).toEqual({
      score: expect.closeTo(1.1, 10),
      scoreError: expect.closeTo(0.1, 10),
    });
    expect(row.target.score).toBeCloseTo(2.1, 10);
  });

  test("higher throughput is an improvement", () => {
    // In throughput mode a higher score is better.
    const [row] = compareRuns(
      run([result("a", FAST, { mode: "thrpt" })]),
      run([result("a", SLOW, { mode: "thrpt" })]),
    );
    expect(row.verdict).toBe("improvement");
  });

  test("a difference within the noise is not significant", () => {
    const [row] = compareRuns(
      run([result("a", FAST)]),
      run([result("a", NOISY)]),
    );
    expect(row.p).toBeGreaterThanOrEqual(0.05);
    expect(row.verdict).toBe("no-change");
  });

  test("the significance level can be changed", () => {
    // p is about 0.09 for these samples, so it is significant at 0.1 only.
    const base = run([result("a", [10, 11, 12, 13, 14, 15])]);
    const target = run([result("a", [12, 13, 14, 15, 16, 17])]);
    expect(compareRuns(base, target, 0.05)[0].verdict).toBe("no-change");
    expect(compareRuns(base, target, 0.1)[0].verdict).toBe("regression");
  });

  test("results without enough iterations are untestable", () => {
    const [row] = compareRuns(
      run([result("a", [10])]),
      run([result("a", [20])]),
    );
    expect(row.verdict).toBe("untestable");
    expect(row.p).toBeNull();
    // The change is still reported.
    expect(row.changePercent).toBeCloseTo(100, 6);
  });

  test("a result without a reference result cannot be compared", () => {
    const base = run([result("a", FAST)]);
    const target = { results: [result("a", SLOW)], referenceResults: [] };
    const [row] = compareRuns(base, target);
    expect(row.verdict).toBe("no-reference");
    expect(row.target).toBeNull();
    expect(row.changePercent).toBeNull();
    expect(row.p).toBeNull();
  });

  test("results matched on parameters, not position", () => {
    const results = [
      result("a", FAST, { params: { sourceType: "delta" } }),
      result("a", SLOW, { params: { sourceType: "ndjson" } }),
    ];
    const rows = compareRuns(run(results), run([...results].reverse()));
    expect(rows.map((row) => row.verdict)).toEqual(["no-change", "no-change"]);
  });

  test("results present in only one run are reported", () => {
    const rows = compareRuns(
      run([result("a", FAST)]),
      run([result("b", FAST)]),
    );
    expect(
      rows.map((row) => [row.benchmark, row.verdict, row.base, row.target]),
    ).toEqual([
      ["PathlingBenchmark.a", "base-only", { score: 11, scoreError: 1 }, null],
      [
        "PathlingBenchmark.b",
        "target-only",
        null,
        { score: 11, scoreError: 1 },
      ],
    ]);
  });

  test("rows are ordered by benchmark then parameters", () => {
    const rows = compareRuns(
      run([
        result("b", FAST),
        result("a", FAST, { params: { sourceType: "ndjson" } }),
        result("a", FAST),
      ]),
      run([]),
    );
    expect(rows.map((row) => `${row.benchmark} ${row.params}`)).toEqual([
      "PathlingBenchmark.a sourceType=delta",
      "PathlingBenchmark.a sourceType=ndjson",
      "PathlingBenchmark.b sourceType=delta",
    ]);
  });
});

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
 * Integration test for publishing a benchmark run into the data directory
 * served alongside the site.
 *
 * @author John Grimes
 */

import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { publishRun } from "./publishBenchmark.js";

const RESULTS = [
  {
    benchmark: "au.csiro.pathling.benchmark.PathlingBenchmark.conditionFlat",
    mode: "sample",
    params: { sourceType: "delta" },
    primaryMetric: {
      score: 12.5,
      scoreError: 0.5,
      scoreUnit: "ms/op",
      rawDataHistogram: [[[[12.5, 3]]]],
    },
  },
];

// The same benchmark run against the reference release.
const REFERENCE_RESULTS = [
  {
    ...RESULTS[0],
    primaryMetric: { ...RESULTS[0].primaryMetric, score: 10, scoreError: 0.4 },
  },
];

/**
 * Builds the options for publishing the test run.
 *
 * @param {object} overrides - Options to replace.
 * @returns {object} The options for {@link publishRun}.
 */
function options(overrides = {}) {
  return {
    resultsPath: join(dir, "results.json"),
    referenceResultsPath: join(dir, "reference.json"),
    reference: "9.1.0",
    dataDir: join(dir, "data"),
    runId: "123",
    commit: "abc",
    version: "9.9.0",
    date: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

let dir;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "benchmarks-"));
  writeFileSync(join(dir, "results.json"), JSON.stringify(RESULTS));
  writeFileSync(join(dir, "reference.json"), JSON.stringify(REFERENCE_RESULTS));
});

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

/**
 * Reads a JSON file.
 *
 * @param {string} path - The file path.
 * @returns {any} The parsed content.
 */
function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

test("publishing copies the results and records the run in the index", () => {
  const dataDir = join(dir, "data");

  // Publish twice to check that a run is recorded only once.
  for (let i = 0; i < 2; i++) {
    publishRun(options());
  }

  expect(readJson(join(dataDir, "runs", "123.json"))).toEqual(RESULTS);
  expect(readJson(join(dataDir, "runs", "123-reference.json"))).toEqual(
    REFERENCE_RESULTS,
  );
  expect(readJson(join(dataDir, "index.json"))).toEqual({
    runs: [
      {
        id: "123",
        date: "2026-01-01T00:00:00Z",
        commit: "abc",
        version: "9.9.0",
        reference: "9.1.0",
        results: [
          {
            benchmark: "PathlingBenchmark.conditionFlat",
            mode: "sample",
            params: "sourceType=delta",
            score: 12.5,
            scoreError: 0.5,
            scoreUnit: "ms/op",
            relative: {
              score: expect.closeTo(1.25, 10),
              scoreError: expect.closeTo(1.25 * 0.04 * Math.SQRT2, 10),
            },
          },
        ],
      },
    ],
  });
});

test("publishing rejects results that are not a JMH result array", () => {
  writeFileSync(join(dir, "results.json"), JSON.stringify({ not: "jmh" }));
  expect(() => publishRun(options())).toThrow("JMH");
});

test("publishing rejects reference results that are not a JMH result array", () => {
  writeFileSync(join(dir, "reference.json"), JSON.stringify([{ not: "jmh" }]));
  expect(() => publishRun(options())).toThrow("JMH");
});

test("publishing rejects a date that is not an ISO 8601 UTC time", () => {
  // The index is sorted by comparing dates as strings, which needs one format.
  expect(() =>
    publishRun(options({ date: "2026-01-01T10:00:00+10:00" })),
  ).toThrow("date");
});

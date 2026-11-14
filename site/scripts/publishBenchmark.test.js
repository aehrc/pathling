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

let dir;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "benchmarks-"));
  writeFileSync(join(dir, "results.json"), JSON.stringify(RESULTS));
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
    publishRun({
      resultsPath: join(dir, "results.json"),
      dataDir,
      runId: "123",
      commit: "abc",
      date: "2026-01-01T00:00:00Z",
    });
  }

  expect(readJson(join(dataDir, "runs", "123.json"))).toEqual(RESULTS);
  expect(readJson(join(dataDir, "index.json"))).toEqual({
    runs: [
      {
        id: "123",
        date: "2026-01-01T00:00:00Z",
        commit: "abc",
        results: [
          {
            benchmark: "PathlingBenchmark.conditionFlat",
            mode: "sample",
            params: "sourceType=delta",
            score: 12.5,
            scoreError: 0.5,
            scoreUnit: "ms/op",
          },
        ],
      },
    ],
  });
});

test("publishing rejects results that are not a JMH result array", () => {
  writeFileSync(join(dir, "results.json"), JSON.stringify({ not: "jmh" }));
  expect(() =>
    publishRun({
      resultsPath: join(dir, "results.json"),
      dataDir: join(dir, "data"),
      runId: "123",
      commit: "abc",
      date: "2026-01-01T00:00:00Z",
    }),
  ).toThrow("JMH");
});

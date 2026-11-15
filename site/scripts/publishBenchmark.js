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
 * Adds the JMH result files of a benchmark run to the benchmark data directory
 * served with the site at /benchmarks/data. A run contains the results of the
 * code under test and of the reference release, measured on the same runner.
 * The full results are copied to `runs/<run-id>.json` and
 * `runs/<run-id>-reference.json`, and a summary is added to `index.json`.
 *
 * Usage:
 *
 *   bun scripts/publishBenchmark.js --results benchmark-abc1234.json \
 *     --reference-results benchmark-abc1234-reference.json \
 *     --reference 9.1.0 --data-dir gh-pages/benchmarks/data \
 *     --run-id 123456789 --commit <sha> --version 11.0.0-SNAPSHOT \
 *     --date 2026-01-01T00:00:00Z
 *
 * @author John Grimes
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { parseArgs } from "node:util";
import { summariseResults } from "../src/benchmarks/jmh.js";
import { addRun } from "../src/benchmarks/runIndex.js";

/**
 * Publishes the results of one benchmark run.
 *
 * @param {object} options - The run to publish.
 * @param {string} options.resultsPath - The JMH JSON results file.
 * @param {string} options.referenceResultsPath - The JMH JSON results file of
 *   the reference release.
 * @param {string} options.reference - The version of the reference release.
 * @param {string} options.dataDir - The benchmark data directory.
 * @param {string} options.runId - The GitHub Actions run ID of the benchmark.
 * @param {string} options.commit - The SHA of the benchmarked commit.
 * @param {string} options.version - The benchmarked version: a release, or the
 *   development version of the commit.
 * @param {string} options.date - The ISO 8601 time at which the benchmark ran.
 * @returns {void}
 * @throws {Error} If either results file is not a JMH result array, or the
 *   date is not an ISO 8601 UTC time without fractional seconds.
 */
export function publishRun({
  resultsPath,
  referenceResultsPath,
  reference,
  dataDir,
  runId,
  commit,
  version,
  date,
}) {
  // Runs are ordered by comparing dates as strings, so they must share a format.
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(date)) {
    throw new Error(`The date ${date} is not in the form YYYY-MM-DDTHH:MM:SSZ`);
  }
  const content = readFileSync(resultsPath, "utf8");
  const results = parseResults(content, resultsPath);
  const referenceContent = readFileSync(referenceResultsPath, "utf8");
  const referenceResults = parseResults(referenceContent, referenceResultsPath);

  const indexPath = join(dataDir, "index.json");
  let index = {};
  try {
    index = JSON.parse(readFileSync(indexPath, "utf8"));
  } catch (error) {
    if (error.code !== "ENOENT") {
      throw error;
    }
  }

  mkdirSync(join(dataDir, "runs"), { recursive: true });
  writeFileSync(join(dataDir, "runs", `${runId}.json`), content);
  writeFileSync(
    join(dataDir, "runs", `${runId}-reference.json`),
    referenceContent,
  );
  const updated = addRun(index, {
    id: runId,
    date,
    commit,
    version,
    reference,
    results: summariseResults(results, referenceResults),
  });
  writeFileSync(indexPath, `${JSON.stringify(updated, null, 2)}\n`);
}

/**
 * Parses a JMH JSON result file.
 *
 * @param {string} content - The content of the file.
 * @param {string} path - The path of the file, for the error message.
 * @returns {object[]} The JMH results.
 * @throws {Error} If the content is not a JMH result array.
 */
function parseResults(content, path) {
  const results = JSON.parse(content);
  if (
    !Array.isArray(results) ||
    !results.every((result) => result.benchmark && result.primaryMetric)
  ) {
    throw new Error(`${path} is not a JMH JSON result file`);
  }
  return results;
}

if (import.meta.main) {
  const { values } = parseArgs({
    options: {
      results: { type: "string" },
      "reference-results": { type: "string" },
      reference: { type: "string" },
      "data-dir": { type: "string" },
      "run-id": { type: "string" },
      commit: { type: "string" },
      version: { type: "string" },
      date: { type: "string" },
    },
  });
  const missing = [
    "results",
    "reference-results",
    "reference",
    "data-dir",
    "run-id",
    "commit",
    "version",
    "date",
  ].filter((name) => !values[name]);
  if (missing.length > 0) {
    console.error(
      `Missing options: ${missing.map((m) => `--${m}`).join(", ")}`,
    );
    process.exit(1);
  }
  publishRun({
    resultsPath: values.results,
    referenceResultsPath: values["reference-results"],
    reference: values.reference,
    dataDir: values["data-dir"],
    runId: values["run-id"],
    commit: values.commit,
    version: values.version,
    date: values.date,
  });
  console.log(`Published run ${values["run-id"]} to ${values["data-dir"]}`);
}

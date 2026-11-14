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
 * Adds a JMH result file to the benchmark data directory served with the site
 * at /benchmarks/data. The full results are copied to `runs/<run-id>.json`
 * and a summary is added to `index.json`.
 *
 * Usage:
 *
 *   bun scripts/publishBenchmark.js --results benchmark-abc1234.json \
 *     --data-dir gh-pages/benchmarks/data --run-id 123456789 \
 *     --commit <sha> --date 2026-01-01T00:00:00Z
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
 * @param {string} options.dataDir - The benchmark data directory.
 * @param {string} options.runId - The GitHub Actions run ID of the benchmark.
 * @param {string} options.commit - The SHA of the benchmarked commit.
 * @param {string} options.date - The ISO 8601 time at which the benchmark ran.
 * @returns {void}
 * @throws {Error} If the results file is not a JMH result array.
 */
export function publishRun({ resultsPath, dataDir, runId, commit, date }) {
  const content = readFileSync(resultsPath, "utf8");
  const results = JSON.parse(content);
  if (
    !Array.isArray(results) ||
    !results.every((result) => result.benchmark && result.primaryMetric)
  ) {
    throw new Error(`${resultsPath} is not a JMH JSON result file`);
  }

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
  const updated = addRun(index, {
    id: runId,
    date,
    commit,
    results: summariseResults(results),
  });
  writeFileSync(indexPath, `${JSON.stringify(updated, null, 2)}\n`);
}

if (import.meta.main) {
  const { values } = parseArgs({
    options: {
      results: { type: "string" },
      "data-dir": { type: "string" },
      "run-id": { type: "string" },
      commit: { type: "string" },
      date: { type: "string" },
    },
  });
  const missing = ["results", "data-dir", "run-id", "commit", "date"].filter(
    (name) => !values[name],
  );
  if (missing.length > 0) {
    console.error(
      `Missing options: ${missing.map((m) => `--${m}`).join(", ")}`,
    );
    process.exit(1);
  }
  publishRun({
    resultsPath: values.results,
    dataDir: values["data-dir"],
    runId: values["run-id"],
    commit: values.commit,
    date: values.date,
  });
  console.log(`Published run ${values["run-id"]} to ${values["data-dir"]}`);
}

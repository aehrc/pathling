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
 * Comparison of the results of two benchmark runs.
 *
 * @author John Grimes
 */

import { iterationScores, resultKey, summariseResults } from "./jmh.js";
import { welchTTest } from "./stats.js";

/**
 * The comparison of one benchmark between two runs.
 *
 * @typedef {object} ComparisonRow
 * @property {string} key - The result key, see {@link resultKey}.
 * @property {string} benchmark - The class and method name.
 * @property {string} mode - The JMH mode.
 * @property {string} params - The parameter label.
 * @property {{score: number, scoreError: number | null, scoreUnit: string} | null} base
 *   - The result in the base run.
 * @property {{score: number, scoreError: number | null, scoreUnit: string} | null} target
 *   - The result in the target run.
 * @property {number | null} changePercent - The change in score from base to
 *   target, as a percentage of the base score.
 * @property {number | null} p - The two-sided p-value of Welch's t-test on the
 *   iteration scores.
 * @property {"improvement" | "regression" | "no-change" | "untestable" | "base-only" | "target-only"} verdict
 *   - The outcome of the comparison.
 */

/**
 * Compares the results of two runs. Each benchmark present in both runs is
 * tested for a significant difference using Welch's t-test on its iteration
 * scores.
 *
 * @param {object[]} baseResults - The JMH results of the base run.
 * @param {object[]} targetResults - The JMH results of the target run.
 * @param {number} [alpha=0.05] - The significance level.
 * @returns {ComparisonRow[]} One row per benchmark, ordered by benchmark name,
 *   parameters and mode.
 * @example
 * compareRuns(older, newer).filter((row) => row.verdict === "regression");
 */
export function compareRuns(baseResults, targetResults, alpha = 0.05) {
  const base = new Map(
    baseResults.map((result) => [resultKey(result), result]),
  );
  const target = new Map(
    targetResults.map((result) => [resultKey(result), result]),
  );
  const keys = new Set([...base.keys(), ...target.keys()]);
  return [...keys]
    .map((key) => compareResult(key, base.get(key), target.get(key), alpha))
    .sort(
      (a, b) =>
        a.benchmark.localeCompare(b.benchmark) ||
        a.params.localeCompare(b.params) ||
        a.mode.localeCompare(b.mode),
    );
}

/**
 * Compares one benchmark between two runs.
 *
 * @param {string} key - The result key.
 * @param {object | undefined} baseResult - The JMH result in the base run.
 * @param {object | undefined} targetResult - The JMH result in the target run.
 * @param {number} alpha - The significance level.
 * @returns {ComparisonRow} The comparison.
 */
function compareResult(key, baseResult, targetResult, alpha) {
  const [summary] = summariseResults([baseResult ?? targetResult]);
  const row = {
    key,
    benchmark: summary.benchmark,
    mode: summary.mode,
    params: summary.params,
    base: baseResult ? scoreOf(baseResult) : null,
    target: targetResult ? scoreOf(targetResult) : null,
    changePercent: null,
    p: null,
  };
  if (!baseResult || !targetResult) {
    return { ...row, verdict: baseResult ? "base-only" : "target-only" };
  }

  const changePercent =
    ((row.target.score - row.base.score) / row.base.score) * 100;
  const test = welchTTest(
    iterationScores(baseResult),
    iterationScores(targetResult),
  );
  if (!test) {
    return { ...row, changePercent, verdict: "untestable" };
  }
  // Throughput is the only JMH mode in which a higher score is better.
  const higherIsBetter = summary.mode === "thrpt";
  const improved = higherIsBetter ? changePercent > 0 : changePercent < 0;
  const verdict =
    test.p >= alpha ? "no-change" : improved ? "improvement" : "regression";
  return { ...row, changePercent, p: test.p, verdict };
}

/**
 * Extracts the score fields of a JMH result.
 *
 * @param {object} result - A JMH result.
 * @returns {{score: number, scoreError: number | null, scoreUnit: string}} The
 *   score, its error and unit.
 */
function scoreOf(result) {
  const [{ score, scoreError, scoreUnit }] = summariseResults([result]);
  return { score, scoreError, scoreUnit };
}

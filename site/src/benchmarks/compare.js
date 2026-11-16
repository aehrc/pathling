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
 * Each run benchmarks both the code under test and a fixed reference release
 * on the same runner. The comparison uses scores relative to the reference, so
 * that a difference in the speed of the two runners is not mistaken for a
 * change in performance.
 *
 * @author John Grimes
 */

import {
  iterationScores,
  paramsLabel,
  relativeScore,
  resultKey,
  shortBenchmarkName,
} from "./jmh.js";
import { welchTTest } from "./stats.js";

/**
 * The results of one run.
 *
 * @typedef {object} RunResults
 * @property {object[]} results - The JMH results of the code under test.
 * @property {object[]} referenceResults - The JMH results of the reference
 *   release from the same run.
 */

/**
 * The comparison of one benchmark between two runs.
 *
 * @typedef {object} ComparisonRow
 * @property {string} key - The result key, see {@link resultKey}.
 * @property {string} benchmark - The class and method name.
 * @property {string} mode - The JMH mode.
 * @property {string} params - The parameter label.
 * @property {import("./jmh.js").RelativeScore | null} base - The relative
 *   score in the base run.
 * @property {import("./jmh.js").RelativeScore | null} target - The relative
 *   score in the target run.
 * @property {number | null} changePercent - The change in relative score from
 *   base to target, as a percentage of the base relative score.
 * @property {number | null} p - The two-sided p-value of Welch's t-test on the
 *   relative iteration scores.
 * @property {"improvement" | "regression" | "no-change" | "untestable" | "no-reference" | "base-only" | "target-only"} verdict
 *   - The outcome of the comparison.
 */

/**
 * Compares the results of two runs. Each benchmark present in both runs is
 * tested for a significant difference using Welch's t-test on its iteration
 * scores, each divided by the reference score from the same run.
 *
 * The test does not account for the error in the reference scores, so it
 * slightly overstates the significance of a difference.
 *
 * @param {RunResults} baseRun - The results of the base run.
 * @param {RunResults} targetRun - The results of the target run.
 * @param {number} [alpha=0.05] - The significance level.
 * @returns {ComparisonRow[]} One row per benchmark, ordered by benchmark name,
 *   parameters and mode.
 * @example
 * compareRuns(older, newer).filter((row) => row.verdict === "regression");
 */
export function compareRuns(baseRun, targetRun, alpha = 0.05) {
  const base = pairWithReferences(baseRun);
  const target = pairWithReferences(targetRun);
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
 * Pairs each result of a run with its reference result.
 *
 * @param {RunResults} run - The results of a run.
 * @returns {Map<string, {result: object, reference: object | undefined}>} The
 *   pairs, keyed by result key.
 */
function pairWithReferences({ results, referenceResults }) {
  const references = new Map(
    referenceResults.map((result) => [resultKey(result), result]),
  );
  return new Map(
    results.map((result) => [
      resultKey(result),
      { result, reference: references.get(resultKey(result)) },
    ]),
  );
}

/**
 * Compares one benchmark between two runs.
 *
 * @param {string} key - The result key.
 * @param {{result: object, reference: object | undefined} | undefined} base -
 *   The result in the base run, with its reference result.
 * @param {{result: object, reference: object | undefined} | undefined} target -
 *   The result in the target run, with its reference result.
 * @param {number} alpha - The significance level.
 * @returns {ComparisonRow} The comparison.
 */
function compareResult(key, base, target, alpha) {
  const { result } = base ?? target;
  const row = {
    key,
    benchmark: shortBenchmarkName(result.benchmark),
    mode: result.mode,
    params: paramsLabel(result.params),
    base: base ? relativeScore(base.result, base.reference) : null,
    target: target ? relativeScore(target.result, target.reference) : null,
    changePercent: null,
    p: null,
  };
  if (!base || !target) {
    return { ...row, verdict: base ? "base-only" : "target-only" };
  }
  if (!row.base || !row.target) {
    return { ...row, verdict: "no-reference" };
  }

  const changePercent =
    ((row.target.score - row.base.score) / row.base.score) * 100;
  const test = welchTTest(relativeIterations(base), relativeIterations(target));
  if (!test) {
    return { ...row, changePercent, verdict: "untestable" };
  }
  // Throughput is the only JMH mode in which a higher score is better.
  const higherIsBetter = row.mode === "thrpt";
  const improved = higherIsBetter ? changePercent > 0 : changePercent < 0;
  const verdict =
    test.p >= alpha ? "no-change" : improved ? "improvement" : "regression";
  return { ...row, changePercent, p: test.p, verdict };
}

/**
 * Divides each iteration score of a result by its reference score.
 *
 * @param {{result: object, reference: object}} pair - A result with its
 *   reference result.
 * @returns {number[]} The relative iteration scores.
 */
function relativeIterations({ result, reference }) {
  return iterationScores(result).map(
    (score) => score / reference.primaryMetric.score,
  );
}

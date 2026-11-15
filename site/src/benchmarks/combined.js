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
 * Combined benchmark scores, which summarise the scores of many benchmarks in
 * a run as a single number.
 *
 * @author John Grimes
 */

/**
 * @typedef {import("./runIndex.js").RunIndex} RunIndex
 * @typedef {import("./runIndex.js").Timeline} Timeline
 */

/**
 * The phases of the benchmarks, in the order their timelines are shown. The
 * phase of a benchmark is the leading lower-case word of its method name.
 */
const PHASES = ["encode", "decode", "plan", "execute"];

/**
 * Combines scores into their geometric mean. The error is propagated assuming
 * that the scores are independent: the relative error of the mean is the root
 * sum of squares of the relative errors of the scores, divided by their
 * number.
 *
 * @param {{score: number, scoreError: number | null}[]} scores - The scores,
 *   at least one.
 * @returns {{score: number, scoreError: number | null}} The geometric mean, with
 *   no error if the error of any score is unknown.
 */
export function geometricMean(scores) {
  const n = scores.length;
  const score = Math.exp(
    scores.reduce((sum, entry) => sum + Math.log(entry.score), 0) / n,
  );
  if (scores.some((entry) => entry.scoreError == null)) {
    return { score, scoreError: null };
  }
  const relativeError =
    Math.sqrt(
      scores.reduce(
        (sum, entry) => sum + (entry.scoreError / entry.score) ** 2,
        0,
      ),
    ) / n;
  return { score, scoreError: score * relativeError };
}

/**
 * Identifies a result by its benchmark, mode and parameters.
 *
 * @param {import("./jmh.js").ResultSummary} result - The result.
 * @returns {string} The key of the result.
 */
function resultKey(result) {
  return `${result.benchmark}|${result.mode}|${result.params}`;
}

/**
 * Finds the phase of a benchmark from the leading lower-case word of its method
 * name.
 *
 * @param {string} benchmark - The class and method name.
 * @returns {string | undefined} The phase, or undefined if the method name does
 *   not start with a known phase.
 */
function phaseOf(benchmark) {
  const method = benchmark.slice(benchmark.lastIndexOf(".") + 1);
  const word = method.match(/^[a-z]+/)?.[0];
  return PHASES.includes(word) ? word : undefined;
}

/**
 * Builds the combined timelines: an overall timeline, followed by one timeline
 * for each phase. Each point is the geometric mean of the scores relative to
 * the reference release in one run.
 *
 * Only the benchmarks that have a relative score in every run are combined, so
 * that a benchmark added or removed part of the way through the history does
 * not shift the combined score. A phase without any such benchmark has no
 * timeline, and there are no timelines at all if there are no such benchmarks.
 *
 * @param {RunIndex} index - The run index.
 * @returns {Timeline[]} The combined timelines, in the same shape as the
 *   timelines of individual benchmarks.
 */
export function buildCombinedTimelines(index) {
  const runs = index.runs.map((run) => ({
    run,
    results: new Map(
      run.results
        .filter((result) => result.relative)
        .map((result) => [resultKey(result), result]),
    ),
  }));
  if (runs.length === 0) {
    return [];
  }
  const common = [...runs[0].results.values()].filter((result) =>
    runs.every(({ results }) => results.has(resultKey(result))),
  );
  if (common.length === 0) {
    return [];
  }

  const groups = [
    { name: "Overall", results: common },
    ...PHASES.map((phase) => ({
      name: phase[0].toUpperCase() + phase.slice(1),
      results: common.filter((result) => phaseOf(result.benchmark) === phase),
    })),
  ].filter((group) => group.results.length > 0);

  return groups.map((group) => ({
    benchmark: group.name,
    unit: `relative to ${index.runs[0].reference}`,
    series: [
      {
        label: "geometric mean",
        points: runs.map(({ run, results }) => ({
          runId: run.id,
          date: run.date,
          commit: run.commit,
          ...geometricMean(
            group.results.map(
              (result) => results.get(resultKey(result)).relative,
            ),
          ),
        })),
      },
    ],
  }));
}

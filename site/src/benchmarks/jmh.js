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
 * Functions for reading the JSON written by JMH with `-rf json`.
 *
 * @author John Grimes
 */

/**
 * A summary of one JMH result, as stored in the run index.
 *
 * @typedef {object} ResultSummary
 * @property {string} benchmark - The class and method name.
 * @property {string} mode - The JMH mode, e.g. "avgt" or "sample".
 * @property {string} params - The parameter label, see {@link paramsLabel}.
 * @property {number} score - The primary metric score.
 * @property {number | null} scoreError - The 99.9% error, if JMH reported one.
 * @property {string} scoreUnit - The unit of the score, e.g. "ms/op".
 */

/**
 * Removes the package from a fully qualified benchmark name.
 *
 * @param {string} name - The benchmark name reported by JMH.
 * @returns {string} The class and method name.
 * @example
 * shortBenchmarkName("au.csiro.pathling.benchmark.PathlingBenchmark.encode");
 * // "PathlingBenchmark.encode"
 */
export function shortBenchmarkName(name) {
  return name.split(".").slice(-2).join(".");
}

/**
 * Formats benchmark parameters as a stable label.
 *
 * @param {Record<string, string> | undefined} params - The JMH parameters.
 * @returns {string} The parameters as "name=value" pairs sorted by name, or an
 *   empty string if there are none.
 * @example
 * paramsLabel({ view: "PatientFlat", sourceType: "delta" });
 * // "sourceType=delta, view=PatientFlat"
 */
export function paramsLabel(params) {
  return Object.keys(params ?? {})
    .sort()
    .map((name) => `${name}=${params[name]}`)
    .join(", ");
}

/**
 * Builds a key that identifies the same benchmark across runs.
 *
 * @param {object} result - A JMH result.
 * @returns {string} A key combining benchmark name, mode and parameters.
 */
export function resultKey(result) {
  return [
    shortBenchmarkName(result.benchmark),
    result.mode,
    paramsLabel(result.params),
  ].join("|");
}

/**
 * Extracts the score of each measurement iteration, across all forks. These
 * are the observations used to test for a significant difference.
 *
 * In sample mode JMH records a histogram of [value, count] pairs for each
 * iteration, and the iteration score is the weighted mean of that histogram.
 *
 * @param {object} result - A JMH result.
 * @returns {number[]} The iteration scores, or an empty list if JMH did not
 *   record raw data.
 */
export function iterationScores(result) {
  const { rawData, rawDataHistogram } = result.primaryMetric;
  if (rawData) {
    return rawData.flat();
  }
  if (rawDataHistogram) {
    return rawDataHistogram.flat().map((histogram) => {
      let total = 0;
      let count = 0;
      for (const [value, n] of histogram) {
        total += value * n;
        count += n;
      }
      return total / count;
    });
  }
  return [];
}

/**
 * Reduces JMH results to the fields needed to plot them over time.
 *
 * @param {object[]} results - The JMH results of one run.
 * @returns {ResultSummary[]} The summaries.
 */
export function summariseResults(results) {
  return results.map((result) => {
    const { score, scoreError, scoreUnit } = result.primaryMetric;
    return {
      benchmark: shortBenchmarkName(result.benchmark),
      mode: result.mode,
      params: paramsLabel(result.params),
      score,
      scoreError: Number.isFinite(scoreError) ? scoreError : null,
      scoreUnit,
    };
  });
}

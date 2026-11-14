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
 * The index of published benchmark runs. The index lists every run with a
 * summary of its results, so that the timeline can be drawn without fetching
 * the full JMH output of each run.
 *
 * @author John Grimes
 */

/**
 * @typedef {import("./jmh.js").ResultSummary} ResultSummary
 */

/**
 * A published run.
 *
 * @typedef {object} RunEntry
 * @property {string} id - The GitHub Actions run ID of the benchmark workflow.
 *   The full JMH output is published at `runs/<id>.json`.
 * @property {string} date - The ISO 8601 time at which the benchmark ran.
 * @property {string} commit - The SHA of the commit that was benchmarked.
 * @property {ResultSummary[]} results - The summarised results.
 */

/**
 * The run index.
 *
 * @typedef {object} RunIndex
 * @property {RunEntry[]} runs - The runs, ordered by date.
 */

/**
 * Adds a run to the index, replacing any run with the same ID.
 *
 * @param {Partial<RunIndex>} index - The current index, which is not modified.
 * @param {RunEntry} entry - The run to add.
 * @returns {RunIndex} The updated index.
 */
export function addRun(index, entry) {
  const runs = (index.runs ?? []).filter((run) => run.id !== entry.id);
  return {
    runs: [...runs, entry].sort((a, b) => a.date.localeCompare(b.date)),
  };
}

/**
 * A point on a timeline.
 *
 * @typedef {object} TimelinePoint
 * @property {string} runId - The run ID.
 * @property {string} date - The time at which the benchmark ran.
 * @property {string} commit - The commit SHA.
 * @property {number} score - The score.
 * @property {number | null} scoreError - The 99.9% error.
 */

/**
 * The results of one benchmark over time.
 *
 * @typedef {object} Timeline
 * @property {string} benchmark - The class and method name.
 * @property {string} unit - The unit of the scores.
 * @property {{label: string, points: TimelinePoint[]}[]} series - One series
 *   per combination of mode and parameters.
 */

/**
 * Groups the results in the index into one timeline per benchmark.
 *
 * Each series is labelled by its parameters. The mode is added to the label
 * only when the benchmark has been run in more than one mode.
 *
 * @param {RunIndex} index - The run index.
 * @returns {Timeline[]} The timelines, ordered by benchmark name.
 */
export function buildTimelines(index) {
  /** @type {Map<string, Map<string, {mode: string, params: string, points: TimelinePoint[]}>>} */
  const benchmarks = new Map();
  const units = new Map();
  for (const run of index.runs) {
    for (const result of run.results) {
      if (!benchmarks.has(result.benchmark)) {
        benchmarks.set(result.benchmark, new Map());
        units.set(result.benchmark, result.scoreUnit);
      }
      const series = benchmarks.get(result.benchmark);
      const key = `${result.mode}|${result.params}`;
      if (!series.has(key)) {
        series.set(key, {
          mode: result.mode,
          params: result.params,
          points: [],
        });
      }
      series.get(key).points.push({
        runId: run.id,
        date: run.date,
        commit: run.commit,
        score: result.score,
        scoreError: result.scoreError,
      });
    }
  }

  return [...benchmarks.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([benchmark, series]) => {
      const multipleModes =
        new Set([...series.values()].map((entry) => entry.mode)).size > 1;
      return {
        benchmark,
        unit: units.get(benchmark),
        series: [...series.values()]
          .map((entry) => ({
            label:
              [multipleModes ? entry.mode : "", entry.params]
                .filter(Boolean)
                .join(", ") || "score",
            points: entry.points,
          }))
          .sort((a, b) => a.label.localeCompare(b.label)),
      };
    });
}

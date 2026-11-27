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
 * The details of one benchmark run: its environment, configuration and the
 * full results of each benchmark.
 *
 * @author John Grimes
 */

import React from "react";
import {
  iterationScores,
  paramsLabel,
  resultKey,
  shortBenchmarkName,
  summariseResults,
} from "../../benchmarks/jmh.js";
import {
  commitUrl,
  formatNumber,
  formatScore,
  runName,
  workflowRunUrl,
} from "./format";
import styles from "./styles.module.css";
import useJson from "./useJson";

const PERCENTILES = ["50.0", "90.0", "99.0"];

/**
 * Renders the details of a run.
 *
 * @param {object} props - The component props.
 * @param {import("../../benchmarks/runIndex.js").RunEntry} props.run - The run.
 * @param {string} props.resultsUrl - The URL of the run's JMH results.
 * @param {string} props.referenceResultsUrl - The URL of the JMH results of
 *   the reference release from the same run.
 * @param {React.ReactNode} props.navigation - Links to related views.
 * @returns {React.ReactElement} The run details.
 */
export default function RunDetail({
  run,
  resultsUrl,
  referenceResultsUrl,
  navigation,
}) {
  const results = useJson(resultsUrl);
  const referenceResults = useJson(referenceResultsUrl);
  const error = results.error ?? referenceResults.error;

  return (
    <section>
      <h2>Run {runName(run)}</h2>
      {navigation}
      <dl className={styles.metadata}>
        <dt>Commit</dt>
        <dd>
          <a href={commitUrl(run.commit)}>{run.commit}</a>
        </dd>
        <dt>Reference release</dt>
        <dd>{run.reference}</dd>
        <dt>Workflow run</dt>
        <dd>
          <a href={workflowRunUrl(run.id)}>{run.id}</a>
        </dd>
        <dt>Raw results</dt>
        <dd>
          <a href={resultsUrl} download>
            JMH JSON
          </a>
          ,{" "}
          <a href={referenceResultsUrl} download>
            JMH JSON for {run.reference}
          </a>
        </dd>
        {results.data?.length > 0 && <Environment result={results.data[0]} />}
      </dl>
      {(results.loading || referenceResults.loading) && (
        <p role="status">Loading results…</p>
      )}
      {error && <p className={styles.error}>{error}</p>}
      {results.data && referenceResults.data && (
        <ResultsTable
          results={results.data}
          referenceResults={referenceResults.data}
          reference={run.reference}
        />
      )}
    </section>
  );
}

/**
 * Renders the JVM and JMH configuration recorded with the results.
 *
 * @param {object} props - The component props.
 * @param {object} props.result - The first JMH result of the run.
 * @returns {React.ReactElement} Description list entries.
 */
function Environment({ result }) {
  return (
    <>
      <dt>JMH</dt>
      <dd>{result.jmhVersion}</dd>
      <dt>JVM</dt>
      <dd>
        {result.vmName} {result.vmVersion}
      </dd>
      <dt>JVM arguments</dt>
      <dd>
        <code>{(result.jvmArgs ?? []).join(" ")}</code>
      </dd>
      <dt>Configuration</dt>
      <dd>
        {result.forks} forks, {result.warmupIterations} × {result.warmupTime}{" "}
        warmup, {result.measurementIterations} × {result.measurementTime}{" "}
        measurement, {result.threads} thread(s)
      </dd>
    </>
  );
}

/**
 * Renders a table with one row per benchmark result.
 *
 * @param {object} props - The component props.
 * @param {object[]} props.results - The JMH results.
 * @param {object[]} props.referenceResults - The JMH results of the reference
 *   release from the same run.
 * @param {string} props.reference - The version of the reference release.
 * @returns {React.ReactElement} The table.
 */
function ResultsTable({ results, referenceResults, reference }) {
  const summaries = summariseResults(results, referenceResults);
  const referenceSummaries = new Map(
    referenceResults.map((result) => [
      resultKey(result),
      summariseResults([result], [])[0],
    ]),
  );
  return (
    <div className={styles.tableWrapper}>
      <table>
        <thead>
          <tr>
            <th>Benchmark</th>
            <th>Parameters</th>
            <th>Mode</th>
            <th>Score (99.9% CI)</th>
            <th>{reference} score</th>
            <th>Unit</th>
            <th>Relative to {reference}</th>
            {PERCENTILES.map((percentile) => (
              <th key={percentile}>p{parseFloat(percentile)}</th>
            ))}
            <th>Iteration scores</th>
          </tr>
        </thead>
        <tbody>
          {results.map((result, i) => {
            const metric = result.primaryMetric;
            const summary = summaries[i];
            const referenceSummary = referenceSummaries.get(resultKey(result));
            return (
              <tr key={resultKey(result)}>
                <td>{shortBenchmarkName(result.benchmark)}</td>
                <td>{paramsLabel(result.params)}</td>
                <td>{result.mode}</td>
                <td className={styles.number}>{formatScore(summary)}</td>
                <td className={styles.number}>
                  {referenceSummary ? formatScore(referenceSummary) : "-"}
                </td>
                <td>{metric.scoreUnit}</td>
                <td className={styles.number}>
                  {summary.relative ? formatScore(summary.relative) : "-"}
                </td>
                {PERCENTILES.map((percentile) => (
                  <td key={percentile} className={styles.number}>
                    {formatNumber(
                      metric.scorePercentiles?.[percentile] ?? null,
                    )}
                  </td>
                ))}
                <td className={styles.iterations}>
                  {iterationScores(result).map(formatNumber).join(", ")}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

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
  shortBenchmarkName,
  summariseResults,
} from "../../benchmarks/jmh.js";
import {
  commitUrl,
  formatDate,
  formatNumber,
  formatScore,
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
 * @param {React.ReactNode} props.navigation - Links to related views.
 * @returns {React.ReactElement} The run details.
 */
export default function RunDetail({ run, resultsUrl, navigation }) {
  const { data: results, error, loading } = useJson(resultsUrl);

  return (
    <section>
      <h2>Run {formatDate(run.date)}</h2>
      {navigation}
      <dl className={styles.metadata}>
        <dt>Commit</dt>
        <dd>
          <a href={commitUrl(run.commit)}>{run.commit}</a>
        </dd>
        <dt>Workflow run</dt>
        <dd>
          <a href={workflowRunUrl(run.id)}>{run.id}</a>
        </dd>
        <dt>Raw results</dt>
        <dd>
          <a href={resultsUrl} download>
            JMH JSON
          </a>
        </dd>
        {results?.length > 0 && <Environment result={results[0]} />}
      </dl>
      {loading && <p role="status">Loading results…</p>}
      {error && <p className={styles.error}>{error}</p>}
      {results && <ResultsTable results={results} />}
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
 * @returns {React.ReactElement} The table.
 */
function ResultsTable({ results }) {
  return (
    <div className={styles.tableWrapper}>
      <table>
        <thead>
          <tr>
            <th>Benchmark</th>
            <th>Parameters</th>
            <th>Mode</th>
            <th>Score (99.9% CI)</th>
            <th>Unit</th>
            {PERCENTILES.map((percentile) => (
              <th key={percentile}>p{parseFloat(percentile)}</th>
            ))}
            <th>Iteration scores</th>
          </tr>
        </thead>
        <tbody>
          {results.map((result) => {
            const metric = result.primaryMetric;
            return (
              <tr
                key={`${result.benchmark} ${result.mode} ${paramsLabel(result.params)}`}
              >
                <td>{shortBenchmarkName(result.benchmark)}</td>
                <td>{paramsLabel(result.params)}</td>
                <td>{result.mode}</td>
                <td className={styles.number}>
                  {formatScore(summariseResults([result])[0])}
                </td>
                <td>{metric.scoreUnit}</td>
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

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
 * The comparison of two benchmark runs, with a test of the statistical
 * significance of each difference.
 *
 * @author John Grimes
 */

import React from "react";
import { compareRuns } from "../../benchmarks/compare.js";
import { commitUrl, formatNumber, formatPValue, formatScore } from "./format";
import styles from "./styles.module.css";
import useJson from "./useJson";

const ALPHA = 0.05;

const VERDICTS = {
  improvement: { label: "Improvement", className: styles.improvement },
  regression: { label: "Regression", className: styles.regression },
  "no-change": { label: "No significant change", className: "" },
  untestable: { label: "Too few iterations to test", className: "" },
  "base-only": { label: "Only in base run", className: "" },
  "target-only": { label: "Only in target run", className: "" },
};

/**
 * Renders the comparison of two runs.
 *
 * @param {object} props - The component props.
 * @param {import("../../benchmarks/runIndex.js").RunEntry} props.base - The
 *   base run.
 * @param {import("../../benchmarks/runIndex.js").RunEntry} props.target - The
 *   target run.
 * @param {(runId: string) => string} props.resultsUrl - Builds the URL of a
 *   run's JMH results.
 * @param {(runId: string) => React.ReactNode} props.runLink - Renders a link to
 *   a run's details.
 * @param {React.ReactNode} props.navigation - Links to related views.
 * @returns {React.ReactElement} The comparison.
 */
export default function RunComparison({
  base,
  target,
  resultsUrl,
  runLink,
  navigation,
}) {
  const baseResults = useJson(resultsUrl(base.id));
  const targetResults = useJson(resultsUrl(target.id));
  const error = baseResults.error ?? targetResults.error;
  const rows =
    baseResults.data && targetResults.data
      ? compareRuns(baseResults.data, targetResults.data, ALPHA)
      : null;

  return (
    <section>
      <h2>Comparison</h2>
      {navigation}
      <dl className={styles.metadata}>
        <dt>Base</dt>
        <dd>
          {runLink(base.id)}, commit{" "}
          <a href={commitUrl(base.commit)}>{base.commit.slice(0, 7)}</a>
        </dd>
        <dt>Target</dt>
        <dd>
          {runLink(target.id)}, commit{" "}
          <a href={commitUrl(target.commit)}>{target.commit.slice(0, 7)}</a>
        </dd>
      </dl>
      <p>
        Each difference is tested with a two-sided Welch&apos;s t-test on the
        per-iteration scores of the two runs (one score per measurement
        iteration in each fork). A difference is significant when p &lt; {ALPHA}
        . The change is the difference in JMH score as a percentage of the base
        score.
      </p>
      {(baseResults.loading || targetResults.loading) && (
        <p role="status">Loading results…</p>
      )}
      {error && <p className={styles.error}>{error}</p>}
      {rows && <ComparisonTable rows={rows} />}
    </section>
  );
}

/**
 * Renders the comparison rows with a summary of the verdicts.
 *
 * @param {object} props - The component props.
 * @param {import("../../benchmarks/compare.js").ComparisonRow[]} props.rows -
 *   The comparison rows.
 * @returns {React.ReactElement} The summary and table.
 */
function ComparisonTable({ rows }) {
  const count = (verdict) =>
    rows.filter((row) => row.verdict === verdict).length;
  return (
    <>
      <p role="status">
        Of {rows.length} benchmark(s): {count("improvement")} significant
        improvement(s), {count("regression")} significant regression(s),{" "}
        {count("no-change")} without a significant change, {count("untestable")}{" "}
        with too few iterations to test, and{" "}
        {count("base-only") + count("target-only")} present in only one run.
      </p>
      <div className={styles.tableWrapper}>
        <table>
          <thead>
            <tr>
              <th>Benchmark</th>
              <th>Parameters</th>
              <th>Mode</th>
              <th>Base</th>
              <th>Target</th>
              <th>Unit</th>
              <th>Change</th>
              <th>p</th>
              <th>Verdict</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const verdict = VERDICTS[row.verdict];
              const unit = (row.base ?? row.target).scoreUnit;
              return (
                <tr key={row.key}>
                  <td>{row.benchmark}</td>
                  <td>{row.params}</td>
                  <td>{row.mode}</td>
                  <td className={styles.number}>
                    {row.base ? formatScore(row.base) : "-"}
                  </td>
                  <td className={styles.number}>
                    {row.target ? formatScore(row.target) : "-"}
                  </td>
                  <td>{unit}</td>
                  <td className={`${styles.number} ${verdict.className}`}>
                    {row.changePercent === null
                      ? "-"
                      : `${row.changePercent > 0 ? "+" : ""}${formatNumber(row.changePercent)}%`}
                  </td>
                  <td className={styles.number}>{formatPValue(row.p)}</td>
                  <td className={verdict.className}>{verdict.label}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </>
  );
}

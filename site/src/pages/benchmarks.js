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
 * The benchmarks page. It shows the results of every published benchmark run
 * over time, the details of a single run (`?run=<id>`), and the comparison of
 * two runs (`?base=<id>&target=<id>`).
 *
 * The data is published to /benchmarks/data by the benchmark workflow, not
 * built into the site, so that new runs appear without rebuilding the site.
 *
 * @author John Grimes
 */

import { useHistory, useLocation } from "@docusaurus/router";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Layout from "@theme/Layout";
import React, { useState } from "react";
import { buildTimelines } from "../benchmarks/runIndex.js";
import RunComparison from "../components/benchmarks/RunComparison";
import RunDetail from "../components/benchmarks/RunDetail";
import TimelineChart from "../components/benchmarks/TimelineChart";
import {
  commitUrl,
  formatDate,
  runLabel,
} from "../components/benchmarks/format";
import styles from "../components/benchmarks/styles.module.css";
import useJson from "../components/benchmarks/useJson";

/**
 * Renders the benchmarks page.
 *
 * @returns {React.ReactElement} The page.
 */
export default function Benchmarks() {
  const dataUrl = useBaseUrl("/benchmarks/data");
  const location = useLocation();
  const history = useHistory();
  const {
    data: index,
    error,
    notFound,
    loading,
  } = useJson(`${dataUrl}/index.json`);

  const query = new URLSearchParams(location.search);
  const href = (params) =>
    `${location.pathname}?${new URLSearchParams(params)}`;
  const navigate = (params) => history.push(href(params));
  const resultsUrl = (runId) => `${dataUrl}/runs/${runId}.json`;
  const runs = index?.runs ?? [];
  const findRun = (id) => runs.find((run) => run.id === id);

  const runLink = (runId) => (
    <a
      href={href({ run: runId })}
      onClick={(event) => {
        event.preventDefault();
        navigate({ run: runId });
      }}
    >
      {formatDate(findRun(runId).date)}
    </a>
  );
  const overviewLink = (
    <a
      href={location.pathname}
      onClick={(event) => {
        event.preventDefault();
        history.push(location.pathname);
      }}
    >
      All runs
    </a>
  );

  let content;
  if (loading) {
    content = <p role="status">Loading benchmark runs…</p>;
  } else if (runs.length === 0 && (notFound || !error)) {
    // The index does not exist until the first run is published.
    content = <p>No benchmark runs have been published yet.</p>;
  } else if (error) {
    content = <p className={styles.error}>{error}</p>;
  } else if (query.has("base") || query.has("target")) {
    const base = findRun(query.get("base"));
    const target = findRun(query.get("target"));
    content =
      base && target ? (
        <RunComparison
          base={base}
          target={target}
          resultsUrl={resultsUrl}
          runLink={runLink}
          navigation={<p>{overviewLink}</p>}
        />
      ) : (
        <UnknownRun overviewLink={overviewLink} />
      );
  } else if (query.has("run")) {
    const position = runs.findIndex((run) => run.id === query.get("run"));
    const run = runs[position];
    const previous = runs[position - 1];
    content = run ? (
      <RunDetail
        key={run.id}
        run={run}
        resultsUrl={resultsUrl(run.id)}
        navigation={
          <p className={styles.navigation}>
            {overviewLink}
            {previous && (
              <a
                href={href({ base: previous.id, target: run.id })}
                onClick={(event) => {
                  event.preventDefault();
                  navigate({ base: previous.id, target: run.id });
                }}
              >
                Compare with previous run
              </a>
            )}
          </p>
        }
      />
    ) : (
      <UnknownRun overviewLink={overviewLink} />
    );
  } else {
    content = (
      <Overview
        runs={runs}
        index={index}
        runHref={(runId) => href({ run: runId })}
        runLink={runLink}
        onSelectRun={(runId) => navigate({ run: runId })}
        onCompare={(base, target) => navigate({ base, target })}
      />
    );
  }

  return (
    <Layout
      title="Benchmarks"
      description="Performance benchmark results for Pathling over time"
    >
      <main className={`container margin-vert--lg ${styles.page}`}>
        <h1>Benchmarks</h1>
        {content}
      </main>
    </Layout>
  );
}

/**
 * Renders a message for a run that is not in the index.
 *
 * @param {object} props - The component props.
 * @param {React.ReactNode} props.overviewLink - A link back to all runs.
 * @returns {React.ReactElement} The message.
 */
function UnknownRun({ overviewLink }) {
  return (
    <p className={styles.error}>
      The requested run has not been published. {overviewLink}
    </p>
  );
}

/**
 * Renders the comparison form and a timeline for each benchmark.
 *
 * @param {object} props - The component props.
 * @param {import("../benchmarks/runIndex.js").RunEntry[]} props.runs - The
 *   runs, ordered by date.
 * @param {import("../benchmarks/runIndex.js").RunIndex} props.index - The run
 *   index.
 * @param {(runId: string) => string} props.runHref - Builds the link to a run.
 * @param {(runId: string) => React.ReactNode} props.runLink - Renders a link
 *   to a run.
 * @param {(runId: string) => void} props.onSelectRun - Opens a run.
 * @param {(base: string, target: string) => void} props.onCompare - Opens the
 *   comparison of two runs.
 * @returns {React.ReactElement} The overview.
 */
function Overview({ runs, index, runHref, runLink, onSelectRun, onCompare }) {
  const [base, setBase] = useState(runs[Math.max(runs.length - 2, 0)].id);
  const [target, setTarget] = useState(runs[runs.length - 1].id);

  const options = [...runs].reverse().map((run) => (
    <option key={run.id} value={run.id}>
      {runLabel(run)}
    </option>
  ));

  return (
    <>
      <p>
        These are the results of the{" "}
        <a href="https://github.com/aehrc/pathling/tree/main/benchmark">
          JMH benchmarks
        </a>{" "}
        run by the benchmark workflow on GitHub-hosted runners. Each point is
        one run; select a point to see the full results of that run. Shared
        runners vary in performance, so compare runs using the significance test
        rather than by eye.
      </p>
      <form
        className={styles.compareForm}
        onSubmit={(event) => {
          event.preventDefault();
          onCompare(base, target);
        }}
      >
        <label>
          Base{" "}
          <select value={base} onChange={(e) => setBase(e.target.value)}>
            {options}
          </select>
        </label>
        <label>
          Target{" "}
          <select value={target} onChange={(e) => setTarget(e.target.value)}>
            {options}
          </select>
        </label>
        <button className="button button--primary" type="submit">
          Compare
        </button>
      </form>
      {buildTimelines(index).map((timeline) => (
        <section key={timeline.benchmark}>
          <h2>{timeline.benchmark}</h2>
          <TimelineChart
            timeline={timeline}
            runHref={runHref}
            onSelectRun={onSelectRun}
            dateRange={[runs[0].date, runs[runs.length - 1].date]}
          />
        </section>
      ))}
      <section>
        <h2>Runs</h2>
        <ul>
          {[...runs].reverse().map((run) => (
            <li key={run.id}>
              {runLink(run.id)}, commit{" "}
              <a href={commitUrl(run.commit)}>{run.commit.slice(0, 7)}</a>
            </li>
          ))}
        </ul>
      </section>
    </>
  );
}

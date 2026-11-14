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
 * A line chart of one benchmark's scores over time, with one line per
 * parameter combination. Each point links to the run it came from.
 *
 * @author John Grimes
 */

import React from "react";
import { commitUrl, formatDate, formatNumber, formatScore } from "./format";
import styles from "./styles.module.css";

const WIDTH = 800;
const HEIGHT = 300;
const MARGIN = { top: 12, right: 40, bottom: 48, left: 64 };
const PLOT_WIDTH = WIDTH - MARGIN.left - MARGIN.right;
const PLOT_HEIGHT = HEIGHT - MARGIN.top - MARGIN.bottom;
// The minimum horizontal distance between commit labels.
const LABEL_SPACING = 56;

// A qualitative palette that remains distinguishable in light and dark modes.
const COLOURS = [
  "#2890a4",
  "#e6862e",
  "#7b5ea7",
  "#3a9a4b",
  "#d0453f",
  "#8c6d31",
  "#d65fa8",
  "#6f7f8f",
];

/**
 * Chooses a tick spacing of 1, 2 or 5 times a power of ten.
 *
 * @param {number} max - The largest value on the axis.
 * @param {number} count - The approximate number of ticks wanted.
 * @returns {number[]} The tick values from zero to at least max.
 */
function niceTicks(max, count) {
  // An axis needs a positive range, even if every score is zero.
  const raw = max > 0 ? max / count : 1;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  const step =
    [1, 2, 5, 10].map((m) => m * magnitude).find((s) => s >= raw) ?? raw;
  const ticks = [];
  for (let value = 0; value < Math.max(max, raw) + step; value += step) {
    ticks.push(value);
  }
  return ticks;
}

/**
 * Renders the timeline of one benchmark.
 *
 * @param {object} props - The component props.
 * @param {import("../../benchmarks/runIndex.js").Timeline} props.timeline - The
 *   benchmark's timeline.
 * @param {(runId: string) => void} props.onSelectRun - Called when a point is
 *   clicked.
 * @param {(runId: string) => string} props.runHref - Builds the link to a run.
 * @param {[string, string]} props.dateRange - The dates of the first and last
 *   runs, so that every chart shares the same time axis.
 * @returns {React.ReactElement} The chart.
 */
export default function TimelineChart({
  timeline,
  onSelectRun,
  runHref,
  dateRange,
}) {
  const points = timeline.series.flatMap((series) => series.points);
  const [minTime, maxTime] = dateRange.map((date) => Date.parse(date));
  const maxScore = Math.max(
    ...points.map((point) => point.score + (point.scoreError ?? 0)),
  );
  const yTicks = niceTicks(maxScore, 5);
  const yMax = yTicks[yTicks.length - 1];

  const x = (date) =>
    MARGIN.left +
    (maxTime === minTime
      ? PLOT_WIDTH / 2
      : ((Date.parse(date) - minTime) / (maxTime - minTime)) * PLOT_WIDTH);
  const y = (value) => MARGIN.top + PLOT_HEIGHT * (1 - value / yMax);

  // Label each run in this chart with its commit, skipping labels that would
  // overlap, but always labelling the first and last runs with their date too.
  // Times are shown when all runs span less than three days.
  const showTime = maxTime - minTime < 3 * 24 * 60 * 60 * 1000;
  const dateLabel = (date) => {
    const iso = new Date(date).toISOString();
    return showTime ? iso.slice(5, 16).replace("T", " ") : iso.slice(0, 10);
  };
  const runs = [
    ...new Map(points.map((point) => [point.runId, point])).values(),
  ].sort((a, b) => a.date.localeCompare(b.date));
  const labelled = [];
  for (const run of runs) {
    if (
      labelled.length > 0 &&
      x(run.date) - x(labelled[labelled.length - 1].date) < LABEL_SPACING
    ) {
      // Make way for the last run, which is always labelled.
      if (run !== runs[runs.length - 1] || labelled.length === 1) {
        continue;
      }
      labelled.pop();
    }
    labelled.push(run);
  }

  const handleClick = (event, runId) => {
    event.preventDefault();
    onSelectRun(runId);
  };

  return (
    <figure className={styles.chart}>
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        aria-label={`${timeline.benchmark} scores over time`}
      >
        {yTicks.map((tick) => (
          <g key={tick}>
            <line
              className={styles.gridLine}
              x1={MARGIN.left}
              x2={WIDTH - MARGIN.right}
              y1={y(tick)}
              y2={y(tick)}
            />
            <text
              className={styles.axisLabel}
              x={MARGIN.left - 8}
              y={y(tick)}
              textAnchor="end"
              dominantBaseline="middle"
            >
              {formatNumber(tick)}
            </text>
          </g>
        ))}
        <text
          className={styles.axisLabel}
          transform={`translate(14 ${MARGIN.top + PLOT_HEIGHT / 2}) rotate(-90)`}
          textAnchor="middle"
        >
          {timeline.unit}
        </text>
        {labelled.map((run, index) => (
          <g key={run.runId}>
            <a href={commitUrl(run.commit)}>
              <title>{`Commit ${run.commit}`}</title>
              <text
                className={styles.commitLabel}
                x={x(run.date)}
                y={HEIGHT - 26}
                textAnchor="middle"
              >
                {run.commit.slice(0, 7)}
              </text>
            </a>
            {(index === 0 || index === labelled.length - 1) && (
              <text
                className={styles.axisLabel}
                x={x(run.date)}
                y={HEIGHT - 8}
                textAnchor="middle"
              >
                {dateLabel(run.date)}
              </text>
            )}
          </g>
        ))}
        {timeline.series.map((series, index) => {
          const colour = COLOURS[index % COLOURS.length];
          return (
            <g key={series.label}>
              <polyline
                fill="none"
                stroke={colour}
                strokeWidth={2}
                points={series.points
                  .map((point) => `${x(point.date)},${y(point.score)}`)
                  .join(" ")}
              />
              {series.points.map((point) => (
                <a
                  key={point.runId}
                  href={runHref(point.runId)}
                  onClick={(event) => handleClick(event, point.runId)}
                >
                  <title>
                    {`${series.label}: ${formatScore(point)} ${timeline.unit}\n` +
                      `${formatDate(point.date)}, commit ${point.commit.slice(0, 7)}`}
                  </title>
                  {point.scoreError !== null && (
                    <line
                      className={styles.errorBar}
                      stroke={colour}
                      x1={x(point.date)}
                      x2={x(point.date)}
                      y1={y(Math.max(0, point.score - point.scoreError))}
                      y2={y(point.score + point.scoreError)}
                    />
                  )}
                  <circle
                    className={styles.point}
                    cx={x(point.date)}
                    cy={y(point.score)}
                    r={4}
                    fill={colour}
                  />
                </a>
              ))}
            </g>
          );
        })}
      </svg>
      <figcaption className={styles.legend}>
        {timeline.series.map((series, index) => (
          <span key={series.label} className={styles.legendItem}>
            <span
              className={styles.swatch}
              style={{ background: COLOURS[index % COLOURS.length] }}
            />
            {series.label}
          </span>
        ))}
      </figcaption>
    </figure>
  );
}

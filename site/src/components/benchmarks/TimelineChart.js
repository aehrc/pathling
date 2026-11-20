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
import { lineObstacles, placeTags } from "../../benchmarks/annotations.js";
import {
  commitUrl,
  formatDate,
  formatNumber,
  formatScore,
  runName,
} from "./format";
import styles from "./styles.module.css";

const WIDTH = 800;
const MARGIN = { top: 12, right: 40, bottom: 48, left: 64 };
const PLOT_WIDTH = WIDTH - MARGIN.left - MARGIN.right;
const PLOT_HEIGHT = 240;
// The minimum horizontal distance between commit labels.
const LABEL_SPACING = 56;
const POINT_RADIUS = 4;
// The space that the tag of an annotation takes up across its line, the space
// between the foot of the plot and the first tag on a line, and the minimum
// space between two tags along a line.
const TAG_THICKNESS = 14;
const TAG_INSET = 4;
const TAG_GAP = 6;
// The approximate width of a character in the tag of a release and in the bold
// tag of a change.
const TAG_CHAR_WIDTH = { release: 6, change: 7 };
const HEIGHT = MARGIN.top + PLOT_HEIGHT + MARGIN.bottom;
const PLOT_BOTTOM = MARGIN.top + PLOT_HEIGHT;

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
 * Builds the hover text of an annotation.
 *
 * @param {import("../../benchmarks/annotations.js").Annotation} annotation -
 *   The annotation.
 * @returns {string} The label and date, and the description if there is one.
 */
function annotationTitle({ kind, label, date, description }) {
  const heading = `${kind === "release" ? "Release " : ""}${label}, ${formatDate(date)}`;
  return description ? `${heading}\n${description}` : heading;
}

/**
 * Renders part of the marker of an annotation, linked to the page about the
 * annotation if it has one.
 *
 * @param {object} props - The component props.
 * @param {import("../../benchmarks/annotations.js").Annotation}
 *   props.annotation - The annotation.
 * @param {React.ReactNode} props.children - The part of the marker.
 * @returns {React.ReactElement} The linked part.
 */
function AnnotationLink({ annotation, children }) {
  return (
    <a
      href={annotation.url}
      className={
        annotation.kind === "release"
          ? styles.releaseAnnotation
          : styles.changeAnnotation
      }
    >
      <title>{annotationTitle(annotation)}</title>
      {children}
    </a>
  );
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
 * @param {import("../../benchmarks/annotations.js").Annotation[]}
 *   props.annotations - The annotations to mark, which must lie within the
 *   date range.
 * @returns {React.ReactElement} The chart.
 */
export default function TimelineChart({
  timeline,
  onSelectRun,
  runHref,
  dateRange,
  annotations,
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

  // Mark each annotation with a vertical line through the plot, tagged with
  // its label. A tag runs up its line from the foot of the plot, where the
  // scores usually leave room, as they lie near one on an axis from zero.
  // Where a series is in the way, the tag moves up its line to clear it, and
  // is drawn over the series only when it cannot. A tag with no room clear of
  // the other tags is left out, leaving its line and hover text.
  const tagOffset = (value) => PLOT_BOTTOM - TAG_INSET - y(value);
  const obstacles = timeline.series.flatMap((series) => {
    const centres = series.points.map((point) => ({
      x: x(point.date),
      offset: tagOffset(point.score),
    }));
    return [
      // Each point, with its error bar.
      ...series.points.map((point, index) => {
        const centre = centres[index];
        const error = point.scoreError ?? 0;
        return {
          from: centre.x - POINT_RADIUS,
          to: centre.x + POINT_RADIUS,
          start: Math.min(
            centre.offset - POINT_RADIUS,
            tagOffset(point.score - error),
          ),
          end: Math.max(
            centre.offset + POINT_RADIUS,
            tagOffset(point.score + error),
          ),
        };
      }),
      // The line from each point to the next.
      ...centres
        .slice(1)
        .flatMap((centre, index) =>
          lineObstacles(centres[index], centre, TAG_THICKNESS / 2),
        ),
    ];
  });
  const tagPlacements = placeTags(
    annotations.map((annotation) => ({
      x: x(annotation.date),
      length: annotation.label.length * TAG_CHAR_WIDTH[annotation.kind],
      kind: annotation.kind,
    })),
    {
      thickness: TAG_THICKNESS,
      gap: TAG_GAP,
      minX: MARGIN.left,
      maxX: WIDTH - MARGIN.right,
      maxOffset: PLOT_HEIGHT - TAG_INSET,
    },
    obstacles,
  );
  // Paint the lines of changes over those of releases, as a change often lands
  // just before the release that ships it, and would otherwise be hidden
  // beneath it.
  const linePaintOrder = [...annotations].sort(
    (a, b) => Number(a.kind === "change") - Number(b.kind === "change"),
  );

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
        {linePaintOrder.map((annotation) => {
          const lineProps = {
            x1: x(annotation.date),
            x2: x(annotation.date),
            y1: MARGIN.top,
            y2: PLOT_BOTTOM,
          };
          return (
            <AnnotationLink key={annotation.label} annotation={annotation}>
              <line className={styles.annotationLine} {...lineProps} />
              <line className={styles.annotationHitArea} {...lineProps} />
            </AnnotationLink>
          );
        })}
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
                      `${runName(point)}, commit ${point.commit.slice(0, 7)}`}
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
                    r={POINT_RADIUS}
                    fill={colour}
                  />
                </a>
              ))}
            </g>
          );
        })}
        {/* The tags are painted last, so that their outlines keep them legible
            over the lines and scores that they cross. */}
        {annotations.map((annotation, index) => {
          const placement = tagPlacements[index];
          if (placement === null) {
            return null;
          }
          const centre =
            x(annotation.date) +
            ((placement.side === "left" ? -1 : 1) * TAG_THICKNESS) / 2;
          const start = PLOT_BOTTOM - TAG_INSET - placement.offset;
          return (
            <AnnotationLink
              key={`tag-${annotation.label}`}
              annotation={annotation}
            >
              <text
                className={styles.annotationTag}
                transform={`translate(${centre} ${start}) rotate(-90)`}
                dominantBaseline="central"
              >
                {annotation.label}
              </text>
            </AnnotationLink>
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

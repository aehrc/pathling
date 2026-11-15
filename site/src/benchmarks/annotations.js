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
 * Annotations mark releases and notable changes on the benchmark timelines.
 * They are kept in annotations.json alongside this file.
 *
 * @author John Grimes
 */

/**
 * @typedef {import("./runIndex.js").RunEntry} RunEntry
 */

/**
 * A marker on the timelines.
 *
 * @typedef {object} Annotation
 * @property {string} date - The ISO 8601 time in UTC at which the annotation
 *   is placed: the time of the tagged commit for a release, and the time of
 *   the merge for a change.
 * @property {"release" | "change"} kind - Whether the annotation marks a
 *   release or a change that affected performance.
 * @property {string} label - The short text shown on the charts.
 * @property {string} [description] - A longer description.
 * @property {string} [url] - A page about the release or change.
 */

/**
 * Selects the annotations within a range of dates.
 *
 * @param {Annotation[]} annotations - The annotations, which are not
 *   modified.
 * @param {string} start - The ISO 8601 time at which the range starts.
 * @param {string} end - The ISO 8601 time at which the range ends.
 * @returns {Annotation[]} The annotations from start to end inclusive, ordered
 *   by date.
 */
export function annotationsBetween(annotations, start, end) {
  const [from, to] = [Date.parse(start), Date.parse(end)];
  return annotations
    .filter((annotation) => {
      const time = Date.parse(annotation.date);
      return time >= from && time <= to;
    })
    .sort((a, b) => Date.parse(a.date) - Date.parse(b.date));
}

/**
 * Assigns labels to rows so that no two labels in a row overlap. Change labels
 * are placed before release labels, and otherwise labels are placed in the
 * order given. Each label goes in the first row where it fits.
 *
 * @param {{left: number, right: number, kind: string}[]} spans - The
 *   horizontal extent of each label, and the kind of its annotation.
 * @param {number} rowCount - The number of rows available.
 * @returns {(number | null)[]} The row of each label, in the order of the
 *   spans, or null for a label that does not fit in any row.
 * @example
 * assignLabelRows(
 *   [
 *     { left: 0, right: 10, kind: "release" },
 *     { left: 5, right: 15, kind: "release" },
 *   ],
 *   3,
 * ); // [0, 1]
 */
export function assignLabelRows(spans, rowCount) {
  /** @type {{left: number, right: number}[][]} */
  const rows = Array.from({ length: rowCount }, () => []);
  const result = spans.map(() => null);
  const order = spans
    .map((_, index) => index)
    .sort(
      (a, b) =>
        Number(spans[a].kind !== "change") - Number(spans[b].kind !== "change"),
    );
  for (const index of order) {
    const { left, right } = spans[index];
    const row = rows.findIndex((placed) =>
      placed.every((other) => right <= other.left || left >= other.right),
    );
    if (row !== -1) {
      rows[row].push(spans[index]);
      result[index] = row;
    }
  }
  return result;
}

/**
 * Merges runs and annotations into a single list ordered by date. An
 * annotation follows a run with the same date, so that a release follows a
 * run of its tagged commit that is dated by that commit.
 *
 * @param {RunEntry[]} runs - The runs, ordered by date.
 * @param {Annotation[]} annotations - The annotations, ordered by date.
 * @returns {({run: RunEntry} | {annotation: Annotation})[]} The merged list.
 */
export function interleaveAnnotations(runs, annotations) {
  const entries = [
    ...runs.map((run) => ({ time: Date.parse(run.date), order: 0, run })),
    ...annotations.map((annotation) => ({
      time: Date.parse(annotation.date),
      order: 1,
      annotation,
    })),
  ];
  return entries
    .sort((a, b) => a.time - b.time || a.order - b.order)
    .map(({ run, annotation }) => (run ? { run } : { annotation }));
}

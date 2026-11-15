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
 * An area of a chart, in the terms used to place tags.
 *
 * @typedef {object} TagArea
 * @property {number} from - The position across the lines at which the area
 *   starts.
 * @property {number} to - The position across the lines at which it ends.
 * @property {number} start - The distance from the start of the lines at which
 *   the area starts.
 * @property {number} end - The distance from the start of the lines at which
 *   it ends.
 */

/**
 * A point on a chart, in the terms used to place tags.
 *
 * @typedef {object} TagPoint
 * @property {number} x - The position of the point across the lines.
 * @property {number} offset - The distance of the point from the start of the
 *   lines.
 */

/**
 * Where a tag lies relative to its line.
 *
 * @typedef {object} TagPlacement
 * @property {"left" | "right"} side - The side of the line that the tag is on.
 * @property {number} offset - The distance of the tag from the start of the
 *   line.
 */

/**
 * Covers a straight line with areas for tags to keep clear of. The line is
 * divided evenly into areas no wider than a limit, each covering the part of
 * the line that crosses it, so that a steep line takes up less than its
 * bounding box.
 *
 * @param {TagPoint} a - One end of the line.
 * @param {TagPoint} b - The other end of the line.
 * @param {number} maxWidth - The widest that an area may be.
 * @returns {TagArea[]} The areas, in order from the first end to the other.
 * @example
 * lineObstacles({ x: 0, offset: 0 }, { x: 20, offset: 40 }, 10);
 * // [
 * //   { from: 0, to: 10, start: 0, end: 20 },
 * //   { from: 10, to: 20, start: 20, end: 40 },
 * // ]
 */
export function lineObstacles(a, b, maxWidth) {
  const count = Math.max(1, Math.ceil(Math.abs(b.x - a.x) / maxWidth));
  const at = (index) => ({
    x: a.x + ((b.x - a.x) * index) / count,
    offset: a.offset + ((b.offset - a.offset) * index) / count,
  });
  return Array.from({ length: count }, (_, index) => {
    const [one, other] = [at(index), at(index + 1)];
    return {
      from: Math.min(one.x, other.x),
      to: Math.max(one.x, other.x),
      start: Math.min(one.offset, other.offset),
      end: Math.max(one.offset, other.offset),
    };
  });
}

/**
 * The space available to tags.
 *
 * @typedef {object} TagBounds
 * @property {number} thickness - The extent of a tag across its line.
 * @property {number} gap - The minimum space along a line between a tag and
 *   another tag or an obstacle.
 * @property {number} minX - The position before which no tag may extend.
 * @property {number} maxX - The position beyond which no tag may extend.
 * @property {number} maxOffset - The distance from the start of a line beyond
 *   which no tag may extend.
 */

/**
 * Finds the room for a tag that is nearest to the start of its line.
 *
 * @param {{x: number, length: number}} tag - The position of the tag's line,
 *   and the length of the tag along the line.
 * @param {TagBounds} bounds - The space available to the tag.
 * @param {TagArea[]} taken - The areas that the tag must keep clear of.
 * @returns {{side: "left" | "right", area: TagArea} | null} The side of the
 *   line with the nearest room and the area of that room, preferring the left
 *   when both are as near, or null when the tag has no room.
 */
function findRoom({ x, length }, bounds, taken) {
  const { thickness, gap, minX, maxX, maxOffset } = bounds;
  let best = null;
  for (const side of ["left", "right"]) {
    const from = side === "left" ? x - thickness : x;
    const to = from + thickness;
    if (from < minX || to > maxX) {
      continue;
    }
    // Move along the line to the end of each area that is in the way. Every
    // move is further along the line, so this ends.
    let start = 0;
    let blocker;
    const isInTheWay = (other) =>
      from < other.to &&
      to > other.from &&
      start < other.end + gap &&
      start + length + gap > other.start;
    while ((blocker = taken.find(isInTheWay))) {
      start = blocker.end + gap;
    }
    const end = start + length;
    if (end <= maxOffset && (best === null || start < best.area.start)) {
      best = { side, area: { from, to, start, end } };
    }
  }
  return best;
}

/**
 * Places the tags that label the lines of annotations, so that no tag overlaps
 * another. A tag runs along its line, and goes on the left of the line, or on
 * the right when the left is taken. When both sides are taken, it moves along
 * the line until it clears what is in its way, on the side where it moves the
 * least. Tags of changes are placed before those of releases, and otherwise
 * tags are placed in the order given.
 *
 * Tags keep clear of the obstacles too, where they can. A tag with no room
 * that is clear of the obstacles is placed over them, clear of the other tags.
 *
 * @param {{x: number, length: number, kind: string}[]} tags - The position of
 *   each tag's line, the length of the tag along the line, and the kind of its
 *   annotation.
 * @param {TagBounds} bounds - The space available to the tags.
 * @param {TagArea[]} [obstacles] - The areas that the tags keep clear of where
 *   they can.
 * @returns {(TagPlacement | null)[]} The placement of each tag, in the order
 *   of the tags, or null for a tag that has no room clear of the other tags.
 * @example
 * placeTags(
 *   [
 *     { x: 100, length: 40, kind: "release" },
 *     { x: 105, length: 40, kind: "release" },
 *   ],
 *   { thickness: 10, gap: 5, minX: 0, maxX: 500, maxOffset: 100 },
 * ); // [{ side: "left", offset: 0 }, { side: "right", offset: 0 }]
 */
export function placeTags(tags, bounds, obstacles = []) {
  /** @type {TagArea[]} */
  const placed = [];
  const result = tags.map(() => null);
  const order = tags
    .map((_, index) => index)
    .sort(
      (a, b) =>
        Number(tags[a].kind !== "change") - Number(tags[b].kind !== "change"),
    );
  for (const index of order) {
    const room =
      findRoom(tags[index], bounds, [...obstacles, ...placed]) ??
      findRoom(tags[index], bounds, placed);
    if (room !== null) {
      placed.push(room.area);
      result[index] = { side: room.side, offset: room.area.start };
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

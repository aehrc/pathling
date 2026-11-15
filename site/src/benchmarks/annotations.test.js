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
 * Tests for the annotations shown on the benchmark timelines.
 *
 * @author John Grimes
 */

import { describe, expect, test } from "bun:test";
import annotations from "./annotations.json";
import {
  annotationsBetween,
  interleaveAnnotations,
  lineObstacles,
  placeTags,
} from "./annotations.js";

/**
 * Builds an annotation.
 *
 * @param {string} label - The label.
 * @param {string} date - The ISO 8601 time.
 * @param {string} kind - The kind of annotation.
 * @returns {object} The annotation.
 */
function annotation(label, date, kind = "change") {
  return { label, date, kind };
}

/**
 * Builds a run as stored in the index.
 *
 * @param {string} id - The run ID.
 * @param {string} date - The ISO 8601 time.
 * @returns {object} The run.
 */
function run(id, date) {
  return { id, date, commit: `sha-${id}`, reference: "9.1.0", results: [] };
}

describe("annotationsBetween", () => {
  test("keeps annotations within the range, including its ends", () => {
    const list = [
      annotation("before", "2026-01-01T00:00:00Z"),
      annotation("start", "2026-02-01T00:00:00Z"),
      annotation("middle", "2026-02-15T00:00:00Z"),
      annotation("end", "2026-03-01T00:00:00Z"),
      annotation("after", "2026-04-01T00:00:00Z"),
    ];
    const result = annotationsBetween(
      list,
      "2026-02-01T00:00:00Z",
      "2026-03-01T00:00:00Z",
    );
    expect(result.map((a) => a.label)).toEqual(["start", "middle", "end"]);
  });

  test("compares times rather than strings", () => {
    // An annotation dated with an offset must be placed by the instant it
    // denotes: 05:00+10:00 on 1 February is 19:00 UTC on 31 January, which is
    // before the range, although the string sorts within it.
    const list = [annotation("offset", "2026-02-01T05:00:00+10:00")];
    expect(
      annotationsBetween(list, "2026-02-01T00:00:00Z", "2026-02-02T00:00:00Z"),
    ).toEqual([]);
  });

  test("orders the annotations by date", () => {
    const list = [
      annotation("second", "2026-02-02T00:00:00Z"),
      annotation("first", "2026-02-01T00:00:00Z"),
    ];
    const result = annotationsBetween(
      list,
      "2026-01-01T00:00:00Z",
      "2026-03-01T00:00:00Z",
    );
    expect(result.map((a) => a.label)).toEqual(["first", "second"]);
  });

  test("does not modify the original list", () => {
    const list = [
      annotation("second", "2026-02-02T00:00:00Z"),
      annotation("first", "2026-02-01T00:00:00Z"),
    ];
    annotationsBetween(list, "2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z");
    expect(list[0].label).toBe("second");
  });
});

describe("placeTags", () => {
  // Tags are 10 wide, need a gap of 5 along the line between them, and may
  // occupy the plot from 0 to 500 across and up to 100 along a line.
  const bounds = { thickness: 10, gap: 5, minX: 0, maxX: 500, maxOffset: 100 };

  /**
   * Builds the tag of a release.
   *
   * @param {number} x - The position of the line.
   * @param {number} length - The length of the tag along the line.
   * @returns {object} The tag.
   */
  const release = (x, length = 40) => ({ x, length, kind: "release" });

  test("puts a tag with room at the start of the left of its line", () => {
    expect(placeTags([release(100), release(200)], bounds)).toEqual([
      { side: "left", offset: 0 },
      { side: "left", offset: 0 },
    ]);
  });

  test("puts a tag to the right of its line when the left is taken", () => {
    // The left of the second line, from 95 to 105, is taken by the first tag,
    // which lies from 90 to 100.
    expect(placeTags([release(100), release(105)], bounds)).toEqual([
      { side: "left", offset: 0 },
      { side: "right", offset: 0 },
    ]);
  });

  test("treats tags that touch across the lines as not overlapping", () => {
    expect(placeTags([release(100), release(110)], bounds)).toEqual([
      { side: "left", offset: 0 },
      { side: "left", offset: 0 },
    ]);
  });

  test("moves a tag along its line when both sides are taken", () => {
    // The third tag clears the first two by the gap.
    const tags = [release(100), release(101), release(104)];
    expect(placeTags(tags, bounds)).toEqual([
      { side: "left", offset: 0 },
      { side: "right", offset: 0 },
      { side: "left", offset: 45 },
    ]);
  });

  test("moves a tag along the side where it moves the least", () => {
    // The tag on the right is shorter than the one on the left.
    const tags = [release(100, 60), release(101, 20), release(104, 10)];
    expect(placeTags(tags, bounds)).toEqual([
      { side: "left", offset: 0 },
      { side: "right", offset: 0 },
      { side: "right", offset: 25 },
    ]);
  });

  test("moves a tag past every tag in its way", () => {
    // Five tags on one line stack on alternate sides.
    const tags = Array.from({ length: 5 }, () => release(100, 20));
    expect(placeTags(tags, bounds)).toEqual([
      { side: "left", offset: 0 },
      { side: "right", offset: 0 },
      { side: "left", offset: 25 },
      { side: "right", offset: 25 },
      { side: "left", offset: 50 },
    ]);
  });

  test("leaves a tag out when it would pass the end of its line", () => {
    const tags = [
      release(100),
      release(101),
      release(102),
      release(103),
      release(104),
    ];
    // The fifth tag would lie from 90 to 130, beyond the limit of 100.
    expect(placeTags(tags, bounds)).toEqual([
      { side: "left", offset: 0 },
      { side: "right", offset: 0 },
      { side: "left", offset: 45 },
      { side: "right", offset: 45 },
      null,
    ]);
  });

  test("allows a tag that exactly fills its line", () => {
    expect(placeTags([release(100, 100)], bounds)).toEqual([
      { side: "left", offset: 0 },
    ]);
  });

  test("leaves out a tag that is longer than its line", () => {
    expect(placeTags([release(100, 101)], bounds)).toEqual([null]);
  });

  test("keeps a tag within the left edge", () => {
    // The left of a line at 5 would start at -5.
    expect(placeTags([release(5)], bounds)).toEqual([
      { side: "right", offset: 0 },
    ]);
  });

  test("keeps a tag within the right edge", () => {
    // The right of a line at 495 would end at 505, so the second tag moves
    // along the left instead.
    expect(placeTags([release(495), release(495)], bounds)).toEqual([
      { side: "left", offset: 0 },
      { side: "left", offset: 45 },
    ]);
  });

  test("leaves out a tag with no room on either side", () => {
    const narrow = { ...bounds, minX: 95, maxX: 105 };
    expect(placeTags([release(100)], narrow)).toEqual([null]);
  });

  test("places changes before releases", () => {
    // A change is rarer and more informative than a release, so it takes the
    // start of its line even when an earlier release is in the way.
    const tags = [
      release(100),
      { x: 101, length: 50, kind: "change" },
      release(102),
    ];
    expect(placeTags(tags, bounds)).toEqual([
      { side: "left", offset: 55 },
      { side: "left", offset: 0 },
      { side: "right", offset: 0 },
    ]);
  });

  test("returns no placements for no tags", () => {
    expect(placeTags([], bounds)).toEqual([]);
  });

  test("moves a tag along its line past an obstacle on both sides", () => {
    // The obstacle lies across the line, from 10 to 30 along it.
    const obstacles = [{ from: 80, to: 120, start: 10, end: 30 }];
    expect(placeTags([release(100)], bounds, obstacles)).toEqual([
      { side: "left", offset: 35 },
    ]);
  });

  test("puts a tag on the side that an obstacle leaves clear", () => {
    const obstacles = [{ from: 80, to: 100, start: 0, end: 30 }];
    expect(placeTags([release(100)], bounds, obstacles)).toEqual([
      { side: "right", offset: 0 },
    ]);
  });

  test("leaves a tag before an obstacle that it clears by the gap", () => {
    const obstacles = [{ from: 80, to: 120, start: 45, end: 60 }];
    expect(placeTags([release(100)], bounds, obstacles)).toEqual([
      { side: "left", offset: 0 },
    ]);
  });

  test("moves a tag past an obstacle that it is closer to than the gap", () => {
    const obstacles = [{ from: 80, to: 120, start: 44, end: 55 }];
    expect(placeTags([release(100)], bounds, obstacles)).toEqual([
      { side: "left", offset: 60 },
    ]);
  });

  test("moves a tag past both an obstacle and another tag", () => {
    // The second tag clears the obstacle on the right of its line, and then
    // the first tag, which moved past the same obstacle.
    const obstacles = [{ from: 80, to: 120, start: 0, end: 10 }];
    const tags = [release(100, 20), release(100, 20), release(100, 20)];
    expect(placeTags(tags, bounds, obstacles)).toEqual([
      { side: "left", offset: 15 },
      { side: "right", offset: 15 },
      { side: "left", offset: 40 },
    ]);
  });

  test("puts a tag over an obstacle rather than leaving it out", () => {
    // The tag has no room before the obstacle or after it.
    const obstacles = [{ from: 80, to: 120, start: 30, end: 70 }];
    expect(placeTags([release(100)], bounds, obstacles)).toEqual([
      { side: "left", offset: 0 },
    ]);
  });

  test("keeps a tag that is over an obstacle clear of other tags", () => {
    // The first two tags fit before the obstacle. The third cannot clear it,
    // and so moves past the first tag alone.
    const obstacles = [{ from: 80, to: 120, start: 30, end: 100 }];
    const tags = [release(100, 20), release(100, 20), release(100, 20)];
    expect(placeTags(tags, bounds, obstacles)).toEqual([
      { side: "left", offset: 0 },
      { side: "right", offset: 0 },
      { side: "left", offset: 25 },
    ]);
  });

  test("ignores an obstacle that only touches the side of a tag", () => {
    const obstacles = [
      { from: 80, to: 90, start: 0, end: 100 },
      { from: 100, to: 120, start: 0, end: 100 },
    ];
    expect(placeTags([release(100)], bounds, obstacles)).toEqual([
      { side: "left", offset: 0 },
    ]);
  });

  test("does not modify the obstacles", () => {
    const obstacles = [{ from: 80, to: 120, start: 10, end: 30 }];
    placeTags([release(100), release(101)], bounds, obstacles);
    expect(obstacles).toEqual([{ from: 80, to: 120, start: 10, end: 30 }]);
  });
});

describe("lineObstacles", () => {
  test("covers a short line with one area", () => {
    const areas = lineObstacles({ x: 0, offset: 10 }, { x: 5, offset: 20 }, 10);
    expect(areas).toEqual([{ from: 0, to: 5, start: 10, end: 20 }]);
  });

  test("covers a long line with areas no wider than the limit", () => {
    // Each area covers only the part of the line that crosses it, so that a
    // steep line does not take up the whole of its bounding box.
    const areas = lineObstacles({ x: 0, offset: 0 }, { x: 20, offset: 40 }, 10);
    expect(areas).toEqual([
      { from: 0, to: 10, start: 0, end: 20 },
      { from: 10, to: 20, start: 20, end: 40 },
    ]);
  });

  test("orders the extent of each area along a falling line", () => {
    const areas = lineObstacles({ x: 0, offset: 40 }, { x: 20, offset: 0 }, 10);
    expect(areas).toEqual([
      { from: 0, to: 10, start: 20, end: 40 },
      { from: 10, to: 20, start: 0, end: 20 },
    ]);
  });

  test("divides a line evenly when the limit does not", () => {
    const areas = lineObstacles({ x: 0, offset: 0 }, { x: 25, offset: 50 }, 10);
    expect(areas).toHaveLength(3);
    expect(areas[0].from).toBe(0);
    expect(areas[2].to).toBe(25);
    expect(areas[2].end).toBe(50);
    areas.forEach((area) => expect(area.to - area.from).toBeCloseTo(25 / 3));
  });

  test("covers a line between two points at one position", () => {
    const areas = lineObstacles({ x: 5, offset: 30 }, { x: 5, offset: 0 }, 10);
    expect(areas).toEqual([{ from: 5, to: 5, start: 0, end: 30 }]);
  });

  test("covers a line given from its later end", () => {
    const areas = lineObstacles({ x: 20, offset: 40 }, { x: 0, offset: 0 }, 10);
    expect(areas).toEqual([
      { from: 10, to: 20, start: 20, end: 40 },
      { from: 0, to: 10, start: 0, end: 20 },
    ]);
  });
});

describe("interleaveAnnotations", () => {
  test("merges runs and annotations in date order", () => {
    const runs = [
      run("1", "2026-01-01T00:00:00Z"),
      run("2", "2026-03-01T00:00:00Z"),
    ];
    const list = [annotation("change", "2026-02-01T00:00:00Z")];
    expect(interleaveAnnotations(runs, list)).toEqual([
      { run: runs[0] },
      { annotation: list[0] },
      { run: runs[1] },
    ]);
  });

  test("puts an annotation after a run with the same date", () => {
    // A release is dated by its tagged commit, as is the run that benchmarks
    // it, so the release follows its own run.
    const runs = [run("1", "2026-01-01T00:00:00Z")];
    const list = [annotation("v1.0.0", "2026-01-01T00:00:00Z", "release")];
    expect(interleaveAnnotations(runs, list)).toEqual([
      { run: runs[0] },
      { annotation: list[0] },
    ]);
  });

  test("returns the runs alone when there are no annotations", () => {
    const runs = [run("1", "2026-01-01T00:00:00Z")];
    expect(interleaveAnnotations(runs, [])).toEqual([{ run: runs[0] }]);
  });
});

describe("annotations.json", () => {
  // The annotations are edited by hand, so these checks catch mistakes that
  // would otherwise misplace an annotation or break the page.
  test.each(annotations.map((entry) => [entry.label, entry]))(
    "%s is well formed",
    (_, entry) => {
      expect(Object.keys(entry)).toEqual(
        expect.arrayContaining(["date", "kind", "label"]),
      );
      for (const key of Object.keys(entry)) {
        expect(["date", "kind", "label", "description", "url"]).toContain(key);
      }
      // A date without a zone would be read in the local time of the browser.
      expect(entry.date).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/);
      expect(Number.isNaN(Date.parse(entry.date))).toBe(false);
      expect(["release", "change"]).toContain(entry.kind);
      expect(entry.label.trim()).not.toBe("");
      if (entry.url !== undefined) {
        expect(entry.url).toMatch(/^https:\/\//);
      }
      if (entry.kind === "release") {
        expect(entry.label).toMatch(/^v\d+\.\d+\.\d+$/);
      }
    },
  );

  test("has no duplicate labels", () => {
    const labels = annotations.map((entry) => entry.label);
    expect(new Set(labels).size).toBe(labels.length);
  });
});

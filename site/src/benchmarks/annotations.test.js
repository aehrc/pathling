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
  assignLabelRows,
  interleaveAnnotations,
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

describe("assignLabelRows", () => {
  test("puts labels that do not overlap in the first row", () => {
    const spans = [
      { left: 0, right: 10, kind: "release" },
      { left: 20, right: 30, kind: "release" },
    ];
    expect(assignLabelRows(spans, 3)).toEqual([0, 0]);
  });

  test("treats labels that touch as not overlapping", () => {
    const spans = [
      { left: 0, right: 10, kind: "release" },
      { left: 10, right: 20, kind: "release" },
    ];
    expect(assignLabelRows(spans, 3)).toEqual([0, 0]);
  });

  test("moves an overlapping label to the next free row", () => {
    const spans = [
      { left: 0, right: 10, kind: "release" },
      { left: 5, right: 15, kind: "release" },
      { left: 8, right: 18, kind: "release" },
      { left: 12, right: 22, kind: "release" },
    ];
    // The fourth label clears the first, so it can share the first row.
    expect(assignLabelRows(spans, 3)).toEqual([0, 1, 2, 0]);
  });

  test("leaves a label out when every row is taken", () => {
    const spans = [
      { left: 0, right: 10, kind: "release" },
      { left: 1, right: 11, kind: "release" },
      { left: 2, right: 12, kind: "release" },
    ];
    expect(assignLabelRows(spans, 2)).toEqual([0, 1, null]);
  });

  test("places changes before releases", () => {
    // A change is rarer and more informative than a release, so it keeps its
    // label even when an earlier release overlaps it.
    const spans = [
      { left: 0, right: 10, kind: "release" },
      { left: 5, right: 15, kind: "change" },
      { left: 6, right: 16, kind: "release" },
    ];
    expect(assignLabelRows(spans, 2)).toEqual([1, 0, null]);
  });

  test("returns no rows for no labels", () => {
    expect(assignLabelRows([], 3)).toEqual([]);
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

/**
 * Tests for the index of published benchmark runs.
 *
 * @author John Grimes
 */

import { describe, expect, test } from "bun:test";
import { addRun, buildTimelines } from "./runIndex.js";

/**
 * Builds a run summary as stored in the index.
 *
 * @param {string} id - The run identifier.
 * @param {string} date - The ISO 8601 date the run finished.
 * @param {object[]} results - The summarised results.
 * @returns {object} An index entry.
 */
function run(id, date, results = []) {
  return { id, date, commit: `sha-${id}`, results };
}

/**
 * Builds a summarised result.
 *
 * @param {string} benchmark - The short benchmark name.
 * @param {string} params - The parameter label.
 * @param {number} score - The score.
 * @returns {object} A summarised result.
 */
function summary(benchmark, params, score) {
  return {
    benchmark,
    mode: "avgt",
    params,
    score,
    scoreError: 1,
    scoreUnit: "ms/op",
  };
}

describe("addRun", () => {
  test("creates the run list when the index is empty", () => {
    expect(addRun({}, run("1", "2026-01-01T00:00:00Z"))).toEqual({
      runs: [run("1", "2026-01-01T00:00:00Z")],
    });
  });

  test("keeps runs ordered by date", () => {
    const index = { runs: [run("2", "2026-02-01T00:00:00Z")] };
    const updated = addRun(index, run("1", "2026-01-01T00:00:00Z"));
    expect(updated.runs.map((entry) => entry.id)).toEqual(["1", "2"]);
  });

  test("replaces a run with the same identifier", () => {
    // Publishing the same run twice must not duplicate it.
    const index = { runs: [run("1", "2026-01-01T00:00:00Z")] };
    const replacement = run("1", "2026-01-01T00:00:00Z", [
      summary("A.a", "", 1),
    ]);
    expect(addRun(index, replacement).runs).toEqual([replacement]);
  });

  test("does not modify the original index", () => {
    const index = { runs: [run("2", "2026-02-01T00:00:00Z")] };
    addRun(index, run("1", "2026-01-01T00:00:00Z"));
    expect(index.runs).toHaveLength(1);
  });
});

describe("buildTimelines", () => {
  test("groups results into one timeline per benchmark", () => {
    const index = {
      runs: [
        run("1", "2026-01-01T00:00:00Z", [
          summary("A.b", "x=1", 5),
          summary("A.a", "x=1", 1),
          summary("A.a", "x=2", 2),
        ]),
        run("2", "2026-02-01T00:00:00Z", [
          summary("A.a", "x=1", 3),
          summary("A.a", "x=2", 4),
        ]),
      ],
    };

    const timelines = buildTimelines(index);

    expect(timelines.map((timeline) => timeline.benchmark)).toEqual([
      "A.a",
      "A.b",
    ]);
    const [a, b] = timelines;
    expect(a.unit).toBe("ms/op");
    expect(a.series.map((series) => series.label)).toEqual(["x=1", "x=2"]);
    expect(
      a.series[0].points.map((point) => [point.runId, point.score]),
    ).toEqual([
      ["1", 1],
      ["2", 3],
    ]);
    expect(a.series[0].points[0]).toEqual({
      runId: "1",
      date: "2026-01-01T00:00:00Z",
      commit: "sha-1",
      score: 1,
      scoreError: 1,
    });
    // A benchmark missing from a later run simply has fewer points.
    expect(b.series[0].points).toHaveLength(1);
  });

  test("labels series by mode when a benchmark runs in several modes", () => {
    const index = {
      runs: [
        run("1", "2026-01-01T00:00:00Z", [
          summary("A.a", "", 1),
          { ...summary("A.a", "", 2), mode: "thrpt", scoreUnit: "ops/s" },
        ]),
      ],
    };
    const [timeline] = buildTimelines(index);
    expect(timeline.series.map((series) => series.label)).toEqual([
      "avgt",
      "thrpt",
    ]);
  });

  test("an unparameterised benchmark in one mode has a default label", () => {
    const index = {
      runs: [run("1", "2026-01-01T00:00:00Z", [summary("A.a", "", 1)])],
    };
    expect(buildTimelines(index)[0].series[0].label).toBe("score");
  });

  test("an empty index has no timelines", () => {
    expect(buildTimelines({ runs: [] })).toEqual([]);
  });
});

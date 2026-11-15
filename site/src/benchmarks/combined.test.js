/**
 * Tests for the combined benchmark scores.
 *
 * @author John Grimes
 */

import { describe, expect, test } from "bun:test";
import { buildCombinedTimelines, geometricMean } from "./combined.js";

/**
 * Builds a run summary as stored in the index.
 *
 * @param {string} id - The run identifier.
 * @param {string} date - The ISO 8601 date the run finished.
 * @param {object[]} results - The summarised results.
 * @returns {object} An index entry.
 */
function run(id, date, results = []) {
  return { id, date, commit: `sha-${id}`, reference: "9.1.0", results };
}

/**
 * Builds a summarised result.
 *
 * @param {string} benchmark - The short benchmark name.
 * @param {number} score - The score relative to the reference.
 * @param {number | null} scoreError - The error of the relative score.
 * @returns {object} A summarised result.
 */
function summary(benchmark, score, scoreError = 0.1) {
  return {
    benchmark,
    mode: "avgt",
    params: "",
    score: score * 100,
    scoreError: 1,
    scoreUnit: "ms/op",
    relative: { score, scoreError },
  };
}

/**
 * Finds the combined timeline with the given name.
 *
 * @param {object[]} timelines - The combined timelines.
 * @param {string} name - The name of the timeline.
 * @returns {object | undefined} The timeline.
 */
function find(timelines, name) {
  return timelines.find((timeline) => timeline.benchmark === name);
}

/**
 * Checks the scores of the points of a combined timeline.
 *
 * @param {object} timeline - The combined timeline.
 * @param {number[]} expected - The expected scores, in order.
 */
function expectScores(timeline, expected) {
  const scores = timeline.series[0].points.map((point) => point.score);
  expect(scores).toHaveLength(expected.length);
  scores.forEach((score, i) => expect(score).toBeCloseTo(expected[i], 12));
}

describe("geometricMean", () => {
  test("is the nth root of the product of the scores", () => {
    const { score } = geometricMean([
      { score: 2, scoreError: 0.2 },
      { score: 8, scoreError: 0.4 },
    ]);
    expect(score).toBeCloseTo(4, 12);
  });

  test("propagates the relative errors as if independent", () => {
    // The relative errors are 0.1 and 0.05, so the relative error of the mean
    // is sqrt(0.1^2 + 0.05^2) / 2.
    const { scoreError } = geometricMean([
      { score: 2, scoreError: 0.2 },
      { score: 8, scoreError: 0.4 },
    ]);
    expect(scoreError).toBeCloseTo((4 * Math.sqrt(0.0125)) / 2, 12);
  });

  test("has no error when the error of a score is unknown", () => {
    const combined = geometricMean([
      { score: 2, scoreError: 0.2 },
      { score: 8, scoreError: null },
    ]);
    expect(combined.score).toBeCloseTo(4, 12);
    expect(combined.scoreError).toBeNull();
  });
});

describe("buildCombinedTimelines", () => {
  test("combines the benchmarks of each run into an overall timeline", () => {
    const index = {
      runs: [
        run("1", "2026-01-01T00:00:00Z", [
          summary("PathlingBenchmark.encodePatient", 2),
          summary("PathlingBenchmark.planConditionFlat", 8),
        ]),
        run("2", "2026-02-01T00:00:00Z", [
          summary("PathlingBenchmark.encodePatient", 1),
          summary("PathlingBenchmark.planConditionFlat", 9),
        ]),
      ],
    };

    const timelines = buildCombinedTimelines(index);

    // The overall timeline comes first.
    expect(timelines[0].benchmark).toBe("Overall");
    expect(timelines[0].unit).toBe("relative to 9.1.0");
    expect(timelines[0].series).toHaveLength(1);
    const [first, second] = timelines[0].series[0].points;
    expect(first).toEqual({
      runId: "1",
      date: "2026-01-01T00:00:00Z",
      commit: "sha-1",
      score: expect.any(Number),
      scoreError: expect.any(Number),
    });
    expect(first.score).toBeCloseTo(4, 12);
    expect(second.score).toBeCloseTo(3, 12);
  });

  test("leaves out a benchmark that is missing from any run", () => {
    // The questionnaire response view is not benchmarked in the first run, so
    // including it in later runs would make the combined score jump.
    const index = {
      runs: [
        run("1", "2026-01-01T00:00:00Z", [
          summary("PathlingBenchmark.planConditionFlat", 2),
        ]),
        run("2", "2026-02-01T00:00:00Z", [
          summary("PathlingBenchmark.planConditionFlat", 3),
          summary("PathlingBenchmark.planQuestionnaireResponseFlat", 100),
        ]),
      ],
    };

    const overall = find(buildCombinedTimelines(index), "Overall");

    expectScores(overall, [2, 3]);
  });

  test("a result without a reference result does not count as present", () => {
    const index = {
      runs: [
        run("1", "2026-01-01T00:00:00Z", [
          summary("PathlingBenchmark.planConditionFlat", 2),
          {
            ...summary("PathlingBenchmark.planEncounterFlat", 5),
            relative: null,
          },
        ]),
        run("2", "2026-02-01T00:00:00Z", [
          summary("PathlingBenchmark.planConditionFlat", 3),
          summary("PathlingBenchmark.planEncounterFlat", 7),
        ]),
      ],
    };

    const overall = find(buildCombinedTimelines(index), "Overall");

    expectScores(overall, [2, 3]);
  });

  test("combines the benchmarks of each phase into a timeline", () => {
    const index = {
      runs: [
        run("1", "2026-01-01T00:00:00Z", [
          summary("PathlingBenchmark.encodePatient", 2),
          summary("PathlingBenchmark.encodeCondition", 8),
          summary("PathlingBenchmark.decodePatient", 3),
          summary("PathlingBenchmark.planConditionFlat", 5),
          summary("PathlingBenchmark.executeConditionFlat", 7),
        ]),
      ],
    };

    const timelines = buildCombinedTimelines(index);

    expect(timelines.map((timeline) => timeline.benchmark)).toEqual([
      "Overall",
      "Encode",
      "Decode",
      "Plan",
      "Execute",
    ]);
    const scores = timelines.map(
      (timeline) => timeline.series[0].points[0].score,
    );
    expect(scores[1]).toBeCloseTo(4, 12);
    expect(scores[2]).toBeCloseTo(3, 12);
    expect(scores[3]).toBeCloseTo(5, 12);
    expect(scores[4]).toBeCloseTo(7, 12);
    expect(scores[0]).toBeCloseTo(Math.pow(2 * 8 * 3 * 5 * 7, 1 / 5), 12);
  });

  test("a phase without common benchmarks has no timeline", () => {
    const index = {
      runs: [
        run("1", "2026-01-01T00:00:00Z", [
          summary("PathlingBenchmark.encodePatient", 2),
          summary("PathlingBenchmark.planConditionFlat", 5),
        ]),
        run("2", "2026-02-01T00:00:00Z", [
          summary("PathlingBenchmark.planConditionFlat", 5),
        ]),
      ],
    };

    const timelines = buildCombinedTimelines(index);

    expect(timelines.map((timeline) => timeline.benchmark)).toEqual([
      "Overall",
      "Plan",
    ]);
  });

  test("a benchmark without a known phase counts towards the overall score only", () => {
    const index = {
      runs: [
        run("1", "2026-01-01T00:00:00Z", [
          summary("PathlingBenchmark.conditionFlat", 4),
          summary("PathlingBenchmark.planConditionFlat", 9),
        ]),
      ],
    };

    const timelines = buildCombinedTimelines(index);

    expect(timelines.map((timeline) => timeline.benchmark)).toEqual([
      "Overall",
      "Plan",
    ]);
    expect(timelines[0].series[0].points[0].score).toBeCloseTo(6, 12);
    expect(timelines[1].series[0].points[0].score).toBeCloseTo(9, 12);
  });

  test("the phase is the leading lower-case word of the method name", () => {
    // A class name or a longer word that merely starts with a phase does not
    // give the benchmark a phase.
    const index = {
      runs: [
        run("1", "2026-01-01T00:00:00Z", [
          summary("encode.PatientBenchmark", 2),
          summary("PathlingBenchmark.planner", 3),
        ]),
      ],
    };

    const timelines = buildCombinedTimelines(index);

    expect(timelines.map((timeline) => timeline.benchmark)).toEqual([
      "Overall",
    ]);
  });

  test("there are no combined timelines when no benchmark is in every run", () => {
    const index = {
      runs: [
        run("1", "2026-01-01T00:00:00Z", [
          summary("PathlingBenchmark.encodePatient", 2),
        ]),
        run("2", "2026-02-01T00:00:00Z", [
          summary("PathlingBenchmark.planConditionFlat", 5),
        ]),
      ],
    };

    expect(buildCombinedTimelines(index)).toEqual([]);
  });

  test("an empty index has no combined timelines", () => {
    expect(buildCombinedTimelines({ runs: [] })).toEqual([]);
  });

  test("treats each combination of mode and parameters as a benchmark", () => {
    // The delta variant is missing from the second run, so only the ndjson
    // variant is combined.
    const index = {
      runs: [
        run("1", "2026-01-01T00:00:00Z", [
          {
            ...summary("PathlingBenchmark.plan", 2),
            params: "sourceType=ndjson",
          },
          {
            ...summary("PathlingBenchmark.plan", 50),
            params: "sourceType=delta",
          },
        ]),
        run("2", "2026-02-01T00:00:00Z", [
          {
            ...summary("PathlingBenchmark.plan", 3),
            params: "sourceType=ndjson",
          },
        ]),
      ],
    };

    const overall = find(buildCombinedTimelines(index), "Overall");

    expectScores(overall, [2, 3]);
  });
});

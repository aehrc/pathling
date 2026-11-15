/**
 * Tests for the statistics used to compare benchmark runs. Reference values
 * were computed with scipy.stats (ttest_ind with equal_var=False, and t.cdf).
 *
 * @author John Grimes
 */

import { describe, expect, test } from "bun:test";
import { mean, studentTCdf, variance, welchTTest } from "./stats.js";

describe("mean and variance", () => {
  test("mean of values", () => {
    expect(mean([1, 2, 3, 4])).toBe(2.5);
  });

  test("sample variance uses n - 1 as the divisor", () => {
    expect(variance([1, 2, 3, 4])).toBeCloseTo(1.6666667, 6);
  });
});

describe("studentTCdf", () => {
  // Each case is [t, degrees of freedom, expected CDF].
  test.each([
    [2.0, 5, 0.9490302605850709],
    [-1.5, 3.3, 0.1112515983596459],
    [0.0, 10, 0.5],
    [4.0, 1, 0.9220208696226306],
  ])("CDF of t=%p with df=%p matches scipy", (t, df, expected) => {
    expect(studentTCdf(t, df)).toBeCloseTo(expected, 9);
  });
});

describe("welchTTest", () => {
  // Each case is [sample a, sample b, t statistic, p-value, degrees of freedom].
  test.each([
    [
      [10, 11, 12, 13, 14, 15],
      [12, 13, 14, 15, 16, 17],
      -1.851640199545103,
      0.09379255157922319,
      10.0,
    ],
    [
      [204.9, 210.1, 199.8, 207.3, 202.2, 205.0],
      [204.1, 209.5, 201.0, 206.8, 203.3, 204.4],
      0.017445605445027528,
      0.9864387863892516,
      9.588350874828189,
    ],
    [
      [1, 2, 3, 4],
      [10, 11, 12, 13, 14, 15, 16],
      -10.088073689720488,
      3.6362548409259712e-6,
      8.894736842105265,
    ],
    [
      [5.0, 5.1, 4.9],
      [7.2, 6.8, 7.0, 7.1],
      -19.64538562794299,
      8.251333579146889e-6,
      4.849596022374139,
    ],
  ])("matches scipy for %p vs %p", (a, b, t, p, df) => {
    const result = welchTTest(a, b);
    expect(result.t).toBeCloseTo(t, 9);
    expect(result.df).toBeCloseTo(df, 9);
    // Relative tolerance, as some p-values are very small.
    expect(Math.abs(result.p - p) / p).toBeLessThan(1e-6);
  });

  test("returns null when either sample has fewer than two values", () => {
    // A variance cannot be estimated from a single value.
    expect(welchTTest([1], [1, 2, 3])).toBeNull();
    expect(welchTTest([1, 2, 3], [])).toBeNull();
  });

  test("identical constant samples give a p-value of 1", () => {
    expect(welchTTest([3, 3, 3], [3, 3]).p).toBe(1);
  });

  test("different constant samples give a p-value of 0", () => {
    expect(welchTTest([3, 3, 3], [4, 4]).p).toBe(0);
  });
});

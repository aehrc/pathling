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
 * Statistics used to decide whether two benchmark results differ
 * significantly.
 *
 * @author John Grimes
 */

/**
 * Calculates the arithmetic mean.
 *
 * @param {number[]} values - The values.
 * @returns {number} The mean, or NaN for an empty list.
 */
export function mean(values) {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

/**
 * Calculates the unbiased sample variance (divisor n - 1).
 *
 * @param {number[]} values - The values.
 * @returns {number} The sample variance.
 */
export function variance(values) {
  const m = mean(values);
  return (
    values.reduce((sum, value) => sum + (value - m) ** 2, 0) /
    (values.length - 1)
  );
}

// Lanczos approximation coefficients (g = 7, n = 9).
const LANCZOS = [
  0.99999999999980993, 676.5203681218851, -1259.1392167224028,
  771.32342877765313, -176.61502916214059, 12.507343278686905,
  -0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7,
];

/**
 * Calculates the natural logarithm of the gamma function for x > 0.
 *
 * @param {number} x - The argument.
 * @returns {number} ln(Γ(x)).
 */
function logGamma(x) {
  if (x < 0.5) {
    // Reflection formula.
    return Math.log(Math.PI / Math.sin(Math.PI * x)) - logGamma(1 - x);
  }
  const z = x - 1;
  let sum = LANCZOS[0];
  for (let i = 1; i < LANCZOS.length; i++) {
    sum += LANCZOS[i] / (z + i);
  }
  const t = z + 7.5;
  return (
    0.5 * Math.log(2 * Math.PI) + (z + 0.5) * Math.log(t) - t + Math.log(sum)
  );
}

/**
 * Evaluates the continued fraction for the incomplete beta function using the
 * modified Lentz method.
 *
 * @param {number} x - The upper limit of integration.
 * @param {number} a - The first shape parameter.
 * @param {number} b - The second shape parameter.
 * @returns {number} The value of the continued fraction.
 */
function betaContinuedFraction(x, a, b) {
  const tiny = 1e-300;
  let c = 1;
  let d = 1 - ((a + b) * x) / (a + 1);
  d = 1 / (Math.abs(d) < tiny ? tiny : d);
  let h = d;
  for (let m = 1; m <= 300; m++) {
    const m2 = 2 * m;
    // Even step.
    let aa = (m * (b - m) * x) / ((a + m2 - 1) * (a + m2));
    d = 1 / (Math.abs(1 + aa * d) < tiny ? tiny : 1 + aa * d);
    c = Math.abs(1 + aa / c) < tiny ? tiny : 1 + aa / c;
    h *= d * c;
    // Odd step.
    aa = (-(a + m) * (a + b + m) * x) / ((a + m2) * (a + m2 + 1));
    d = 1 / (Math.abs(1 + aa * d) < tiny ? tiny : 1 + aa * d);
    c = Math.abs(1 + aa / c) < tiny ? tiny : 1 + aa / c;
    const delta = d * c;
    h *= delta;
    if (Math.abs(delta - 1) < 1e-15) {
      break;
    }
  }
  return h;
}

/**
 * Calculates the regularised incomplete beta function I_x(a, b).
 *
 * @param {number} x - The upper limit of integration, between 0 and 1.
 * @param {number} a - The first shape parameter.
 * @param {number} b - The second shape parameter.
 * @returns {number} I_x(a, b).
 */
function regularisedIncompleteBeta(x, a, b) {
  if (x <= 0) {
    return 0;
  }
  if (x >= 1) {
    return 1;
  }
  const front = Math.exp(
    logGamma(a + b) -
      logGamma(a) -
      logGamma(b) +
      a * Math.log(x) +
      b * Math.log(1 - x),
  );
  // The continued fraction converges quickly only on this side of the mean.
  return x < (a + 1) / (a + b + 2)
    ? (front * betaContinuedFraction(x, a, b)) / a
    : 1 - (front * betaContinuedFraction(1 - x, b, a)) / b;
}

/**
 * Calculates the cumulative distribution function of Student's t
 * distribution.
 *
 * @param {number} t - The t statistic.
 * @param {number} df - The degrees of freedom, which need not be an integer.
 * @returns {number} P(T ≤ t).
 */
export function studentTCdf(t, df) {
  const tail = 0.5 * regularisedIncompleteBeta(df / (df + t * t), df / 2, 0.5);
  return t > 0 ? 1 - tail : tail;
}

/**
 * The result of a Welch's t-test.
 *
 * @typedef {object} WelchResult
 * @property {number} t - The t statistic (positive when b has the lower mean).
 * @property {number} df - The Welch-Satterthwaite degrees of freedom.
 * @property {number} p - The two-sided p-value.
 */

/**
 * Performs a two-sided Welch's t-test, which does not assume that the two
 * samples have equal variances.
 *
 * @param {number[]} a - The first sample.
 * @param {number[]} b - The second sample.
 * @returns {WelchResult | null} The test result, or null if either sample has
 *   fewer than two values.
 * @example
 * welchTTest([10, 11, 12], [20, 21, 22]).p; // 0.000...
 */
export function welchTTest(a, b) {
  if (a.length < 2 || b.length < 2) {
    return null;
  }
  const difference = mean(a) - mean(b);
  const se2a = variance(a) / a.length;
  const se2b = variance(b) / b.length;
  const se2 = se2a + se2b;
  if (se2 === 0) {
    // Both samples are constant, so any difference in means is certain.
    return { t: NaN, df: NaN, p: difference === 0 ? 1 : 0 };
  }
  const t = difference / Math.sqrt(se2);
  const df =
    (se2 * se2) /
    ((se2a * se2a) / (a.length - 1) + (se2b * se2b) / (b.length - 1));
  const p = regularisedIncompleteBeta(df / (df + t * t), df / 2, 0.5);
  return { t, df, p };
}

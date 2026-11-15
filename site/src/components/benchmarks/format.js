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
 * Formatting and links shared by the benchmark page components.
 *
 * @author John Grimes
 */

export const REPOSITORY_URL = "https://github.com/aehrc/pathling";

const NUMBER_FORMAT = new Intl.NumberFormat("en-AU", {
  maximumSignificantDigits: 4,
});

/**
 * Formats a score for display.
 *
 * @param {number | null} value - The value.
 * @returns {string} The value to four significant figures, or a dash.
 */
export function formatNumber(value) {
  return Number.isFinite(value) ? NUMBER_FORMAT.format(value) : "-";
}

/**
 * Formats a score with its error.
 *
 * @param {{score: number, scoreError: number | null}} result - The result.
 * @returns {string} For example "12.5 ± 0.4".
 */
export function formatScore({ score, scoreError }) {
  return scoreError === null
    ? formatNumber(score)
    : `${formatNumber(score)} ± ${formatNumber(scoreError)}`;
}

/**
 * Formats a p-value.
 *
 * @param {number | null} p - The p-value.
 * @returns {string} The value to three decimal places, "< 0.001", or a dash.
 */
export function formatPValue(p) {
  if (p === null) {
    return "-";
  }
  return p < 0.001 ? "< 0.001" : p.toFixed(3);
}

/**
 * Formats an ISO 8601 time in UTC.
 *
 * @param {string} date - The time.
 * @returns {string} For example "2026-11-14 05:46 UTC".
 */
export function formatDate(date) {
  return `${new Date(date).toISOString().slice(0, 16).replace("T", " ")} UTC`;
}

/**
 * Builds the label for a run.
 *
 * @param {{date: string, commit: string}} run - The run.
 * @returns {string} The date and short commit SHA.
 */
export function runLabel(run) {
  return `${formatDate(run.date)} (${run.commit.slice(0, 7)})`;
}

/**
 * Builds the GitHub URL of a commit.
 *
 * @param {string} commit - The commit SHA.
 * @returns {string} The URL.
 */
export function commitUrl(commit) {
  return `${REPOSITORY_URL}/commit/${commit}`;
}

/**
 * Builds the GitHub URL of a workflow run.
 *
 * @param {string} runId - The GitHub Actions run ID.
 * @returns {string} The URL.
 */
export function workflowRunUrl(runId) {
  return `${REPOSITORY_URL}/actions/runs/${runId}`;
}

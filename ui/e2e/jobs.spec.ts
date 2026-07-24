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
 * End-to-end tests for the Jobs page.
 *
 * @author John Grimes
 */

import { expect, test } from "@playwright/test";

import { mockMetadata } from "./helpers/mockHelpers";

interface JobPart {
  name: string;
  valueString?: string;
  valueCode?: string;
  valueInteger?: number;
  valueInstant?: string;
  valueUri?: string;
}

/**
 * Builds a $jobs Parameters response body from job part groups.
 *
 * @param jobs - The parts for each job.
 * @returns The Parameters resource as a JSON string.
 */
function jobsBody(jobs: JobPart[][]): string {
  return JSON.stringify({
    resourceType: "Parameters",
    parameter: jobs.map((part) => ({ name: "job", part })),
  });
}

/**
 * Builds the parts for a single export job.
 *
 * @param id - The job id.
 * @param status - The job status code.
 * @param progress - The optional progress percentage.
 * @returns The job parts.
 */
function exportJob(id: string, status: string, progress?: number): JobPart[] {
  const parts: JobPart[] = [
    { name: "id", valueString: id },
    { name: "operation", valueCode: "export" },
    { name: "status", valueCode: status },
    { name: "startTime", valueInstant: "2026-07-24T00:42:11.000Z" },
    { name: "url", valueUri: `http://localhost:3000/fhir/$job?id=${id}` },
  ];
  if (progress !== undefined) {
    parts.push({ name: "progress", valueInteger: progress });
  }
  return parts;
}

test.describe("Jobs page", () => {
  test.beforeEach(async ({ page }) => {
    await mockMetadata(page);
  });

  test("lists jobs and reflects live progress updates", async ({ page }) => {
    // The first poll reports 30%, subsequent polls report 62%, simulating progress advancing.
    let calls = 0;
    await page.route("**/$jobs*", async (route) => {
      calls += 1;
      const progress = calls === 1 ? 30 : 62;
      await route.fulfill({
        status: 200,
        contentType: "application/fhir+json",
        body: jobsBody([exportJob("job-1", "in-progress", progress)]),
      });
    });

    await page.goto("/admin/jobs");

    await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
    await expect(
      page.getByRole("cell", { name: "export", exact: true }),
    ).toBeVisible();
    await expect(page.getByText("In progress")).toBeVisible();
    await expect(page.getByText("30%")).toBeVisible();

    // The list refreshes automatically, so the updated progress appears without interaction.
    await expect(page.getByText("62%")).toBeVisible({ timeout: 10000 });
  });

  test("shows the empty state when there are no jobs", async ({ page }) => {
    await page.route("**/$jobs*", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/fhir+json",
        body: jobsBody([]),
      });
    });

    await page.goto("/admin/jobs");

    await expect(page.getByText("No jobs to show")).toBeVisible();
  });

  test("shows an error state with a working retry action", async ({ page }) => {
    // Fail the first request, then succeed once the user retries.
    let failed = false;
    await page.route("**/$jobs*", async (route) => {
      if (!failed) {
        failed = true;
        await route.fulfill({ status: 500, body: "Server error" });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/fhir+json",
        body: jobsBody([exportJob("job-1", "completed")]),
      });
    });

    await page.goto("/admin/jobs");

    await expect(page.getByText(/could not load jobs/i)).toBeVisible();
    await page.getByRole("button", { name: /retry/i }).click();

    // After retrying, the job appears.
    await expect(page.getByText("Completed")).toBeVisible();
  });
});

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

import { mockCapabilityStatementWithAuth } from "./fixtures/fhirData";
import { mockMetadata } from "./helpers/mockHelpers";

import type { Page } from "@playwright/test";

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

/**
 * Mocks the job DELETE endpoint, invoking a callback when a delete is received.
 *
 * @param page - The Playwright page object.
 * @param status - The HTTP status to return for the delete.
 * @param onDelete - Optional callback invoked when the delete is received.
 */
async function mockJobDelete(
  page: Page,
  status: number,
  onDelete?: () => void,
): Promise<void> {
  await page.route(/\/\$job\?/, async (route) => {
    if (route.request().method() === "DELETE") {
      onDelete?.();
      await route.fulfill({ status, body: "" });
      return;
    }
    await route.fallback();
  });
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

    // The list refreshes automatically, so the advanced progress appears without interaction.
    // (Asserting only the later value avoids racing the poll interval past the initial value.)
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
    // The list request fails, so the page shows the error state with a retry action. (Automatic
    // refetch on focus means a transient error would self-heal, so the failure is kept stable here
    // to assert the error surface deterministically; the retry callback itself is unit tested.)
    await page.route("**/$jobs*", async (route) => {
      await route.fulfill({ status: 500, body: "Server error" });
    });

    await page.goto("/admin/jobs");

    await expect(page.getByText(/could not load jobs/i)).toBeVisible();
    await expect(page.getByRole("button", { name: /retry/i })).toBeVisible();
  });

  test("cancels an in-progress job after confirmation", async ({ page }) => {
    let cancelled = false;
    await page.route("**/$jobs*", async (route) => {
      // Once the DELETE has been issued, the job no longer appears.
      const jobs = cancelled ? [] : [exportJob("job-1", "in-progress", 40)];
      await route.fulfill({
        status: 200,
        contentType: "application/fhir+json",
        body: jobsBody(jobs),
      });
    });
    await mockJobDelete(page, 202, () => {
      cancelled = true;
    });

    await page.goto("/admin/jobs");
    await expect(
      page.getByRole("cell", { name: "export", exact: true }),
    ).toBeVisible();

    // Activating cancel on an in-progress job asks for confirmation.
    await page.getByRole("button", { name: /cancel/i }).click();
    await expect(page.getByRole("alertdialog")).toBeVisible();
    await page.getByRole("button", { name: /cancel job/i }).click();

    // After confirming, the row disappears from the refreshed list.
    await expect(
      page.getByRole("cell", { name: "export", exact: true }),
    ).toBeHidden({ timeout: 10000 });
    await expect(page.getByText("No jobs to show")).toBeVisible();
  });

  test("keeps the row when a cancel request fails", async ({ page }) => {
    await page.route("**/$jobs*", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/fhir+json",
        body: jobsBody([exportJob("job-1", "in-progress", 40)]),
      });
    });
    await mockJobDelete(page, 500);

    await page.goto("/admin/jobs");
    await page.getByRole("button", { name: /cancel/i }).click();
    await page.getByRole("button", { name: /cancel job/i }).click();

    // A failure surfaces a toast and the job remains listed. Radix renders an off-screen
    // announcement copy of the toast, so match the first occurrence.
    await expect(page.getByText(/could not cancel job/i).first()).toBeVisible();
    await expect(
      page.getByRole("cell", { name: "export", exact: true }),
    ).toBeVisible();
  });

  // Regression for #2678: the page used to poll from the moment it loaded, so
  // a logged-out visitor saw a "Session expired" dialog stacked on the login
  // prompt and a stream of failing requests behind it.
  test("shows only the login prompt when logged out, and issues no requests", async ({
    page,
  }) => {
    // The clock is installed before any navigation, so that the page's timers
    // are under the test's control from the outset.
    await page.clock.install();

    await mockMetadata(page, mockCapabilityStatementWithAuth);

    let jobsRequests = 0;
    await page.route("**/$jobs*", async (route) => {
      jobsRequests += 1;
      await route.fulfill({ status: 401, body: "" });
    });

    await page.goto("/admin/jobs");

    await expect(
      page.getByText("You need to login before you can use this page."),
    ).toBeVisible();
    await expect(page.getByRole("alertdialog")).toBeHidden();

    // Advance the clock well past the slow refresh interval to confirm nothing
    // polls behind the prompt.
    await page.clock.runFor(30000);

    expect(jobsRequests).toBe(0);
    await expect(page.getByRole("alertdialog")).toBeHidden();
  });

  // Guards the two assertions that no job list request is issued - the one
  // above, and its counterpart in auth.spec.ts - against passing vacuously.
  // Both rely on advancing a fake clock past the refresh interval, so if
  // installing that clock were to suppress polling rather than drive it, they
  // would hold for the wrong reason. The count must rise across two successive
  // advances, which the flushing of a single pending timer would not satisfy.
  test("keeps polling as the fake clock advances past the refresh interval", async ({
    page,
  }) => {
    await page.clock.install();

    let jobsRequests = 0;
    await page.route("**/$jobs*", async (route) => {
      jobsRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/fhir+json",
        body: jobsBody([exportJob("job-1", "completed")]),
      });
    });

    await page.goto("/admin/jobs");
    await expect(
      page.getByRole("cell", { name: "export", exact: true }),
    ).toBeVisible();

    const afterLoad = jobsRequests;

    await page.clock.runFor(30000);
    await expect.poll(() => jobsRequests).toBeGreaterThan(afterLoad);
    const afterFirstAdvance = jobsRequests;

    await page.clock.runFor(30000);
    await expect.poll(() => jobsRequests).toBeGreaterThan(afterFirstAdvance);
  });

  test("removes a finished job without confirmation", async ({ page }) => {
    let removed = false;
    await page.route("**/$jobs*", async (route) => {
      const jobs = removed ? [] : [exportJob("job-1", "completed")];
      await route.fulfill({
        status: 200,
        contentType: "application/fhir+json",
        body: jobsBody(jobs),
      });
    });
    await mockJobDelete(page, 202, () => {
      removed = true;
    });

    await page.goto("/admin/jobs");
    await expect(
      page.getByRole("cell", { name: "export", exact: true }),
    ).toBeVisible();

    // Removing a finished job takes effect immediately, with no confirmation dialog.
    await page.getByRole("button", { name: /remove/i }).click();
    await expect(page.getByRole("alertdialog")).toBeHidden();
    await expect(
      page.getByRole("cell", { name: "export", exact: true }),
    ).toBeHidden({ timeout: 10000 });
  });
});

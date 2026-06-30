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
 * E2E tests for the `$sqlquery-export` affordance on the SQL on FHIR page: running a query, then
 * exporting its result set asynchronously and downloading an output file. All network is mocked
 * with `page.route`.
 *
 * @author John Grimes
 */

import { expect, test } from "@playwright/test";

import {
  mockCapabilityStatement,
  mockSqlQueryLibrary1,
  mockSqlQueryLibraryBundle,
  mockSqlQueryRunCsv,
  mockViewDefinitionBundle,
} from "./fixtures/fhirData";

import type { Page } from "@playwright/test";

/**
 * Mocks the base endpoints needed to load the page and run a stored SQL query.
 *
 * @param page - The Playwright page.
 */
async function mockBaseEndpoints(page: Page) {
  await page.route("**/metadata", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockCapabilityStatement),
    });
  });

  await page.route(/\/Library\?[^"]*$/, async (route) => {
    const url = route.request().url();
    const body = url.includes("sql-query")
      ? mockSqlQueryLibraryBundle
      : { resourceType: "Bundle", type: "searchset", total: 0, entry: [] };
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(body),
    });
  });

  const viewDefinitions = (route: import("@playwright/test").Route) =>
    route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockViewDefinitionBundle),
    });
  await page.route("**/ViewDefinition?*", viewDefinitions);
  await page.route(/\/ViewDefinition$/, viewDefinitions);

  await page.route("**/$sqlquery-run", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "text/csv",
      body: mockSqlQueryRunCsv,
    });
  });
}

/**
 * Mocks the asynchronous export endpoints: kick-off, status poll (returning the completion
 * manifest), and the output-file download.
 *
 * @param page - The Playwright page.
 */
async function mockExportEndpoints(page: Page) {
  // Kick-off returns 202 with a polling URL.
  await page.route(/\/\$sqlquery-export/, async (route) => {
    await route.fulfill({
      status: 202,
      headers: {
        "Content-Location": "http://localhost:3000/fhir/$job?id=sql-export-1",
        "Access-Control-Expose-Headers": "Content-Location",
      },
      body: "",
    });
  });

  // Status poll returns the completion manifest, with one output file.
  await page.route(/\/\$job/, async (route) => {
    if (route.request().method() === "DELETE") {
      await route.fulfill({ status: 204 });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify({
        resourceType: "Parameters",
        parameter: [
          { name: "exportId", valueString: "sql-export-1" },
          { name: "status", valueCode: "completed" },
          { name: "_format", valueCode: "ndjson" },
          {
            name: "output",
            part: [
              { name: "name", valueString: "people" },
              {
                name: "location",
                valueUri:
                  "http://localhost:3000/fhir/$result?job=sql-export-1&file=people.00000.ndjson",
              },
            ],
          },
        ],
      }),
    });
  });

  // The output-file download.
  await page.route(/\/\$result/, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/octet-stream",
      body: '{"id":"p1","family_name":"Smith"}\n',
    });
  });
}

/**
 * Runs the stored SQL query so the result card renders with rows.
 *
 * @param page - The Playwright page.
 */
async function runStoredQuery(page: Page) {
  await page.goto("/admin/sql-on-fhir");
  await page.getByRole("tab", { name: /^sql query$/i }).click();

  await page.getByRole("combobox", { name: /sql query library/i }).click();
  await page.getByRole("option", { name: mockSqlQueryLibrary1.title }).click();

  // Enter a runtime value for the declared parameter.
  await page
    .getByRole("textbox", { name: /runtime value for patient_id/i })
    .fill("Patient/pat-1");

  // Use CSV so the response branch is deterministic and returns rows.
  await page.getByRole("combobox", { name: /output format/i }).click();
  await page.getByRole("option", { name: "csv" }).click();

  await page.getByRole("button", { name: /^execute$/i }).click();
}

test.describe("SQL on FHIR page - SQL query export", () => {
  test("exports a query result and lists the downloadable output", async ({
    page,
  }) => {
    await mockBaseEndpoints(page);
    await mockExportEndpoints(page);

    await runStoredQuery(page);

    // The run completes and renders a result table.
    await expect(page.getByText("2 rows")).toBeVisible();

    // The result card offers the export affordance.
    await expect(page.getByText(/export full result set/i)).toBeVisible();

    // Start the export.
    await page.getByRole("button", { name: /^export$/i }).click();

    // The export job card completes and lists the output file.
    await expect(page.getByText(/completed/i)).toBeVisible();
    await expect(page.getByText("people.00000.ndjson")).toBeVisible();

    // Downloading the output triggers a browser download.
    const downloadPromise = page.waitForEvent("download");
    await page.getByRole("button", { name: /^download$/i }).click();
    const download = await downloadPromise;
    expect(download.suggestedFilename()).toContain("people.00000.ndjson");
  });
});

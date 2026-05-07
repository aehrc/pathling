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
 * E2E tests for the SQL on FHIR page in `$sqlquery-run` mode.
 *
 * @author John Grimes
 */

import { expect, test } from "@playwright/test";

import {
  mockCapabilityStatement,
  mockEmptySqlQueryLibraryBundle,
  mockSqlQueryLibrary1,
  mockSqlQueryLibraryBundle,
  mockSqlQueryRunCsv,
  mockSqlQueryRunOperationOutcome,
  mockViewDefinitionBundle,
} from "./fixtures/fhirData";

import type { Page } from "@playwright/test";

/**
 * Mocks the `metadata` endpoint without auth requirements.
 * @param page
 */
async function mockMetadata(page: Page) {
  await page.route("**/metadata", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockCapabilityStatement),
    });
  });
}

/**
 * Mocks the Library search endpoint, branching on the type filter so the
 * SQLQuery search returns the SQLQuery bundle while other Library
 * searches return an empty bundle.
 * @param page
 * @param bundle
 */
async function mockSqlQueryLibraries(
  page: Page,
  bundle: object = mockSqlQueryLibraryBundle,
) {
  await page.route(/\/Library\?[^"]*$/, async (route) => {
    const url = route.request().url();
    if (url.includes("sql-query")) {
      await route.fulfill({
        status: 200,
        contentType: "application/fhir+json",
        body: JSON.stringify(bundle),
      });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify({
        resourceType: "Bundle",
        type: "searchset",
        total: 0,
        entry: [],
      }),
    });
  });
}

/**
 * Mocks the ViewDefinition search endpoint with the standard fixture so
 * the inline tab's table picker has options to render.
 * @param page
 */
async function mockViewDefinitions(page: Page) {
  const fulfill = (route: import("@playwright/test").Route) =>
    route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockViewDefinitionBundle),
    });
  await page.route("**/ViewDefinition?*", fulfill);
  await page.route(/\/ViewDefinition$/, fulfill);
}

/**
 * Mocks the `$sqlquery-run` endpoint with a CSV response.
 * @param page
 */
async function mockSqlQueryRunCsvResponse(page: Page) {
  await page.route("**/$sqlquery-run", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "text/csv",
      body: mockSqlQueryRunCsv,
    });
  });
}

/**
 * Mocks the `$sqlquery-run` endpoint with a 400 + OperationOutcome response.
 * @param page
 */
async function mockSqlQueryRunFailure(page: Page) {
  await page.route("**/$sqlquery-run", async (route) => {
    await route.fulfill({
      status: 400,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockSqlQueryRunOperationOutcome),
    });
  });
}

/**
 * Mocks the Library create endpoint to return a created Library.
 * @param page
 */
async function mockSaveSqlQueryLibrary(page: Page) {
  await page.route(/\/Library$/, async (route) => {
    if (route.request().method() === "POST") {
      const created = {
        ...mockSqlQueryLibrary1,
        id: "newly-created-library",
        title: "Inline SQL query",
      };
      await route.fulfill({
        status: 201,
        contentType: "application/fhir+json",
        body: JSON.stringify(created),
      });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockSqlQueryLibraryBundle),
    });
  });
}

/**
 * Switches the page to SQL query mode using the segmented control.
 * @param page
 */
async function selectSqlQueryMode(page: Page) {
  await page.getByRole("tab", { name: /^sql query$/i }).click();
}

test.describe("SQL on FHIR page - SQL query mode", () => {
  test("executes a stored Library and renders the result", async ({ page }) => {
    await mockMetadata(page);
    await mockSqlQueryLibraries(page);
    await mockViewDefinitions(page);
    await mockSqlQueryRunCsvResponse(page);

    await page.goto("/admin/sql-on-fhir");
    await selectSqlQueryMode(page);

    // Pick the stored library.
    await page.getByRole("combobox", { name: /sql query library/i }).click();
    await page
      .getByRole("option", { name: mockSqlQueryLibrary1.title })
      .click();

    // Enter a runtime value for the declared parameter.
    await page
      .getByRole("textbox", { name: /runtime value for patient_id/i })
      .fill("Patient/pat-1");

    // Switch the format to CSV so the response branch is deterministic.
    await page.getByRole("combobox", { name: /output format/i }).click();
    await page.getByRole("option", { name: "csv" }).click();

    await page.getByRole("button", { name: /^execute$/i }).click();

    await expect(page.getByText("2 rows")).toBeVisible();
    await expect(
      page.getByRole("columnheader", { name: "patient_id" }),
    ).toBeVisible();
    await expect(page.getByRole("cell", { name: "Alice" })).toBeVisible();
  });

  test("authors and executes an inline Library", async ({ page }) => {
    await mockMetadata(page);
    await mockSqlQueryLibraries(page, mockEmptySqlQueryLibraryBundle);
    await mockViewDefinitions(page);
    await mockSqlQueryRunCsvResponse(page);

    await page.goto("/admin/sql-on-fhir");
    await selectSqlQueryMode(page);

    // Switch to the Provide Library tab.
    await page.getByRole("tab", { name: /provide sql/i }).click();

    // Author the SQL.
    await page.getByRole("textbox", { name: /^sql$/i }).fill("SELECT 1");

    // Add a table row and select the first ViewDefinition.
    await page.getByRole("button", { name: /add table/i }).click();
    await page
      .getByRole("textbox", { name: /label for table 1/i })
      .fill("patients");
    await page
      .getByRole("combobox", { name: /view definition for table 1/i })
      .click();
    await page.getByRole("option", { name: "Patient Demographics" }).click();

    // Use CSV output so the result rendering is deterministic.
    await page.getByRole("combobox", { name: /output format/i }).click();
    await page.getByRole("option", { name: "csv" }).click();

    await page.getByRole("button", { name: /^execute$/i }).click();

    await expect(page.getByText("2 rows")).toBeVisible();
    await expect(page.getByRole("cell", { name: "pat-1" })).toBeVisible();
  });

  test("saves an inline Library and switches to the picker", async ({
    page,
  }) => {
    await mockMetadata(page);
    await mockSaveSqlQueryLibrary(page);
    await mockViewDefinitions(page);

    await page.goto("/admin/sql-on-fhir");
    await selectSqlQueryMode(page);

    await page.getByRole("tab", { name: /provide sql/i }).click();
    await page
      .getByRole("textbox", { name: /library title/i })
      .fill("Inline SQL query");
    await page.getByRole("textbox", { name: /^sql$/i }).fill("SELECT 1");
    await page.getByRole("button", { name: /add table/i }).click();
    await page
      .getByRole("textbox", { name: /label for table 1/i })
      .fill("patients");
    await page
      .getByRole("combobox", { name: /view definition for table 1/i })
      .click();
    await page.getByRole("option", { name: "Patient Demographics" }).click();

    await page.getByRole("button", { name: /save to server/i }).click();

    // The form switches back to the stored tab and selects the new Library.
    await expect(
      page.getByRole("tab", { name: /select query/i }),
    ).toHaveAttribute("aria-selected", "true");
  });

  test("renders a callout when the server returns 400", async ({ page }) => {
    await mockMetadata(page);
    await mockSqlQueryLibraries(page);
    await mockViewDefinitions(page);
    await mockSqlQueryRunFailure(page);

    await page.goto("/admin/sql-on-fhir");
    await selectSqlQueryMode(page);

    await page.getByRole("combobox", { name: /sql query library/i }).click();
    await page
      .getByRole("option", { name: mockSqlQueryLibrary1.title })
      .click();
    await page
      .getByRole("textbox", { name: /runtime value for patient_id/i })
      .fill("Patient/pat-1");
    await page.getByRole("button", { name: /^execute$/i }).click();

    await expect(
      page.getByText(/sql contains a disallowed operation/i),
    ).toBeVisible();
  });
});

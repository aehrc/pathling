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
 * E2E tests for the SQL on FHIR page.
 *
 * @author John Grimes
 */

import { expect, test } from "@playwright/test";

import {
  mockCapabilityStatement,
  mockCapabilityStatementWithAuth,
  mockEmptyViewDefinitionBundle,
  mockEmptyViewRunNdjson,
  mockViewDefinition1,
  mockViewDefinitionBundle,
  mockViewRunNdjson,
} from "./fixtures/fhirData";

import type { Page } from "@playwright/test";

// =============================================================================
// Composable mock helpers
// =============================================================================

/**
 * Sets up metadata endpoint mock.
 *
 * @param page - The Playwright page object.
 * @param capabilities - The CapabilityStatement to return.
 */
async function mockMetadata(
  page: Page,
  capabilities: object = mockCapabilityStatement,
) {
  await page.route("**/metadata", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(capabilities),
    });
  });
}

/**
 * Sets up ViewDefinition search and single-resource mocks.
 *
 * @param page - The Playwright page object.
 * @param bundle - The Bundle to return for ViewDefinition searches.
 */
async function mockViewDefinitions(
  page: Page,
  bundle: object = mockViewDefinitionBundle,
) {
  await page.route("**/ViewDefinition?*", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(bundle),
    });
  });
  await page.route(/\/ViewDefinition$/, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(bundle),
    });
  });
}

/**
 * Sets up view run endpoint mock with custom response.
 *
 * @param page - The Playwright page object.
 * @param response - The response configuration.
 * @param response.status - HTTP status code (defaults to 200).
 * @param response.body - Response body (string or object).
 * @param response.delayMs - Optional delay before responding.
 */
async function mockViewRun(
  page: Page,
  response: { status?: number; body: string | object; delayMs?: number },
) {
  const { status = 200, body, delayMs } = response;
  const bodyStr = typeof body === "string" ? body : JSON.stringify(body);
  const contentType =
    status === 200 ? "application/x-ndjson" : "application/fhir+json";

  await page.route(/\/ViewDefinition\/[^/]+\/\$run/, async (route) => {
    if (delayMs) await new Promise((resolve) => setTimeout(resolve, delayMs));
    await route.fulfill({ status, contentType, body: bodyStr });
  });

  await page.route("**/ViewDefinition/$run", async (route) => {
    if (delayMs) await new Promise((resolve) => setTimeout(resolve, delayMs));
    await route.fulfill({ status, contentType, body: bodyStr });
  });
}

// =============================================================================
// Composite setup helpers
// =============================================================================

/**
 * Sets up API mocks for standard functionality tests.
 * Mocks capabilities without auth, ViewDefinition search, and view run.
 *
 * @param page - The Playwright page object.
 */
async function setupStandardMocks(page: Page) {
  await mockMetadata(page);
  await mockViewDefinitions(page);
  await mockViewRun(page, { body: mockViewRunNdjson });
}

/**
 * Sets up mocks with delayed view run response to observe in-progress states.
 *
 * @param page - The Playwright page object.
 * @param options - Configuration options for the mock.
 * @param options.delayMs - Delay in milliseconds before responding.
 */
async function setupDelayedViewRunMocks(
  page: Page,
  options: { delayMs?: number } = {},
) {
  const { delayMs = 2000 } = options;
  await mockMetadata(page);
  await mockViewDefinitions(page);
  await mockViewRun(page, { body: mockViewRunNdjson, delayMs });
}

test.describe("SQL on FHIR page", () => {
  test.describe("Initialisation", () => {
    test("loads and displays form with tabs", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Verify the form heading is displayed.
      await expect(
        page.getByRole("heading", { name: "SQL on FHIR" }),
      ).toBeVisible();

      // Verify both tabs are present.
      await expect(
        page.getByRole("tab", { name: "Select view definition" }),
      ).toBeVisible();
      await expect(
        page.getByRole("tab", { name: "Provide JSON" }),
      ).toBeVisible();
    });

    test("shows stored view definition tab by default", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Verify the stored view definition tab is selected.
      await expect(
        page.getByRole("tab", { name: "Select view definition" }),
      ).toHaveAttribute("aria-selected", "true");
    });

    test("shows no query cards initially", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Verify no query cards are displayed before executing a query.
      await expect(
        page.getByText("Executing view definition..."),
      ).not.toBeVisible();
      await expect(
        page.getByRole("columnheader", { name: "patient_id" }),
      ).not.toBeVisible();
    });
  });

  test.describe("Authentication", () => {
    test("shows login prompt when auth required but not authenticated", async ({
      page,
    }) => {
      await mockMetadata(page, mockCapabilityStatementWithAuth);

      await page.goto("/admin/sql-on-fhir");

      // Verify login required message is shown.
      await expect(
        page.getByText("You need to login before you can use this page."),
      ).toBeVisible();
    });
  });

  test.describe("Stored view definition", () => {
    test("loads available view definitions in dropdown", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Open the dropdown.
      await page.getByRole("combobox").click();

      // Verify both view definitions are listed.
      await expect(
        page.getByRole("option", { name: "Patient Demographics" }),
      ).toBeVisible();
      await expect(
        page.getByRole("option", { name: "Observation Vitals" }),
      ).toBeVisible();
    });

    test("shows JSON preview when definition selected", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Select a view definition from the dropdown.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();

      // Verify JSON preview is displayed (contains the resource type).
      await expect(page.getByText('"resourceType"')).toBeVisible();
      await expect(page.getByText('"ViewDefinition"')).toBeVisible();
    });

    test("copies JSON to clipboard when copy button clicked", async ({
      page,
      context,
    }) => {
      // Grant clipboard permissions.
      await context.grantPermissions(["clipboard-read", "clipboard-write"]);

      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Select a view definition from the dropdown.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();

      // Click the copy button.
      await page.getByRole("button", { name: "Copy to clipboard" }).click();

      // Verify the clipboard contains the expected JSON.
      const clipboardText = await page.evaluate(() =>
        navigator.clipboard.readText(),
      );
      const parsedClipboard = JSON.parse(clipboardText);
      expect(parsedClipboard.resourceType).toBe("ViewDefinition");
      expect(parsedClipboard.name).toBe("Patient Demographics");
    });

    test("executes stored definition and displays results", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Select a view definition.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();

      // Click execute.
      await page.getByRole("button", { name: "Execute" }).click();

      // Verify results are displayed (using column headers which are more specific).
      await expect(
        page.getByRole("columnheader", { name: "patient_id" }),
      ).toBeVisible();
      await expect(
        page.getByRole("columnheader", { name: "family_name" }),
      ).toBeVisible();
      await expect(page.getByRole("cell", { name: "Smith" })).toBeVisible();
      await expect(page.getByRole("cell", { name: "Jones" })).toBeVisible();
    });

    test("shows message when no definitions available", async ({ page }) => {
      // Mock with empty ViewDefinition bundle.
      await mockMetadata(page);
      await mockViewDefinitions(page, mockEmptyViewDefinitionBundle);

      await page.goto("/admin/sql-on-fhir");

      // Verify empty state message is shown.
      await expect(
        page.getByText("No view definitions found on the server"),
      ).toBeVisible();
    });
  });

  test.describe("Custom JSON", () => {
    test("switches to custom JSON tab", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Click on the custom JSON tab.
      await page.getByRole("tab", { name: "Provide JSON" }).click();

      // Verify the tab is now selected.
      await expect(
        page.getByRole("tab", { name: "Provide JSON" }),
      ).toHaveAttribute("aria-selected", "true");

      // Verify the label for JSON input is visible.
      await expect(page.getByText("View definition JSON")).toBeVisible();
    });

    test("enables execute button when JSON entered", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Switch to custom JSON tab.
      await page.getByRole("tab", { name: "Provide JSON" }).click();

      // Find the textarea within the custom tab panel.
      const jsonInput = page.locator("textarea").last();
      await jsonInput.fill(
        JSON.stringify({
          resourceType: "ViewDefinition",
          name: "Test View",
          resource: "Patient",
          status: "active",
          select: [{ column: [{ path: "id", name: "id" }] }],
        }),
      );

      // Verify execute button is enabled.
      await expect(page.getByRole("button", { name: "Execute" })).toBeEnabled();
    });

    test("executes custom JSON and displays results", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Switch to custom JSON tab.
      await page.getByRole("tab", { name: "Provide JSON" }).click();

      // Find the textarea within the custom tab panel.
      const jsonInput = page.locator("textarea").last();
      await jsonInput.fill(
        JSON.stringify({
          resourceType: "ViewDefinition",
          name: "Test View",
          resource: "Patient",
          status: "active",
          select: [{ column: [{ path: "id", name: "patient_id" }] }],
        }),
      );

      // Click execute.
      await page.getByRole("button", { name: "Execute" }).click();

      // Verify results are displayed.
      await expect(
        page.getByRole("columnheader", { name: "patient_id" }),
      ).toBeVisible();
      await expect(page.getByRole("cell", { name: "Smith" })).toBeVisible();
    });
  });

  test.describe("Results display", () => {
    test("shows loading state during execution", async ({ page }) => {
      // Set up a delayed response to observe loading state.
      await setupDelayedViewRunMocks(page, { delayMs: 1000 });

      await page.goto("/admin/sql-on-fhir");

      // Select a view definition.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();

      // Click execute.
      await page.getByRole("button", { name: "Execute" }).click();

      // Verify loading state is shown in the query card.
      await expect(
        page.getByText("Executing view definition..."),
      ).toBeVisible();
    });

    test("displays results table with columns and rows", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Select and execute a view definition.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Verify table headers are displayed.
      await expect(
        page.getByRole("columnheader", { name: "patient_id" }),
      ).toBeVisible();
      await expect(
        page.getByRole("columnheader", { name: "family_name" }),
      ).toBeVisible();

      // Verify row data is displayed.
      await expect(page.getByRole("cell", { name: "p1" })).toBeVisible();
      await expect(page.getByRole("cell", { name: "Smith" })).toBeVisible();
      await expect(page.getByRole("cell", { name: "p2" })).toBeVisible();
      await expect(page.getByRole("cell", { name: "Jones" })).toBeVisible();
    });

    test("displays row count badge", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Select and execute a view definition.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Wait for results and verify the count badge.
      await expect(page.getByText("2 rows")).toBeVisible();
    });

    test("shows empty results message when no rows", async ({ page }) => {
      await mockMetadata(page);
      await mockViewDefinitions(page);
      await mockViewRun(page, { body: mockEmptyViewRunNdjson });

      await page.goto("/admin/sql-on-fhir");

      // Select and execute a view definition.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Verify empty results message.
      await expect(page.getByText("No rows returned")).toBeVisible();
    });

    test("displays error message on execution failure", async ({ page }) => {
      const errorOutcome = {
        resourceType: "OperationOutcome",
        issue: [{ severity: "error", diagnostics: "Invalid view definition" }],
      };

      await mockMetadata(page);
      await mockViewDefinitions(page);
      await mockViewRun(page, { status: 400, body: errorOutcome });

      await page.goto("/admin/sql-on-fhir");

      // Select and execute a view definition.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Verify error message is displayed.
      await expect(page.getByText(/View run failed/)).toBeVisible();
    });
  });

  test.describe("Export", () => {
    test("shows output files after export completes", async ({ page }) => {
      // Use setupStandardMocks for base setup.
      await setupStandardMocks(page);

      // Mock reading ViewDefinition by ID (needed before export starts).
      await page.route(/\/ViewDefinition\/[^/]+$/, async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockViewDefinition1),
        });
      });

      // Mock the export kick-off endpoint using regex to match $viewdefinition-export.
      await page.route(/\/\$viewdefinition-export/, async (route) => {
        await route.fulfill({
          status: 202,
          headers: {
            "Content-Location":
              "http://localhost:3000/fhir/$job?id=test-job-123",
            "Access-Control-Expose-Headers": "Content-Location",
          },
          body: "",
        });
      });

      // Mock the job status endpoint using regex - return completed with manifest.
      await page.route(/\/\$job/, async (route) => {
        if (route.request().method() === "GET") {
          await route.fulfill({
            status: 200,
            contentType: "application/fhir+json",
            body: JSON.stringify({
              resourceType: "Parameters",
              parameter: [
                {
                  name: "transactionTime",
                  valueInstant: "2025-01-01T00:00:00Z",
                },
                { name: "requiresAccessToken", valueBoolean: false },
                {
                  name: "output",
                  part: [
                    { name: "name", valueString: "patient_demographics" },
                    {
                      name: "url",
                      valueUri:
                        "http://localhost:3000/fhir/$result?job=test-job-123&file=patient_demographics.ndjson",
                    },
                  ],
                },
              ],
            }),
          });
        } else if (route.request().method() === "DELETE") {
          await route.fulfill({ status: 204 });
        }
      });

      await page.goto("/admin/sql-on-fhir");

      // Select and execute a view definition.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Wait for results to load.
      await expect(page.getByRole("cell", { name: "Smith" })).toBeVisible();

      // Click export button.
      await page.getByRole("button", { name: "Export" }).click();

      // Verify export completed and output file is listed.
      await expect(page.getByText("Completed")).toBeVisible();
      await expect(page.getByText("patient_demographics.ndjson")).toBeVisible();
      await expect(
        page.getByRole("button", { name: "Download" }),
      ).toBeVisible();
    });
  });

  test.describe("Multiple queries", () => {
    test("form remains enabled after starting query", async ({ page }) => {
      await setupDelayedViewRunMocks(page, { delayMs: 5000 });
      await page.goto("/admin/sql-on-fhir");

      // Select and execute a view definition.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Wait for query card to appear with loading state.
      await expect(page.getByText("Executing view definition...")).toBeVisible({
        timeout: 10000,
      });

      // Verify form is still enabled (Execute button is clickable).
      await expect(page.getByRole("button", { name: "Execute" })).toBeEnabled();
    });

    test("starting second query creates additional result card", async ({
      page,
    }) => {
      await setupDelayedViewRunMocks(page, { delayMs: 5000 });
      await page.goto("/admin/sql-on-fhir");

      // Start first query.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Wait for first query card to appear.
      await expect(page.getByText("Executing view definition...")).toBeVisible({
        timeout: 10000,
      });

      // Start second query.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Observation Vitals" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Verify both query cards are visible (2 cards with loading state).
      const loadingCards = page.getByText("Executing view definition...");
      await expect(loadingCards).toHaveCount(2);
    });

    test("query card displays timestamp", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Execute a query.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Wait for query to complete.
      await expect(page.getByRole("cell", { name: "Smith" })).toBeVisible({
        timeout: 10000,
      });

      // Verify timestamp is displayed (matches time format like "10:30:45").
      await expect(page.getByText(/\d{1,2}:\d{2}:\d{2}/)).toBeVisible();
    });

    test("most recent query appears first", async ({ page }) => {
      await setupDelayedViewRunMocks(page, { delayMs: 5000 });
      await page.goto("/admin/sql-on-fhir");

      // Start first query (stored view definition).
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Wait for first query card to appear.
      await expect(
        page.getByText("Run stored view definition").first(),
      ).toBeVisible({ timeout: 10000 });

      // Switch to custom JSON tab and start second query.
      await page.getByRole("tab", { name: "Provide JSON" }).click();
      const jsonInput = page.locator("textarea").last();
      await jsonInput.fill(
        JSON.stringify({
          resourceType: "ViewDefinition",
          name: "Test View",
          resource: "Patient",
          status: "active",
          select: [{ column: [{ path: "id", name: "id" }] }],
        }),
      );
      await page.getByRole("button", { name: "Execute" }).click();

      // Verify Provide JSON card appears before Select view definition card.
      // Use nth(2) to skip the tab and its nested text, getting the card labels.
      const provideJsonBox = await page
        .getByText("Run provided view definition")
        .boundingBox();
      const selectViewBox = await page
        .getByText("Run stored view definition")
        .boundingBox();
      expect(provideJsonBox!.y).toBeLessThan(selectViewBox!.y);
    });
  });

  test.describe("Close button", () => {
    test("close button visible when query is complete", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Execute a query.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Wait for query to complete.
      await expect(page.getByRole("cell", { name: "Smith" })).toBeVisible({
        timeout: 10000,
      });

      // Verify close button is visible.
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
    });

    test("clicking close button removes completed query card", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Execute a query.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Wait for query to complete.
      await expect(page.getByRole("cell", { name: "Smith" })).toBeVisible({
        timeout: 10000,
      });

      // Click close button.
      await page.getByRole("button", { name: "Close" }).click();

      // Verify the query card is removed.
      await expect(page.getByRole("cell", { name: "Smith" })).not.toBeVisible();
    });

    test("close button visible when query errors", async ({ page }) => {
      const errorOutcome = {
        resourceType: "OperationOutcome",
        issue: [{ severity: "error", diagnostics: "Query failed" }],
      };

      await mockMetadata(page);
      await mockViewDefinitions(page);
      await mockViewRun(page, { status: 500, body: errorOutcome });

      await page.goto("/admin/sql-on-fhir");

      // Execute a query.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Wait for error to appear.
      await expect(page.getByText(/View run failed/)).toBeVisible({
        timeout: 10000,
      });

      // Verify close button is visible.
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
    });

    test("clicking close button removes errored query card", async ({
      page,
    }) => {
      const errorOutcome = {
        resourceType: "OperationOutcome",
        issue: [{ severity: "error", diagnostics: "Query failed" }],
      };

      await mockMetadata(page);
      await mockViewDefinitions(page);
      await mockViewRun(page, { status: 500, body: errorOutcome });

      await page.goto("/admin/sql-on-fhir");

      // Execute a query.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Wait for error and close button.
      await expect(page.getByText(/View run failed/)).toBeVisible({
        timeout: 10000,
      });
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();

      // Click close button.
      await page.getByRole("button", { name: "Close" }).click();

      // Verify the query card is removed.
      await expect(page.getByText(/View run failed/)).not.toBeVisible();
    });
  });
});

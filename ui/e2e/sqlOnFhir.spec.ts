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
  mockViewDefinitionBundle,
  mockViewRunNdjson,
} from "./fixtures/fhirData";

/**
 * Sets up API mocks for standard functionality tests.
 * Mocks capabilities without auth, ViewDefinition search, and view run.
 */
async function setupStandardMocks(page: import("@playwright/test").Page) {
  // Mock the metadata endpoint.
  await page.route("**/metadata", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockCapabilityStatement),
    });
  });

  // Mock ViewDefinition search endpoint.
  await page.route("**/ViewDefinition?*", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockViewDefinitionBundle),
    });
  });

  // Also match ViewDefinition without query params.
  await page.route(/\/ViewDefinition$/, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockViewDefinitionBundle),
    });
  });

  // Mock view run endpoints (both stored and inline).
  await page.route("**/ViewDefinition/$run", async (route) => {
    // POST request for inline view run.
    await route.fulfill({
      status: 200,
      contentType: "application/x-ndjson",
      body: mockViewRunNdjson,
    });
  });

  await page.route(/\/ViewDefinition\/[^/]+\/\$run/, async (route) => {
    // GET request for stored view run.
    await route.fulfill({
      status: 200,
      contentType: "application/x-ndjson",
      body: mockViewRunNdjson,
    });
  });
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

    test("shows initial results placeholder", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/sql-on-fhir");

      // Verify the results placeholder is shown.
      await expect(
        page.getByText("Execute a view definition to view results"),
      ).toBeVisible();
    });
  });

  test.describe("Authentication", () => {
    test("shows login prompt when auth required but not authenticated", async ({
      page,
    }) => {
      // Mock capabilities with auth required.
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatementWithAuth),
        });
      });

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
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/ViewDefinition?*", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockEmptyViewDefinitionBundle),
        });
      });

      await page.route(/\/ViewDefinition$/, async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockEmptyViewDefinitionBundle),
        });
      });

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
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/ViewDefinition?*", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockViewDefinitionBundle),
        });
      });

      await page.route(/\/ViewDefinition$/, async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockViewDefinitionBundle),
        });
      });

      await page.route(/\/ViewDefinition\/[^/]+\/\$run/, async (route) => {
        // Delay the response.
        await new Promise((resolve) => setTimeout(resolve, 500));
        await route.fulfill({
          status: 200,
          contentType: "application/x-ndjson",
          body: mockViewRunNdjson,
        });
      });

      await page.goto("/admin/sql-on-fhir");

      // Select a view definition.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();

      // Click execute.
      await page.getByRole("button", { name: "Execute" }).click();

      // Verify loading state is shown.
      await expect(
        page.getByRole("button", { name: "Executing..." }),
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
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/ViewDefinition?*", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockViewDefinitionBundle),
        });
      });

      await page.route(/\/ViewDefinition$/, async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockViewDefinitionBundle),
        });
      });

      await page.route(/\/ViewDefinition\/[^/]+\/\$run/, async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/x-ndjson",
          body: mockEmptyViewRunNdjson,
        });
      });

      await page.goto("/admin/sql-on-fhir");

      // Select and execute a view definition.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Verify empty results message.
      await expect(page.getByText("No rows returned")).toBeVisible();
    });

    test("displays error message on execution failure", async ({ page }) => {
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/ViewDefinition?*", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockViewDefinitionBundle),
        });
      });

      await page.route(/\/ViewDefinition$/, async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockViewDefinitionBundle),
        });
      });

      await page.route(/\/ViewDefinition\/[^/]+\/\$run/, async (route) => {
        await route.fulfill({
          status: 400,
          contentType: "application/fhir+json",
          body: JSON.stringify({
            resourceType: "OperationOutcome",
            issue: [
              { severity: "error", diagnostics: "Invalid view definition" },
            ],
          }),
        });
      });

      await page.goto("/admin/sql-on-fhir");

      // Select and execute a view definition.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Patient Demographics" }).click();
      await page.getByRole("button", { name: "Execute" }).click();

      // Verify error message is displayed.
      await expect(page.getByText(/View run failed/)).toBeVisible();
    });
  });
});

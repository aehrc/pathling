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
 * E2E tests for the Resources page.
 *
 * @author John Grimes
 */

import { expect, test } from "@playwright/test";

import {
  mockCapabilityStatementWithAuth,
  mockEmptyBundle,
  mockPatientBundle,
} from "./fixtures/fhirData";
import { mockMetadata, createOperationOutcome } from "./helpers/mockHelpers";

/**
 * Sets up API mocks for standard functionality tests.
 * Mocks capabilities without auth and provides patient search results.
 *
 * @param page - The Playwright page object.
 */
async function setupStandardMocks(page: import("@playwright/test").Page) {
  await mockMetadata(page);

  // Mock resource search and delete endpoints (matches any resource type).
  await page.route(
    /\/(Patient|Observation|Condition)(\?|\/|$)/,
    async (route) => {
      if (route.request().method() === "GET") {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockPatientBundle),
        });
      } else if (route.request().method() === "DELETE") {
        await route.fulfill({ status: 204 });
      }
    },
  );
}

test.describe("Resources page", () => {
  test.describe("Initialisation", () => {
    test("loads and displays search form", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      // Verify the search form is displayed.
      await expect(
        page.getByRole("heading", { name: "Search resources" }),
      ).toBeVisible();
      await expect(page.getByRole("combobox")).toBeVisible();
      await expect(page.getByText("FHIRPath filters")).toBeVisible();
      await expect(page.getByRole("button", { name: "Search" })).toBeVisible();
    });

    test("shows initial results placeholder", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      // Verify the placeholder text is shown before searching.
      await expect(
        page.getByText("Select a resource type and search to view results"),
      ).toBeVisible();
    });

    test("populates resource types from capabilities", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      // Open the resource type dropdown.
      await page.getByRole("combobox").click();

      // Verify resource types from the mock are available.
      await expect(page.getByRole("option", { name: "Patient" })).toBeVisible();
      await expect(
        page.getByRole("option", { name: "Observation" }),
      ).toBeVisible();
      await expect(
        page.getByRole("option", { name: "Condition" }),
      ).toBeVisible();
    });
  });

  test.describe("Authentication", () => {
    test("shows login prompt when auth required but not authenticated", async ({
      page,
    }) => {
      await mockMetadata(page, mockCapabilityStatementWithAuth);

      await page.goto("/admin/resources");

      // Verify login required message is shown.
      await expect(
        page.getByText("You need to login before you can use this page."),
      ).toBeVisible();
    });
  });

  test.describe("Search functionality", () => {
    test("executes search and displays results", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      // Click search button.
      await page.getByRole("button", { name: "Search" }).click();

      // Verify results are displayed.
      await expect(page.getByText("patient-123")).toBeVisible();
      await expect(page.getByText("patient-456")).toBeVisible();
    });

    test("shows loading state during search", async ({ page }) => {
      // Set up a delayed response to observe loading state.
      await mockMetadata(page);

      await page.route(
        /\/(Patient|Observation|Condition)(\?|\/|$)/,
        async (route) => {
          // Delay the response to allow observing loading state.
          await new Promise((resolve) => setTimeout(resolve, 500));
          await route.fulfill({
            status: 200,
            contentType: "application/fhir+json",
            body: JSON.stringify(mockPatientBundle),
          });
        },
      );

      await page.goto("/admin/resources");
      await page.getByRole("button", { name: "Search" }).click();

      // Verify loading state is shown.
      await expect(
        page.getByRole("button", { name: "Searching..." }),
      ).toBeVisible();
      await expect(page.getByText("Searching...").last()).toBeVisible();
    });

    test("displays result count badge", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      await page.getByRole("button", { name: "Search" }).click();

      // Wait for results and verify the count badge.
      await expect(page.getByText("25 total, first 2 shown")).toBeVisible();
    });

    test("displays empty results message", async ({ page }) => {
      await mockMetadata(page);
      await page.route(
        /\/(Patient|Observation|Condition)(\?|\/|$)/,
        async (route) => {
          await route.fulfill({
            status: 200,
            contentType: "application/fhir+json",
            body: JSON.stringify(mockEmptyBundle),
          });
        },
      );

      await page.goto("/admin/resources");
      await page.getByRole("button", { name: "Search" }).click();

      // Verify empty results message.
      await expect(
        page.getByText("No resources found matching your criteria"),
      ).toBeVisible();
    });

    test("displays error message on search failure", async ({ page }) => {
      await mockMetadata(page);
      await page.route(
        /\/(Patient|Observation|Condition)(\?|\/|$)/,
        async (route) => {
          await route.fulfill({
            status: 400,
            contentType: "application/fhir+json",
            body: createOperationOutcome("Invalid filter"),
          });
        },
      );

      await page.goto("/admin/resources");
      await page.getByRole("button", { name: "Search" }).click();

      // Verify error message is displayed (error message includes "Search failed").
      await expect(page.getByText("Search failed")).toBeVisible();
    });
  });

  test.describe("Filter management", () => {
    test("adds filter input", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      // Initially there should be one filter input.
      const filterInputs = page.getByPlaceholder("e.g., gender = 'female'");
      await expect(filterInputs).toHaveCount(1);

      // Click add filter button.
      await page.getByRole("button", { name: "Add filter" }).click();

      // Verify a second filter input appears.
      await expect(filterInputs).toHaveCount(2);
    });

    test("removes filter input", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      // Add a second filter.
      await page.getByRole("button", { name: "Add filter" }).click();
      const filterInputs = page.getByPlaceholder("e.g., gender = 'female'");
      await expect(filterInputs).toHaveCount(2);

      // Remove one filter by clicking the remove button (X icon next to filter input).
      // The input is wrapped: Flex > Box > TextField.Root > input
      // Go up 3 levels from input to Flex, then find the button.
      const firstFilterRow = filterInputs.first().locator("../../..");
      const removeButton = firstFilterRow.locator("button").first();
      await removeButton.click();

      // Verify only one filter remains.
      await expect(filterInputs).toHaveCount(1);
    });

    test("cannot remove last filter", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      // With only one filter, the remove button should be disabled.
      // The input is wrapped: Flex > Box > TextField.Root > input
      const filterInput = page.getByPlaceholder("e.g., gender = 'female'");
      const filterRow = filterInput.locator("../../..");
      const removeButton = filterRow.locator("button").first();
      await expect(removeButton).toBeDisabled();
    });

    test("submits search with Enter key", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      // Type in the filter input and press Enter.
      await page
        .getByPlaceholder("e.g., gender = 'female'")
        .fill("gender = 'male'");
      await page.getByPlaceholder("e.g., gender = 'female'").press("Enter");

      // Verify search was executed by checking for results.
      await expect(page.getByText("patient-123")).toBeVisible();
    });
  });

  test.describe("Resource cards", () => {
    test("displays resource type badge and ID", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      await page.getByRole("button", { name: "Search" }).click();

      // Verify resource type badge and ID are shown.
      await expect(page.getByText("Patient").first()).toBeVisible();
      await expect(page.getByText("patient-123")).toBeVisible();
    });

    test("displays resource summary", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      await page.getByRole("button", { name: "Search" }).click();

      // Verify patient name summary is displayed.
      await expect(page.getByText("John William Smith")).toBeVisible();
      await expect(page.getByText("Jane Jones")).toBeVisible();
    });

    test("expands to show JSON", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      await page.getByRole("button", { name: "Search" }).click();
      await expect(page.getByText("patient-123")).toBeVisible();

      // JSON should not be visible initially.
      await expect(
        page.getByText('"resourceType": "Patient"'),
      ).not.toBeVisible();

      // Click the resource ID to expand the card.
      await page.getByText("patient-123").click();

      // Verify JSON is now visible.
      await expect(
        page.getByText('"resourceType": "Patient"').first(),
      ).toBeVisible();
    });

    test("collapses JSON view", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      await page.getByRole("button", { name: "Search" }).click();
      await expect(page.getByText("patient-123")).toBeVisible();

      // Click the resource ID to expand the card.
      const resourceId = page.getByText("patient-123", { exact: true });
      await resourceId.click();
      await expect(
        page.getByText('"resourceType": "Patient"').first(),
      ).toBeVisible();

      // Collapse by clicking again.
      await resourceId.click();

      // Verify JSON is hidden.
      await expect(
        page.getByText('"resourceType": "Patient"'),
      ).not.toBeVisible();
    });

    test("copies JSON to clipboard", async ({ page, context }) => {
      // Grant clipboard permissions.
      await context.grantPermissions(["clipboard-read", "clipboard-write"]);

      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      await page.getByRole("button", { name: "Search" }).click();
      await expect(page.getByText("patient-123")).toBeVisible();

      // Click the resource ID to expand the card.
      await page.getByText("patient-123").click();

      // Wait for JSON to be visible.
      await expect(
        page.getByText('"resourceType": "Patient"').first(),
      ).toBeVisible();

      // Click the copy button (inside the expanded code box).
      await page
        .getByRole("button", { name: "Copy to clipboard" })
        .first()
        .click();

      // Verify clipboard content.
      const clipboardText = await page.evaluate(() =>
        navigator.clipboard.readText(),
      );
      expect(clipboardText).toContain('"resourceType": "Patient"');
      expect(clipboardText).toContain('"id": "patient-123"');
    });
  });

  test.describe("Delete flow", () => {
    /**
     * Helper to get the delete button for the first resource card.
     * The delete button is the 2nd button (index 1) within the card.
     *
     * @param page - The Playwright page object.
     * @returns The delete button locator.
     */
    function getDeleteButton(page: import("@playwright/test").Page) {
      const firstCard = page.getByText("patient-123").locator("../../..");
      return firstCard.locator("button").nth(1);
    }

    test("opens delete confirmation dialog", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      await page.getByRole("button", { name: "Search" }).click();
      await expect(page.getByText("patient-123")).toBeVisible();

      // Click delete button on first card (trash icon, 2nd button in card).
      await getDeleteButton(page).click();

      // Verify dialog is open.
      await expect(page.getByRole("alertdialog")).toBeVisible();
      await expect(
        page.getByRole("alertdialog").getByText("Delete resource"),
      ).toBeVisible();
    });

    test("dialog shows resource details", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      await page.getByRole("button", { name: "Search" }).click();
      await expect(page.getByText("patient-123")).toBeVisible();
      await getDeleteButton(page).click();

      // Verify dialog shows resource type and ID.
      await expect(page.getByText("Patient/patient-123")).toBeVisible();
      // Verify dialog shows summary.
      await expect(
        page.getByRole("alertdialog").getByText("John William Smith"),
      ).toBeVisible();
    });

    test("cancelling closes dialog", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      await page.getByRole("button", { name: "Search" }).click();
      await expect(page.getByText("patient-123")).toBeVisible();
      await getDeleteButton(page).click();

      // Click cancel button.
      await page.getByRole("button", { name: "Cancel" }).click();

      // Verify dialog is closed.
      await expect(page.getByRole("alertdialog")).not.toBeVisible();

      // Verify resource is still in the list.
      await expect(page.getByText("patient-123")).toBeVisible();
    });

    test("confirming deletes resource", async ({ page }) => {
      let deleteRequested = false;

      await mockMetadata(page);
      await page.route(
        /\/(Patient|Observation|Condition)(\?|\/|$)/,
        async (route) => {
          if (route.request().method() === "DELETE") {
            deleteRequested = true;
            await route.fulfill({ status: 204 });
          } else {
            await route.fulfill({
              status: 200,
              contentType: "application/fhir+json",
              body: JSON.stringify(mockPatientBundle),
            });
          }
        },
      );

      await page.goto("/admin/resources");
      await page.getByRole("button", { name: "Search" }).click();
      await expect(page.getByText("patient-123")).toBeVisible();
      await getDeleteButton(page).click();

      // Click delete button in dialog.
      await page
        .getByRole("alertdialog")
        .getByRole("button", { name: "Delete" })
        .click();

      // Wait for dialog to close.
      await expect(page.getByRole("alertdialog")).not.toBeVisible();

      // Verify delete was called.
      expect(deleteRequested).toBe(true);
    });

    test("shows success toast after deletion", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/resources");

      await page.getByRole("button", { name: "Search" }).click();
      await expect(page.getByText("patient-123")).toBeVisible();
      await getDeleteButton(page).click();
      await page
        .getByRole("alertdialog")
        .getByRole("button", { name: "Delete" })
        .click();

      // Verify success toast appears in the notifications region.
      const notifications = page.getByRole("region", { name: "Notifications" });
      await expect(notifications.getByText("Resource deleted")).toBeVisible();
    });

    test("shows error toast on delete failure", async ({ page }) => {
      await mockMetadata(page);
      await page.route(
        /\/(Patient|Observation|Condition)(\?|\/|$)/,
        async (route) => {
          await (route.request().method() === "DELETE"
            ? route.fulfill({
                status: 500,
                contentType: "application/fhir+json",
                body: createOperationOutcome("Internal error"),
              })
            : route.fulfill({
                status: 200,
                contentType: "application/fhir+json",
                body: JSON.stringify(mockPatientBundle),
              }));
        },
      );

      await page.goto("/admin/resources");
      await page.getByRole("button", { name: "Search" }).click();
      await expect(page.getByText("patient-123")).toBeVisible();
      await getDeleteButton(page).click();
      await page
        .getByRole("alertdialog")
        .getByRole("button", { name: "Delete" })
        .click();

      // Verify error toast appears in the notifications region.
      const notifications = page.getByRole("region", { name: "Notifications" });
      await expect(
        notifications.getByText("Delete failed", { exact: true }),
      ).toBeVisible();
    });
  });
});

/**
 * E2E tests for the Import page.
 *
 * @author John Grimes
 */

import { expect, test } from "@playwright/test";

import {
  mockCapabilityStatement,
  mockCapabilityStatementWithAuth,
  mockJobStatusComplete,
  mockJobStatusInProgress,
} from "./fixtures/fhirData";

const TEST_JOB_ID = "test-job-123";

/**
 * Sets up standard API mocks for import page tests.
 * Mocks capabilities without auth and provides immediate job completion.
 *
 * @param page - The Playwright page object.
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

  // Mock the import-pnp kick-off endpoint (register first so it matches before $import).
  await page.route("**/$import-pnp", async (route) => {
    await route.fulfill({
      status: 202,
      headers: {
        "Content-Location": `http://localhost:3000/fhir/$job?id=${TEST_JOB_ID}`,
        "Access-Control-Expose-Headers": "Content-Location",
      },
      body: "",
    });
  });

  // Mock the standard import kick-off endpoint.
  await page.route("**/$import", async (route) => {
    await route.fulfill({
      status: 202,
      headers: {
        "Content-Location": `http://localhost:3000/fhir/$job?id=${TEST_JOB_ID}`,
        "Access-Control-Expose-Headers": "Content-Location",
      },
      body: "",
    });
  });

  // Mock job status to return complete immediately.
  await page.route("**/$job*", async (route) => {
    if (route.request().method() === "GET") {
      await route.fulfill({
        status: 200,
        contentType: "application/fhir+json",
        body: JSON.stringify(mockJobStatusComplete),
      });
    } else if (route.request().method() === "DELETE") {
      await route.fulfill({ status: 204 });
    }
  });
}

/**
 * Sets up mocks with delayed job completion to observe progress states.
 *
 * @param page - The Playwright page object.
 * @param options - Configuration options for the mock.
 * @param options.pollCount - Number of poll attempts before job completes.
 * @param options.progress - Progress string to return during polling.
 */
async function setupDelayedJobMocks(
  page: import("@playwright/test").Page,
  options: {
    pollCount?: number;
    progress?: string;
  } = {},
) {
  const { pollCount = 2, progress = "50/100" } = options;
  let pollAttempts = 0;

  await page.route("**/metadata", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockCapabilityStatement),
    });
  });

  // Mock the import-pnp kick-off endpoint (register first so it matches before $import).
  await page.route("**/$import-pnp", async (route) => {
    await route.fulfill({
      status: 202,
      headers: {
        "Content-Location": `http://localhost:3000/fhir/$job?id=${TEST_JOB_ID}`,
        "Access-Control-Expose-Headers": "Content-Location",
      },
      body: "",
    });
  });

  // Mock the standard import kick-off endpoint.
  await page.route("**/$import", async (route) => {
    await route.fulfill({
      status: 202,
      headers: {
        "Content-Location": `http://localhost:3000/fhir/$job?id=${TEST_JOB_ID}`,
        "Access-Control-Expose-Headers": "Content-Location",
      },
      body: "",
    });
  });

  await page.route("**/$job*", async (route) => {
    if (route.request().method() === "GET") {
      pollAttempts++;
      // Return in-progress or complete based on poll count.
      await route.fulfill(
        pollAttempts < pollCount
          ? {
              status: 202,
              contentType: "application/fhir+json",
              headers: {
                "X-Progress": progress,
                "Access-Control-Expose-Headers": "X-Progress",
              },
              body: JSON.stringify(mockJobStatusInProgress),
            }
          : {
              status: 200,
              contentType: "application/fhir+json",
              body: JSON.stringify(mockJobStatusComplete),
            },
      );
    } else if (route.request().method() === "DELETE") {
      await route.fulfill({ status: 204, body: "" });
    }
  });
}

test.describe("Import page", () => {
  test.describe("Initialisation", () => {
    test("loads and displays import form", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/import");

      // Verify the tabs are displayed.
      await expect(
        page.getByRole("tab", { name: "Import from URLs" }),
      ).toBeVisible();
      await expect(
        page.getByRole("tab", { name: "Import from FHIR server" }),
      ).toBeVisible();

      // Verify the form elements are displayed.
      await expect(page.getByText("Input format")).toBeVisible();
      await expect(page.getByText("Input files")).toBeVisible();
      await expect(
        page.getByRole("button", { name: "Start import" }),
      ).toBeVisible();
    });

    test("shows loading state while checking capabilities", async ({
      page,
    }) => {
      // Set up a delayed metadata response.
      await page.route("**/metadata", async (route) => {
        await new Promise((resolve) => setTimeout(resolve, 500));
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.goto("/admin/import");

      // Verify loading state is shown.
      await expect(
        page.getByText("Checking server capabilities..."),
      ).toBeVisible();
    });

    test("shows login prompt when auth required but not authenticated", async ({
      page,
    }) => {
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatementWithAuth),
        });
      });

      await page.goto("/admin/import");

      // Verify login required message is shown.
      await expect(
        page.getByText("You need to login before you can use this page."),
      ).toBeVisible();
    });
  });

  test.describe("Standard import tab", () => {
    test.describe("Form", () => {
      test("displays input format selector with options", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Open the input format dropdown.
        await page.getByRole("combobox").first().click();

        // Verify format options are available.
        await expect(
          page.getByRole("option", { name: "NDJSON" }),
        ).toBeVisible();
        await expect(
          page.getByRole("option", { name: "Parquet" }),
        ).toBeVisible();
        await expect(page.getByRole("option", { name: "Delta" })).toBeVisible();
      });

      test("displays resource type and URL inputs", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Verify resource type dropdown is present.
        const resourceTypeDropdown = page.getByRole("combobox").nth(1);
        await expect(resourceTypeDropdown).toBeVisible();

        // Verify URL input is present.
        await expect(
          page.getByPlaceholder("e.g., s3a://bucket/Patient.ndjson"),
        ).toBeVisible();
      });

      test("populates resource types from capabilities", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Open the resource type dropdown.
        await page.getByRole("combobox").nth(1).click();

        // Verify resource types from the mock are available.
        await expect(
          page.getByRole("option", { name: "Patient" }),
        ).toBeVisible();
        await expect(
          page.getByRole("option", { name: "Observation" }),
        ).toBeVisible();
        await expect(
          page.getByRole("option", { name: "Condition" }),
        ).toBeVisible();
      });

      test("adds input row", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Initially there should be one URL input.
        const urlInputs = page.getByPlaceholder(
          "e.g., s3a://bucket/Patient.ndjson",
        );
        await expect(urlInputs).toHaveCount(1);

        // Click add input button.
        await page.getByRole("button", { name: "Add input" }).click();

        // Verify a second input row appears.
        await expect(urlInputs).toHaveCount(2);
      });

      test("removes input row", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Add a second input.
        await page.getByRole("button", { name: "Add input" }).click();
        const urlInputs = page.getByPlaceholder(
          "e.g., s3a://bucket/Patient.ndjson",
        );
        await expect(urlInputs).toHaveCount(2);

        // Find and click the remove button (red IconButton with Cross2Icon).
        // Each input row has a red remove button as the last element.
        const removeButtons = page.locator('button[data-accent-color="red"]');
        await expect(removeButtons).toHaveCount(2);
        await removeButtons.first().click();

        // Verify only one input remains.
        await expect(urlInputs).toHaveCount(1);
      });

      test("cannot remove last input row", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // With only one input, the remove button should be disabled.
        const removeButton = page.locator('button[data-accent-color="red"]');
        await expect(removeButton).toBeDisabled();
      });

      test("displays save mode options", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Verify save mode options are displayed.
        await expect(page.getByText("Overwrite")).toBeVisible();
        await expect(page.getByText("Merge")).toBeVisible();
        await expect(page.getByText("Append")).toBeVisible();
      });

      test("submit button disabled when URL empty", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Verify import button is disabled when URL is empty.
        await expect(
          page.getByRole("button", { name: "Start import" }),
        ).toBeDisabled();

        // Enter a URL.
        await page
          .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
          .fill("s3a://test/data.ndjson");

        // Verify import button is now enabled.
        await expect(
          page.getByRole("button", { name: "Start import" }),
        ).toBeEnabled();
      });
    });

    test.describe("Import execution", () => {
      test("submits import and shows pending state", async ({ page }) => {
        await setupDelayedJobMocks(page, { pollCount: 3 });
        await page.goto("/admin/import");

        // Enter a URL and submit.
        await page
          .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
          .fill("s3a://test/data.ndjson");
        await page.getByRole("button", { name: "Start import" }).click();

        // Verify status card appears with import info.
        await expect(page.getByText("Importing 1 source(s)")).toBeVisible();
      });

      test("shows progress during import", async ({ page }) => {
        await setupDelayedJobMocks(page, { pollCount: 3, progress: "50/100" });
        await page.goto("/admin/import");

        // Enter a URL and submit.
        await page
          .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
          .fill("s3a://test/data.ndjson");
        await page.getByRole("button", { name: "Start import" }).click();

        // Verify progress is displayed (allow time for polling).
        await expect(page.getByText("50%")).toBeVisible({ timeout: 10000 });
      });

      test("shows completion message on success", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Enter a URL and submit.
        await page
          .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
          .fill("s3a://test/data.ndjson");
        await page.getByRole("button", { name: "Start import" }).click();

        // Verify completion message is shown (allow time for polling).
        await expect(
          page.getByText("Import completed successfully"),
        ).toBeVisible({ timeout: 10000 });

        // Verify "Close" button is shown (replaces old "New Import" button).
        await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
      });

      test("shows error message on failure", async ({ page }) => {
        await page.route("**/metadata", async (route) => {
          await route.fulfill({
            status: 200,
            contentType: "application/fhir+json",
            body: JSON.stringify(mockCapabilityStatement),
          });
        });

        await page.route("**/$import", async (route) => {
          await route.fulfill({
            status: 400,
            contentType: "application/fhir+json",
            body: JSON.stringify({
              resourceType: "OperationOutcome",
              issue: [{ severity: "error", diagnostics: "Invalid source URL" }],
            }),
          });
        });

        await page.goto("/admin/import");

        // Enter a URL and submit.
        await page
          .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
          .fill("s3a://invalid/data.ndjson");
        await page.getByRole("button", { name: "Start import" }).click();

        // Verify error message is displayed.
        await expect(page.getByText("Error:")).toBeVisible();
      });
    });

    test.describe("Cancellation", () => {
      test("cancels running import", async ({ page }) => {
        let cancelRequestUrl: string | null = null;

        await page.route("**/metadata", async (route) => {
          await route.fulfill({
            status: 200,
            contentType: "application/fhir+json",
            body: JSON.stringify(mockCapabilityStatement),
          });
        });

        await page.route("**/$import", async (route) => {
          await route.fulfill({
            status: 202,
            headers: {
              "Content-Location": `http://localhost:3000/fhir/$job?id=${TEST_JOB_ID}`,
              "Access-Control-Expose-Headers": "Content-Location",
            },
            body: "",
          });
        });

        await page.route("**/$job*", async (route) => {
          if (route.request().method() === "GET") {
            // Always return in-progress to keep the job running.
            await route.fulfill({
              status: 202,
              contentType: "application/fhir+json",
              body: JSON.stringify(mockJobStatusInProgress),
            });
          } else if (route.request().method() === "DELETE") {
            cancelRequestUrl = route.request().url();
            await route.fulfill({ status: 204, body: "" });
          }
        });

        await page.goto("/admin/import");

        // Enter a URL and submit.
        await page
          .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
          .fill("s3a://test/data.ndjson");
        await page.getByRole("button", { name: "Start import" }).click();

        // Wait for status card to appear with Cancel button.
        await expect(page.getByText("Importing 1 source(s)")).toBeVisible();
        await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible({
          timeout: 10000,
        });

        // Click cancel button.
        await page.getByRole("button", { name: "Cancel" }).click();

        // Verify cancel was requested to the correct URL.
        expect(cancelRequestUrl).toContain(`$job?id=${TEST_JOB_ID}`);
      });

      test("shows cancelled status indicator when import is cancelled", async ({
        page,
      }) => {
        await page.route("**/metadata", async (route) => {
          await route.fulfill({
            status: 200,
            contentType: "application/fhir+json",
            body: JSON.stringify(mockCapabilityStatement),
          });
        });

        await page.route("**/$import", async (route) => {
          await route.fulfill({
            status: 202,
            headers: {
              "Content-Location": `http://localhost:3000/fhir/$job?id=${TEST_JOB_ID}`,
              "Access-Control-Expose-Headers": "Content-Location",
            },
            body: "",
          });
        });

        await page.route("**/$job*", async (route) => {
          if (route.request().method() === "GET") {
            await route.fulfill({
              status: 202,
              contentType: "application/fhir+json",
              body: JSON.stringify(mockJobStatusInProgress),
            });
          } else if (route.request().method() === "DELETE") {
            await route.fulfill({ status: 204, body: "" });
          }
        });

        await page.goto("/admin/import");

        // Enter a URL and submit.
        await page
          .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
          .fill("s3a://test/data.ndjson");
        await page.getByRole("button", { name: "Start import" }).click();

        // Wait for cancel button and click it.
        await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible({
          timeout: 10000,
        });
        await page.getByRole("button", { name: "Cancel" }).click();

        // Verify cancelled status indicator is visible.
        await expect(page.getByText("Cancelled")).toBeVisible();
      });

      test("close button visible when import is cancelled", async ({
        page,
      }) => {
        await page.route("**/metadata", async (route) => {
          await route.fulfill({
            status: 200,
            contentType: "application/fhir+json",
            body: JSON.stringify(mockCapabilityStatement),
          });
        });

        await page.route("**/$import", async (route) => {
          await route.fulfill({
            status: 202,
            headers: {
              "Content-Location": `http://localhost:3000/fhir/$job?id=${TEST_JOB_ID}`,
              "Access-Control-Expose-Headers": "Content-Location",
            },
            body: "",
          });
        });

        await page.route("**/$job*", async (route) => {
          if (route.request().method() === "GET") {
            await route.fulfill({
              status: 202,
              contentType: "application/fhir+json",
              body: JSON.stringify(mockJobStatusInProgress),
            });
          } else if (route.request().method() === "DELETE") {
            await route.fulfill({ status: 204, body: "" });
          }
        });

        await page.goto("/admin/import");

        // Enter a URL and submit.
        await page
          .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
          .fill("s3a://test/data.ndjson");
        await page.getByRole("button", { name: "Start import" }).click();

        // Wait for cancel button and click it.
        await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible({
          timeout: 10000,
        });
        await page.getByRole("button", { name: "Cancel" }).click();

        // Verify close button is visible.
        await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
      });

      test("clicking close button removes cancelled import card", async ({
        page,
      }) => {
        await page.route("**/metadata", async (route) => {
          await route.fulfill({
            status: 200,
            contentType: "application/fhir+json",
            body: JSON.stringify(mockCapabilityStatement),
          });
        });

        await page.route("**/$import", async (route) => {
          await route.fulfill({
            status: 202,
            headers: {
              "Content-Location": `http://localhost:3000/fhir/$job?id=${TEST_JOB_ID}`,
              "Access-Control-Expose-Headers": "Content-Location",
            },
            body: "",
          });
        });

        await page.route("**/$job*", async (route) => {
          if (route.request().method() === "GET") {
            await route.fulfill({
              status: 202,
              contentType: "application/fhir+json",
              body: JSON.stringify(mockJobStatusInProgress),
            });
          } else if (route.request().method() === "DELETE") {
            await route.fulfill({ status: 204, body: "" });
          }
        });

        await page.goto("/admin/import");

        // Enter a URL and submit.
        await page
          .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
          .fill("s3a://test/data.ndjson");
        await page.getByRole("button", { name: "Start import" }).click();

        // Wait for cancel button and click it.
        await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible({
          timeout: 10000,
        });
        await page.getByRole("button", { name: "Cancel" }).click();

        // Wait for close button and click it.
        await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
        await page.getByRole("button", { name: "Close" }).click();

        // Verify the import card is removed (check for card-specific content).
        await expect(page.getByText("Importing 1 source(s)")).not.toBeVisible();
      });
    });
  });

  test.describe("PnP import tab", () => {
    test.describe("Form", () => {
      test("displays export URL field", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Switch to PnP tab.
        await page
          .getByRole("tab", { name: "Import from FHIR server" })
          .click();

        // Verify export URL field is present.
        await expect(
          page.getByPlaceholder("e.g., https://example.org/fhir/$export"),
        ).toBeVisible();
      });

      test("displays input format and save mode", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Switch to PnP tab.
        await page
          .getByRole("tab", { name: "Import from FHIR server" })
          .click();

        // Verify input format dropdown is present.
        await expect(page.getByText("Input format")).toBeVisible();

        // Verify save mode options are displayed.
        await expect(page.getByText("Overwrite")).toBeVisible();
        await expect(page.getByText("Merge")).toBeVisible();
      });

      test("submit button disabled when URL empty", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Switch to PnP tab.
        await page
          .getByRole("tab", { name: "Import from FHIR server" })
          .click();

        // Verify import button is disabled when URL is empty.
        await expect(
          page.getByRole("button", { name: "Start import" }),
        ).toBeDisabled();

        // Enter a URL.
        await page
          .getByPlaceholder("e.g., https://example.org/fhir/$export")
          .fill("https://test.org/fhir/$export");

        // Verify import button is now enabled.
        await expect(
          page.getByRole("button", { name: "Start import" }),
        ).toBeEnabled();
      });
    });

    test.describe("Import execution", () => {
      test("submits import and shows progress", async ({ page }) => {
        await setupDelayedJobMocks(page, { pollCount: 3, progress: "25/100" });
        await page.goto("/admin/import");

        // Switch to PnP tab.
        await page
          .getByRole("tab", { name: "Import from FHIR server" })
          .click();

        // Enter export URL and submit.
        await page
          .getByPlaceholder("e.g., https://example.org/fhir/$export")
          .fill("https://test.org/fhir/$export");
        await page.getByRole("button", { name: "Start import" }).click();

        // Verify status card appears.
        await expect(
          page.getByText("Importing from https://test.org/fhir/$export"),
        ).toBeVisible();
      });

      test("shows completion on success", async ({ page }) => {
        await setupStandardMocks(page);
        await page.goto("/admin/import");

        // Switch to PnP tab.
        await page
          .getByRole("tab", { name: "Import from FHIR server" })
          .click();

        // Enter export URL and submit.
        await page
          .getByPlaceholder("e.g., https://example.org/fhir/$export")
          .fill("https://test.org/fhir/$export");
        await page.getByRole("button", { name: "Start import" }).click();

        // Verify completion message is shown (allow time for polling).
        await expect(
          page.getByText("Import completed successfully"),
        ).toBeVisible({ timeout: 10000 });
      });
    });
  });

  test.describe("Multiple imports", () => {
    test("form remains enabled after starting import", async ({ page }) => {
      await setupDelayedJobMocks(page, { pollCount: 10 });
      await page.goto("/admin/import");

      // Enter a URL and start import.
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();

      // Verify import card is visible.
      await expect(page.getByText("Importing 1 source(s)")).toBeVisible({
        timeout: 10000,
      });

      // Verify form is still enabled (Start import button is clickable).
      await expect(
        page.getByRole("button", { name: "Start import" }),
      ).toBeEnabled();
    });

    test("starting second import creates additional result card", async ({
      page,
    }) => {
      await setupDelayedJobMocks(page, { pollCount: 10 });
      await page.goto("/admin/import");

      // Start first import.
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data1.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();

      // Verify first import card appears.
      await expect(page.getByText("Importing 1 source(s)")).toBeVisible({
        timeout: 10000,
      });

      // Clear the URL and start second import.
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data2.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();

      // Verify both import cards are visible (2 cards with "Importing 1 source(s)").
      const importCards = page.getByText("Importing 1 source(s)");
      await expect(importCards).toHaveCount(2);
    });

    test("each result card shows its own import type", async ({ page }) => {
      await setupDelayedJobMocks(page, { pollCount: 10 });
      await page.goto("/admin/import");

      // Start standard import.
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();

      // Verify standard import card appears.
      await expect(page.getByText("Importing 1 source(s)")).toBeVisible({
        timeout: 10000,
      });

      // Switch to PnP tab and start PnP import.
      await page.getByRole("tab", { name: "Import from FHIR server" }).click();
      await page
        .getByPlaceholder("e.g., https://example.org/fhir/$export")
        .fill("https://test.org/fhir/$export");
      await page.getByRole("button", { name: "Start import" }).click();

      // Verify both cards show their respective content.
      await expect(page.getByText("Importing 1 source(s)")).toBeVisible();
      await expect(
        page.getByText("Importing from https://test.org/fhir/$export"),
      ).toBeVisible();
    });

    test("import card displays timestamp", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/import");

      // Start import.
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();

      // Wait for the import card to appear.
      await expect(page.getByText("Importing 1 source(s)")).toBeVisible({
        timeout: 10000,
      });

      // Verify timestamp is displayed (matches time with seconds like "10:30:45").
      await expect(page.getByText(/\d{1,2}:\d{2}:\d{2}/)).toBeVisible();
    });

    test("most recent import appears first", async ({ page }) => {
      await setupDelayedJobMocks(page, { pollCount: 10 });
      await page.goto("/admin/import");

      // Start first import (standard).
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();

      // Wait for first import card to appear.
      await expect(page.getByText("Importing 1 source(s)")).toBeVisible({
        timeout: 10000,
      });

      // Switch to PnP tab and start second import.
      await page.getByRole("tab", { name: "Import from FHIR server" }).click();
      await page
        .getByPlaceholder("e.g., https://example.org/fhir/$export")
        .fill("https://test.org/fhir/$export");
      await page.getByRole("button", { name: "Start import" }).click();

      // Verify PnP card is visible.
      await expect(
        page.getByText("Importing from https://test.org/fhir/$export"),
      ).toBeVisible();

      // Verify most recent (FHIR server) appears before older (URLs) import.
      const fhirServerBox = await page
        .getByText("Importing from https://test.org/fhir/$export")
        .boundingBox();
      const urlsBox = await page
        .getByText("Importing 1 source(s)")
        .boundingBox();
      expect(fhirServerBox!.y).toBeLessThan(urlsBox!.y);
    });

    test("New Import button is not present in completed import cards", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/import");

      // Start and complete an import.
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();
      await expect(page.getByText("Import completed successfully")).toBeVisible(
        { timeout: 10000 },
      );

      // Verify New Import button is not present.
      await expect(
        page.getByRole("button", { name: "New Import" }),
      ).not.toBeVisible();
    });
  });

  test.describe("Close button", () => {
    test("close button visible when import is complete", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/import");

      // Start import.
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();

      // Wait for import to complete.
      await expect(page.getByText("Import completed successfully")).toBeVisible(
        { timeout: 10000 },
      );

      // Verify close button is visible.
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
    });

    test("clicking close button removes completed import card", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/import");

      // Start import.
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();

      // Wait for import to complete.
      await expect(page.getByText("Import completed successfully")).toBeVisible(
        { timeout: 10000 },
      );

      // Click close button.
      await page.getByRole("button", { name: "Close" }).click();

      // Verify the import card is removed (check card-specific content).
      await expect(page.getByText("Importing 1 source(s)")).not.toBeVisible();
    });

    test("close button visible when import errors", async ({ page }) => {
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/$import", async (route) => {
        await route.fulfill({
          status: 500,
          contentType: "application/fhir+json",
          body: JSON.stringify({
            resourceType: "OperationOutcome",
            issue: [{ severity: "error", diagnostics: "Import failed" }],
          }),
        });
      });

      await page.goto("/admin/import");

      // Start import.
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();

      // Wait for error to appear.
      await expect(page.getByText("Error:")).toBeVisible({ timeout: 10000 });

      // Verify close button is visible.
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
    });

    test("clicking close button removes errored import card", async ({
      page,
    }) => {
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/$import", async (route) => {
        await route.fulfill({
          status: 500,
          contentType: "application/fhir+json",
          body: JSON.stringify({
            resourceType: "OperationOutcome",
            issue: [{ severity: "error", diagnostics: "Import failed" }],
          }),
        });
      });

      await page.goto("/admin/import");

      // Start import.
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();

      // Wait for error and close button.
      await expect(page.getByText("Error:")).toBeVisible({ timeout: 10000 });
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();

      // Click close button.
      await page.getByRole("button", { name: "Close" }).click();

      // Verify the import card is removed (check for error message gone).
      await expect(page.getByText("Error:")).not.toBeVisible();
    });
  });

  test.describe("Tab behaviour", () => {
    test("tabs remain enabled during import", async ({ page }) => {
      // Set up mocks that keep the job running.
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/$import", async (route) => {
        await route.fulfill({
          status: 202,
          headers: {
            "Content-Location": `http://localhost:3000/fhir/$job?id=${TEST_JOB_ID}`,
            "Access-Control-Expose-Headers": "Content-Location",
          },
          body: "",
        });
      });

      await page.route("**/$job*", async (route) => {
        if (route.request().method() === "GET") {
          await route.fulfill({
            status: 202,
            contentType: "application/fhir+json",
            body: JSON.stringify(mockJobStatusInProgress),
          });
        } else if (route.request().method() === "DELETE") {
          await route.fulfill({ status: 204, body: "" });
        }
      });

      await page.goto("/admin/import");

      // Enter a URL and start import.
      await page
        .getByPlaceholder("e.g., s3a://bucket/Patient.ndjson")
        .fill("s3a://test/data.ndjson");
      await page.getByRole("button", { name: "Start import" }).click();

      // Wait for import to start.
      await expect(page.getByText("Importing 1 source(s)")).toBeVisible();
      await expect(page.getByText("Processing...")).toBeVisible({
        timeout: 10000,
      });

      // Verify both tabs remain enabled (no data-disabled attribute).
      const urlsTab = page.getByRole("tab", { name: "Import from URLs" });
      const fhirTab = page.getByRole("tab", {
        name: "Import from FHIR server",
      });
      await expect(urlsTab).not.toHaveAttribute("data-disabled", "");
      await expect(fhirTab).not.toHaveAttribute("data-disabled", "");
    });
  });
});

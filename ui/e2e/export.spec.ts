/**
 * E2E tests for the Export page.
 *
 * @author John Grimes
 */

import { expect, test } from "@playwright/test";

import {
  mockCapabilityStatement,
  mockCapabilityStatementWithAuth,
  mockExportManifest,
} from "./fixtures/fhirData";

const TEST_JOB_ID = "export-job-123";

/**
 * Sets up standard API mocks for export page tests.
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

  // Mock all export kick-off endpoints with a single route.
  // Matches /$export, /Patient/$export, /Patient/{id}/$export, /Group/{id}/$export.
  await page.route("**/$export*", async (route) => {
    await route.fulfill({
      status: 202,
      headers: {
        "Content-Location": `http://localhost:3000/fhir/$job?id=${TEST_JOB_ID}`,
        "Access-Control-Expose-Headers": "Content-Location",
      },
      body: "",
    });
  });

  // Mock job status to return complete immediately with manifest.
  await page.route("**/$job*", async (route) => {
    if (route.request().method() === "GET") {
      await route.fulfill({
        status: 200,
        contentType: "application/fhir+json",
        body: JSON.stringify(mockExportManifest),
      });
    } else if (route.request().method() === "DELETE") {
      await route.fulfill({ status: 204 });
    }
  });

  // Mock file download endpoint.
  await page.route("**/export-files*", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+ndjson",
      body: '{"resourceType":"Patient","id":"1"}\n{"resourceType":"Patient","id":"2"}',
    });
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

  // Mock all export kick-off endpoints with a single route.
  await page.route("**/$export*", async (route) => {
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
              body: "",
            }
          : {
              status: 200,
              contentType: "application/fhir+json",
              body: JSON.stringify(mockExportManifest),
            },
      );
    } else if (route.request().method() === "DELETE") {
      await route.fulfill({ status: 204, body: "" });
    }
  });

  await page.route("**/export-files*", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+ndjson",
      body: '{"resourceType":"Patient","id":"1"}',
    });
  });
}

test.describe("Export page", () => {
  test.describe("Initialisation", () => {
    test("loads and displays export form", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Verify the form is displayed.
      await expect(
        page.getByRole("heading", { name: "New export" }),
      ).toBeVisible();
      await expect(page.getByText("Export level")).toBeVisible();
      await expect(
        page.getByText("Resource types", { exact: true }),
      ).toBeVisible();
      await expect(
        page.getByRole("button", { name: "Start export" }),
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

      await page.goto("/admin/export");

      // Verify loading state is shown.
      await expect(
        page.getByText("Checking server capabilities..."),
      ).toBeVisible();
    });

    test("populates resource types from capabilities", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Verify resource types from the mock are available as checkbox cards.
      await expect(page.getByLabel("Patient")).toBeVisible();
      await expect(page.getByLabel("Observation")).toBeVisible();
      await expect(page.getByLabel("Condition")).toBeVisible();
    });
  });

  test.describe("Authentication", () => {
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

      await page.goto("/admin/export");

      // Verify login required message is shown.
      await expect(
        page.getByText("You need to login before you can use this page."),
      ).toBeVisible();
    });
  });

  test.describe("Form functionality", () => {
    test("displays all export level options", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Open the export level dropdown.
      await page.getByRole("combobox").click();

      // Verify all export level options are available.
      await expect(
        page.getByRole("option", { name: "All data in system" }),
      ).toBeVisible();
      await expect(
        page.getByRole("option", { name: "All patient data" }),
      ).toBeVisible();
      await expect(
        page.getByRole("option", { name: "Data for single patient" }),
      ).toBeVisible();
      await expect(
        page.getByRole("option", { name: "Data for patients in group" }),
      ).toBeVisible();
    });

    test("shows patient ID field when Patient Instance selected", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Patient ID field should not be visible initially.
      await expect(
        page.getByPlaceholder("e.g., patient-123"),
      ).not.toBeVisible();

      // Select Patient Instance export level.
      await page.getByRole("combobox").click();
      await page
        .getByRole("option", { name: "Data for single patient" })
        .click();

      // Verify Patient ID field is now visible.
      await expect(page.getByText("Patient ID")).toBeVisible();
      await expect(page.getByPlaceholder("e.g., patient-123")).toBeVisible();
    });

    test("shows group ID field when Group selected", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Group ID field should not be visible initially.
      await expect(page.getByPlaceholder("e.g., group-456")).not.toBeVisible();

      // Select Group export level.
      await page.getByRole("combobox").click();
      await page
        .getByRole("option", { name: "Data for patients in group" })
        .click();

      // Verify Group ID field is now visible.
      await expect(page.getByText("Group ID")).toBeVisible();
      await expect(page.getByPlaceholder("e.g., group-456")).toBeVisible();
    });

    test("hides patient/group ID fields for System export", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // System is the default, so these fields should not be visible.
      await expect(
        page.getByPlaceholder("e.g., patient-123"),
      ).not.toBeVisible();
      await expect(page.getByPlaceholder("e.g., group-456")).not.toBeVisible();
    });

    test("resource types can be selected and cleared", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Select a resource type by clicking the label (use force to bypass interception).
      const patientLabel = page.getByLabel("Patient");
      await patientLabel.click({ force: true });
      await expect(patientLabel).toBeChecked();

      // Click Clear to deselect all.
      await page.getByText("Clear").click();
      await expect(patientLabel).not.toBeChecked();
    });

    test("submit button disabled when patient ID required but empty", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Select Patient Instance export level.
      await page.getByRole("combobox").click();
      await page
        .getByRole("option", { name: "Data for single patient" })
        .click();

      // Submit button should be disabled.
      await expect(
        page.getByRole("button", { name: "Start export" }),
      ).toBeDisabled();

      // Enter a patient ID.
      await page.getByPlaceholder("e.g., patient-123").fill("patient-123");

      // Submit button should now be enabled.
      await expect(
        page.getByRole("button", { name: "Start export" }),
      ).toBeEnabled();
    });

    test("submit button disabled when group ID required but empty", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Select Group export level.
      await page.getByRole("combobox").click();
      await page
        .getByRole("option", { name: "Data for patients in group" })
        .click();

      // Submit button should be disabled.
      await expect(
        page.getByRole("button", { name: "Start export" }),
      ).toBeDisabled();

      // Enter a group ID.
      await page.getByPlaceholder("e.g., group-456").fill("group-456");

      // Submit button should now be enabled.
      await expect(
        page.getByRole("button", { name: "Start export" }),
      ).toBeEnabled();
    });
  });

  test.describe("Export execution", () => {
    test("executes system export and displays results", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify status card shows export type.
      await expect(page.getByText("System export")).toBeVisible();

      // Verify output files are displayed (allow time for polling).
      await expect(page.getByText("Output files (2)")).toBeVisible({
        timeout: 10000,
      });
      await expect(page.getByText("Patient.ndjson")).toBeVisible();
      await expect(page.getByText("Observation.ndjson")).toBeVisible();
    });

    test("executes patient type export", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Select Patient Type export level.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "All patient data" }).click();

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify status card shows correct export type.
      await expect(page.getByText("All patients export")).toBeVisible();
    });

    test("executes patient instance export with patient ID", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Select Patient Instance export level.
      await page.getByRole("combobox").click();
      await page
        .getByRole("option", { name: "Data for single patient" })
        .click();

      // Enter patient ID.
      await page.getByPlaceholder("e.g., patient-123").fill("test-patient-id");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify status card shows correct export type.
      await expect(page.getByText("Patient export")).toBeVisible();
    });

    test("executes group export with group ID", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Select Group export level.
      await page.getByRole("combobox").click();
      await page
        .getByRole("option", { name: "Data for patients in group" })
        .click();

      // Enter group ID.
      await page.getByPlaceholder("e.g., group-456").fill("test-group-id");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify status card shows correct export type.
      await expect(page.getByText("Group export")).toBeVisible();
    });

    test("shows progress during export", async ({ page }) => {
      await setupDelayedJobMocks(page, { pollCount: 3, progress: "50/100" });
      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify progress is displayed (allow time for polling).
      await expect(page.getByText("50%")).toBeVisible({ timeout: 10000 });
    });

    test("displays output file counts", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify output file counts are displayed.
      await expect(page.getByText("100 resources")).toBeVisible({
        timeout: 10000,
      });
      await expect(page.getByText("250 resources")).toBeVisible();
    });

    test("displays selected resource types in status card", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Select specific resource types (use force to bypass label interception).
      await page.getByLabel("Patient").click({ force: true });
      await page.getByLabel("Observation").click({ force: true });

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify resource types are shown in status card.
      await expect(page.getByText("Types: Patient, Observation")).toBeVisible();
    });

    test("close button visible when export is complete", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for export to complete.
      await expect(page.getByText("Output files (2)")).toBeVisible({
        timeout: 10000,
      });

      // Verify close button is visible.
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
    });

    test("clicking close button removes completed export card", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for export to complete.
      await expect(page.getByText("Output files (2)")).toBeVisible({
        timeout: 10000,
      });

      // Verify close button is visible.
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();

      // Click close button.
      await page.getByRole("button", { name: "Close" }).click();

      // Verify the export card is removed.
      await expect(page.getByText("System export")).not.toBeVisible();
    });
  });

  test.describe("Cancellation", () => {
    test("cancel button visible during export", async ({ page }) => {
      await setupDelayedJobMocks(page, { pollCount: 10 });
      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify cancel button is visible.
      await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible({
        timeout: 10000,
      });
    });

    test("cancelling stops the export", async ({ page }) => {
      let cancelRequestUrl: string | null = null;

      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      // Mock system export kick-off.
      await page.route("**/$export*", async (route) => {
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
            headers: {
              "X-Progress": "25/100",
              "Access-Control-Expose-Headers": "X-Progress",
            },
            body: "",
          });
        } else if (route.request().method() === "DELETE") {
          cancelRequestUrl = route.request().url();
          await route.fulfill({ status: 204, body: "" });
        }
      });

      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for cancel button to appear.
      await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible({
        timeout: 10000,
      });

      // Click cancel.
      await page.getByRole("button", { name: "Cancel" }).click();

      // Verify the export was cancelled and DELETE request was sent to the correct URL.
      await expect(
        page.getByRole("button", { name: "Cancel" }),
      ).not.toBeVisible();
      expect(cancelRequestUrl).toContain(`$job?id=${TEST_JOB_ID}`);
    });

    test("shows cancelled status indicator when export is cancelled", async ({
      page,
    }) => {
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/$export*", async (route) => {
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
        // Always return in-progress to keep the job running.
        await route.fulfill({
          status: 202,
          contentType: "application/fhir+json",
          headers: {
            "X-Progress": "25/100",
            "Access-Control-Expose-Headers": "X-Progress",
          },
          body: "",
        });
      });

      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for cancel button and click it.
      await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible({
        timeout: 10000,
      });
      await page.getByRole("button", { name: "Cancel" }).click();

      // Verify cancelled status indicator is visible.
      await expect(page.getByText("Cancelled")).toBeVisible();
    });

    test("close button visible when export is cancelled", async ({ page }) => {
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/$export*", async (route) => {
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
        // Always return in-progress to keep the job running.
        await route.fulfill({
          status: 202,
          contentType: "application/fhir+json",
          headers: {
            "X-Progress": "25/100",
            "Access-Control-Expose-Headers": "X-Progress",
          },
          body: "",
        });
      });

      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for cancel button and click it.
      await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible({
        timeout: 10000,
      });
      await page.getByRole("button", { name: "Cancel" }).click();

      // Verify close button is visible.
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
    });

    test("clicking close button removes cancelled export card", async ({
      page,
    }) => {
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/$export*", async (route) => {
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
        // Always return in-progress to keep the job running.
        await route.fulfill({
          status: 202,
          contentType: "application/fhir+json",
          headers: {
            "X-Progress": "25/100",
            "Access-Control-Expose-Headers": "X-Progress",
          },
          body: "",
        });
      });

      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for cancel button and click it.
      await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible({
        timeout: 10000,
      });
      await page.getByRole("button", { name: "Cancel" }).click();

      // Verify close button is visible.
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();

      // Click close button.
      await page.getByRole("button", { name: "Close" }).click();

      // Verify the export card is removed.
      await expect(page.getByText("System export")).not.toBeVisible();
    });
  });

  test.describe("File download", () => {
    test("download button triggers file download", async ({ page }) => {
      let downloadRequested = false;

      await setupStandardMocks(page);

      // Override the file download mock to track the request.
      await page.route("**/export-files*", async (route) => {
        downloadRequested = true;
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+ndjson",
          body: '{"resourceType":"Patient","id":"1"}',
        });
      });

      await page.goto("/admin/export");

      // Start and complete export.
      await page.getByRole("button", { name: "Start export" }).click();
      await expect(page.getByText("Output files (2)")).toBeVisible({
        timeout: 10000,
      });

      // Click download on first file.
      await page.getByRole("button", { name: "Download" }).first().click();

      // Verify download was requested.
      expect(downloadRequested).toBe(true);
    });
  });

  test.describe("Multiple exports", () => {
    test("form remains enabled after starting export", async ({ page }) => {
      await setupDelayedJobMocks(page, { pollCount: 10 });
      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify export card is visible.
      await expect(page.getByText("System export")).toBeVisible({
        timeout: 10000,
      });

      // Verify form is still enabled (Start export button is clickable).
      await expect(
        page.getByRole("button", { name: "Start export" }),
      ).toBeEnabled();
    });

    test("starting second export creates additional result card", async ({
      page,
    }) => {
      await setupDelayedJobMocks(page, { pollCount: 10 });
      await page.goto("/admin/export");

      // Start first export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify first export card appears.
      await expect(page.getByText("System export")).toBeVisible({
        timeout: 10000,
      });

      // Select different export level for second export (while first is still running).
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "All patient data" }).click();

      // Start second export immediately (don't wait for first to complete).
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify both export cards are visible.
      await expect(page.getByText("System export")).toBeVisible();
      await expect(page.getByText("All patients export")).toBeVisible();
    });

    test("each result card shows its own export type and resource types", async ({
      page,
    }) => {
      await setupDelayedJobMocks(page, { pollCount: 10 });
      await page.goto("/admin/export");

      // Select Patient type for first export.
      await page.getByLabel("Patient").click({ force: true });

      // Start first export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify first export card appears (while still running).
      await expect(page.getByText("Types: Patient")).toBeVisible({
        timeout: 10000,
      });

      // Clear and select Observation for second export.
      await page.getByText("Clear").click();
      await page.getByLabel("Observation").click({ force: true });

      // Select different export level for second export.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "All patient data" }).click();

      // Start second export immediately (while first is still running).
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify both cards show their respective resource types.
      await expect(page.getByText("Types: Patient")).toBeVisible();
      await expect(page.getByText("Types: Observation")).toBeVisible();
    });

    test("export card displays timestamp", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for the export card to appear.
      await expect(page.getByText("System export")).toBeVisible({
        timeout: 10000,
      });

      // Verify timestamp is displayed (matches time with seconds like "10:30:45").
      await expect(page.getByText(/\d{1,2}:\d{2}:\d{2}/)).toBeVisible();
    });

    test("most recent export appears first", async ({ page }) => {
      await setupDelayedJobMocks(page, { pollCount: 10 });
      await page.goto("/admin/export");

      // Start first export (System export).
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for first export card to appear.
      await expect(page.getByText("System export")).toBeVisible({
        timeout: 10000,
      });

      // Select different export level for second export.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "All patient data" }).click();

      // Start second export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify both cards are visible.
      await expect(page.getByText("All patients export")).toBeVisible();

      // Verify most recent (All patients) appears before older (System) export.
      const allPatientsBox = await page
        .getByText("All patients export")
        .boundingBox();
      const systemBox = await page.getByText("System export").boundingBox();
      expect(allPatientsBox!.y).toBeLessThan(systemBox!.y);
    });

    test("New Export button is not present in completed export cards", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Start and complete an export.
      await page.getByRole("button", { name: "Start export" }).click();
      await expect(page.getByText("Output files (2)")).toBeVisible({
        timeout: 10000,
      });

      // Verify New Export button is not present.
      await expect(
        page.getByRole("button", { name: "New Export" }),
      ).not.toBeVisible();
    });
  });

  test.describe("Error handling", () => {
    test("displays error message on kick-off failure", async ({ page }) => {
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/$export*", async (route) => {
        await route.fulfill({
          status: 400,
          contentType: "application/fhir+json",
          body: JSON.stringify({
            resourceType: "OperationOutcome",
            issue: [
              { severity: "error", diagnostics: "Invalid export request" },
            ],
          }),
        });
      });

      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify error message is displayed.
      await expect(page.getByText("Error:")).toBeVisible();
    });

    test("displays error message on polling failure", async ({ page }) => {
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/$export*", async (route) => {
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
        await route.fulfill({
          status: 500,
          contentType: "application/fhir+json",
          body: JSON.stringify({
            resourceType: "OperationOutcome",
            issue: [{ severity: "error", diagnostics: "Export job failed" }],
          }),
        });
      });

      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify error message is displayed.
      await expect(page.getByText("Error:")).toBeVisible({ timeout: 10000 });
    });

    test("close button visible when export errors", async ({ page }) => {
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/$export*", async (route) => {
        await route.fulfill({
          status: 500,
          contentType: "application/fhir+json",
          body: JSON.stringify({
            resourceType: "OperationOutcome",
            issue: [{ severity: "error", diagnostics: "Export failed" }],
          }),
        });
      });

      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for error to appear.
      await expect(page.getByText("Error:")).toBeVisible({ timeout: 10000 });

      // Verify close button is visible.
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
    });

    test("clicking close button removes errored export card", async ({
      page,
    }) => {
      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.route("**/$export*", async (route) => {
        await route.fulfill({
          status: 500,
          contentType: "application/fhir+json",
          body: JSON.stringify({
            resourceType: "OperationOutcome",
            issue: [{ severity: "error", diagnostics: "Export failed" }],
          }),
        });
      });

      await page.goto("/admin/export");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for error and close button.
      await expect(page.getByText("Error:")).toBeVisible({ timeout: 10000 });
      await expect(page.getByRole("button", { name: "Close" })).toBeVisible();

      // Click close button.
      await page.getByRole("button", { name: "Close" }).click();

      // Verify the export card is removed.
      await expect(page.getByText("System export")).not.toBeVisible();
    });
  });

  test.describe("Export parameters", () => {
    test("passes _since parameter to server", async ({ page }) => {
      let exportUrl: string | null = null;

      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      // Intercept export request to capture URL.
      await page.route("**/$export*", async (route) => {
        exportUrl = route.request().url();
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
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockExportManifest),
        });
      });

      await page.goto("/admin/export");

      // Fill the Since field with a datetime value.
      await page
        .locator('input[type="datetime-local"]')
        .first()
        .fill("2024-01-15T10:30");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for export to complete.
      await expect(page.getByText("Output files (2)")).toBeVisible({
        timeout: 10000,
      });

      // Verify _since parameter was passed (colon is URL-encoded as %3A).
      expect(exportUrl).toContain("_since=");
      expect(exportUrl).toContain("2024-01-15");
    });

    test("passes _until parameter to server", async ({ page }) => {
      let exportUrl: string | null = null;

      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      // Intercept export request to capture URL.
      await page.route("**/$export*", async (route) => {
        exportUrl = route.request().url();
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
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockExportManifest),
        });
      });

      await page.goto("/admin/export");

      // Fill the Until field with a datetime value (second datetime-local input).
      await page
        .locator('input[type="datetime-local"]')
        .nth(1)
        .fill("2024-06-30T23:59");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for export to complete.
      await expect(page.getByText("Output files (2)")).toBeVisible({
        timeout: 10000,
      });

      // Verify _until parameter was passed (colon is URL-encoded as %3A).
      expect(exportUrl).toContain("_until=");
      expect(exportUrl).toContain("2024-06-30");
    });

    test("passes _elements parameter to server", async ({ page }) => {
      let exportUrl: string | null = null;

      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      // Intercept export request to capture URL.
      await page.route("**/$export*", async (route) => {
        exportUrl = route.request().url();
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
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockExportManifest),
        });
      });

      await page.goto("/admin/export");

      // Fill the Elements field.
      await page.getByPlaceholder("e.g., id,meta,name").fill("id,meta,name");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for export to complete.
      await expect(page.getByText("Output files (2)")).toBeVisible({
        timeout: 10000,
      });

      // Verify _elements parameter was passed (commas are URL-encoded as %2C).
      expect(exportUrl).toContain("_elements=");
      expect(exportUrl).toMatch(/id.*meta.*name/);
    });

    test("passes all parameters together to server", async ({ page }) => {
      let exportUrl: string | null = null;

      await page.route("**/metadata", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      // Intercept export request to capture URL.
      await page.route("**/$export*", async (route) => {
        exportUrl = route.request().url();
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
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockExportManifest),
        });
      });

      await page.goto("/admin/export");

      // Fill all three fields.
      await page
        .locator('input[type="datetime-local"]')
        .first()
        .fill("2024-01-15T10:30");
      await page
        .locator('input[type="datetime-local"]')
        .nth(1)
        .fill("2024-06-30T23:59");
      await page.getByPlaceholder("e.g., id,meta,name").fill("id,meta,name");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Wait for export to complete.
      await expect(page.getByText("Output files (2)")).toBeVisible({
        timeout: 10000,
      });

      // Verify all parameters were passed (special chars are URL-encoded).
      expect(exportUrl).toContain("_since=");
      expect(exportUrl).toContain("2024-01-15");
      expect(exportUrl).toContain("_until=");
      expect(exportUrl).toContain("2024-06-30");
      expect(exportUrl).toContain("_elements=");
      expect(exportUrl).toMatch(/id.*meta.*name/);
    });
  });
});

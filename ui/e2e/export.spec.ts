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
      if (pollAttempts < pollCount) {
        // Return in-progress with progress header.
        await route.fulfill({
          status: 202,
          contentType: "application/fhir+json",
          headers: {
            "X-Progress": progress,
            "Access-Control-Expose-Headers": "X-Progress",
          },
          body: "",
        });
      } else {
        // Return complete with manifest.
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockExportManifest),
        });
      }
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
        page.getByRole("option", { name: "System (all resources)" }),
      ).toBeVisible();
      await expect(
        page.getByRole("option", { name: "Patient type (all patients)" }),
      ).toBeVisible();
      await expect(
        page.getByRole("option", { name: "Patient instance (single patient)" }),
      ).toBeVisible();
      await expect(
        page.getByRole("option", { name: "Group (group members)" }),
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
        .getByRole("option", { name: "Patient instance (single patient)" })
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
      await page.getByRole("option", { name: "Group (group members)" }).click();

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

    test("Select all selects all resource types", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Click Select all.
      await page.getByText("Select all").click();

      // Verify all checkboxes are checked.
      await expect(page.getByLabel("Patient")).toBeChecked();
      await expect(page.getByLabel("Observation")).toBeChecked();
      await expect(page.getByLabel("Condition")).toBeChecked();
    });

    test("submit button disabled when patient ID required but empty", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Select Patient Instance export level.
      await page.getByRole("combobox").click();
      await page
        .getByRole("option", { name: "Patient instance (single patient)" })
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
      await page.getByRole("option", { name: "Group (group members)" }).click();

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
      await expect(page.getByText("System Export")).toBeVisible();

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
      await page
        .getByRole("option", { name: "Patient type (all patients)" })
        .click();

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify status card shows correct export type.
      await expect(page.getByText("Patient Type Export")).toBeVisible();
    });

    test("executes patient instance export with patient ID", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Select Patient Instance export level.
      await page.getByRole("combobox").click();
      await page
        .getByRole("option", { name: "Patient instance (single patient)" })
        .click();

      // Enter patient ID.
      await page.getByPlaceholder("e.g., patient-123").fill("test-patient-id");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify status card shows correct export type.
      await expect(page.getByText("Patient Instance Export")).toBeVisible();
    });

    test("executes group export with group ID", async ({ page }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Select Group export level.
      await page.getByRole("combobox").click();
      await page.getByRole("option", { name: "Group (group members)" }).click();

      // Enter group ID.
      await page.getByPlaceholder("e.g., group-456").fill("test-group-id");

      // Start export.
      await page.getByRole("button", { name: "Start export" }).click();

      // Verify status card shows correct export type.
      await expect(page.getByText("Group Export")).toBeVisible();
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

      // Wait for cancel button to appear.
      await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible({
        timeout: 10000,
      });

      // Click cancel.
      await page.getByRole("button", { name: "Cancel" }).click();

      // Verify the export was cancelled (Cancel button disappears, status card remains).
      await expect(
        page.getByRole("button", { name: "Cancel" }),
      ).not.toBeVisible();
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

  test.describe("New export", () => {
    test("New Export button resets state and re-enables form", async ({
      page,
    }) => {
      await setupStandardMocks(page);
      await page.goto("/admin/export");

      // Complete an export.
      await page.getByRole("button", { name: "Start export" }).click();
      await expect(page.getByText("Output files (2)")).toBeVisible({
        timeout: 10000,
      });

      // Click New Export button.
      await page.getByRole("button", { name: "New Export" }).click();

      // Verify status card is no longer visible.
      await expect(page.getByText("System Export")).not.toBeVisible();

      // Verify form is re-enabled.
      await expect(
        page.getByRole("button", { name: "Start export" }),
      ).toBeEnabled();
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
  });
});

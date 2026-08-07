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
 * E2E tests for the export set: composing a mixed basket of subjects on the
 * SQL on FHIR page and exporting them as one `$sql-export` job. All network is
 * mocked with `page.route`.
 *
 * @author John Grimes
 */

import { expect, test } from "@playwright/test";

import {
  mockCapabilityStatement,
  mockSqlQueryLibrary1,
  mockSqlQueryLibraryBundle,
  mockViewDefinitionBundle,
} from "./fixtures/fhirData";

import type { Page, Route } from "@playwright/test";

/** The parsed kick-off body, captured by the export mock. */
interface KickOffBody {
  parameter?: Array<{
    name?: string;
    valueReference?: { reference?: string };
    valueCode?: string;
    part?: Array<{
      name?: string;
      valueString?: string;
      valueReference?: { reference?: string };
    }>;
  }>;
}

/**
 * Mocks the endpoints needed to load the page and list the stored subjects.
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
    const body = route.request().url().includes("sql-query")
      ? mockSqlQueryLibraryBundle
      : { resourceType: "Bundle", type: "searchset", total: 0, entry: [] };
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(body),
    });
  });

  const viewDefinitions = (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockViewDefinitionBundle),
    });
  await page.route("**/ViewDefinition?*", viewDefinitions);
  await page.route(/\/ViewDefinition$/, viewDefinitions);
}

/**
 * Mocks the asynchronous export endpoints, capturing the kick-off body so the
 * test can assert the shape of the job that was started.
 *
 * @param page - The Playwright page.
 * @returns A holder whose `body` is the captured kick-off body.
 */
async function mockExportEndpoints(
  page: Page,
): Promise<{ body?: KickOffBody }> {
  const captured: { body?: KickOffBody } = {};

  await page.route(/\/\$sql-export/, async (route) => {
    captured.body = JSON.parse(
      route.request().postData() ?? "{}",
    ) as KickOffBody;
    await route.fulfill({
      status: 202,
      headers: {
        "Content-Location": "http://localhost:3000/fhir/$job?id=sql-export-set",
        "Access-Control-Expose-Headers": "Content-Location",
      },
      body: "",
    });
  });

  // The status poll returns a completion manifest naming one output per
  // subject, which is what the job card renders as download rows.
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
          { name: "exportId", valueString: "sql-export-set" },
          { name: "status", valueCode: "completed" },
          { name: "_format", valueCode: "ndjson" },
          {
            name: "output",
            part: [
              { name: "name", valueString: "renamed_view" },
              {
                name: "location",
                valueUri:
                  "http://localhost:3000/fhir/$result?job=sql-export-set&file=renamed_view.00000.ndjson",
              },
            ],
          },
          {
            name: "output",
            part: [
              { name: "name", valueString: mockSqlQueryLibrary1.id },
              {
                name: "location",
                valueUri:
                  "http://localhost:3000/fhir/$result?job=sql-export-set&file=query.00000.ndjson",
              },
            ],
          },
        ],
      }),
    });
  });

  await page.route(/\/\$result/, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/octet-stream",
      body: '{"id":"p1"}\n',
    });
  });

  return captured;
}

test.describe("SQL on FHIR page - export set", () => {
  test("exports a mixed set as one job with two named outputs", async ({
    page,
  }) => {
    await mockBaseEndpoints(page);
    const captured = await mockExportEndpoints(page);

    await page.goto("/admin/sql-on-fhir");

    // Add a stored view to the set.
    await page.getByRole("combobox").first().click();
    await page.getByRole("option", { name: "Patient Demographics" }).click();
    await page.getByRole("button", { name: /add to export set/i }).click();

    // Add a stored query to the set.
    await page.getByRole("tab", { name: /^sql query$/i }).click();
    await page.getByRole("combobox", { name: /sql query source/i }).click();
    await page
      .getByRole("option", { name: mockSqlQueryLibrary1.title })
      .click();
    await page
      .getByRole("textbox", { name: /runtime value for patient_id/i })
      .fill("Patient/pat-1");
    await page.getByRole("button", { name: /add to export set/i }).click();

    await expect(page.getByText("Export set (2)")).toBeVisible();

    // Rename the first entry in place.
    const firstName = page.getByRole("textbox", {
      name: /output name for view entry/i,
    });
    await firstName.fill("renamed_view");

    // Apply a job-wide patient filter.
    await page.getByLabel("Patients").fill("pat-1");

    await page.getByRole("button", { name: "Export set", exact: true }).click();

    // One job carrying both subjects and the filter.
    await expect
      .poll(() => captured.body?.parameter?.length ?? 0)
      .toBeGreaterThan(0);
    const parts = captured.body!.parameter!;
    const subjects = parts.filter((p) => p.name === "subject");
    expect(subjects).toHaveLength(2);
    expect(subjects[0].part?.find((p) => p.name === "name")?.valueString).toBe(
      "renamed_view",
    );
    expect(
      subjects[0].part?.find((p) => p.name === "subjectReference")
        ?.valueReference?.reference,
    ).toBe("ViewDefinition/patient-demographics");
    expect(
      subjects[1].part?.find((p) => p.name === "subjectReference")
        ?.valueReference?.reference,
    ).toBe(`Library/${mockSqlQueryLibrary1.id}`);
    expect(
      parts.find((p) => p.name === "patient")?.valueReference?.reference,
    ).toBe("Patient/pat-1");

    // The job card lists one download row per manifest output.
    await expect(page.getByText("renamed_view.00000.ndjson")).toBeVisible();
    await expect(page.getByText("query.00000.ndjson")).toBeVisible();

    // Both outputs download.
    const downloadButtons = page.getByRole("button", { name: /^download$/i });
    await expect(downloadButtons).toHaveCount(2);
    for (const index of [0, 1]) {
      const downloadPromise = page.waitForEvent("download");
      await downloadButtons.nth(index).click();
      await downloadPromise;
    }
  });

  test("blocks the export until a name collision is resolved", async ({
    page,
  }) => {
    await mockBaseEndpoints(page);
    await mockExportEndpoints(page);

    await page.goto("/admin/sql-on-fhir");

    // Add the same stored view twice, then make the two names identical.
    await page.getByRole("combobox").first().click();
    await page.getByRole("option", { name: "Patient Demographics" }).click();
    await page.getByRole("button", { name: /add to export set/i }).click();
    await page.getByRole("button", { name: /add to export set/i }).click();

    const names = page.getByRole("textbox", {
      name: /output name for view entry/i,
    });
    await expect(names).toHaveCount(2);
    await names.nth(1).fill("patient-demographics");

    await expect(
      page.getByRole("button", { name: "Export set", exact: true }),
    ).toBeDisabled();
    await expect(page.getByRole("alert")).toContainText(/distinct name/i);

    // Renaming clears the block.
    await names.nth(1).fill("second_copy");
    await expect(
      page.getByRole("button", { name: "Export set", exact: true }),
    ).toBeEnabled();
  });
});

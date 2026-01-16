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
 * Shared mock helpers for E2E tests.
 *
 * @author John Grimes
 */

import { mockCapabilityStatement } from "../fixtures/fhirData";

import type { Page } from "@playwright/test";

/**
 * Sets up metadata endpoint mock.
 *
 * @param page - The Playwright page object.
 * @param capabilities - The CapabilityStatement to return (defaults to mockCapabilityStatement).
 */
export async function mockMetadata(
  page: Page,
  capabilities: object = mockCapabilityStatement,
): Promise<void> {
  await page.route("**/metadata", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(capabilities),
    });
  });
}

/**
 * Creates an OperationOutcome error response body.
 *
 * @param diagnostics - The error message to include in the OperationOutcome.
 * @returns The OperationOutcome object as a string.
 */
export function createOperationOutcome(diagnostics: string): string {
  return JSON.stringify({
    resourceType: "OperationOutcome",
    issue: [{ severity: "error", diagnostics }],
  });
}

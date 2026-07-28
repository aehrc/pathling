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
 * E2E tests for authentication flows.
 * Tests login prompts, OAuth initiation, callback handling, and error states.
 *
 * @author John Grimes
 */

import { expect, type Page, test } from "@playwright/test";

import {
  mockCapabilityStatement,
  mockCapabilityStatementWithAuth,
  mockSmartConfiguration,
} from "./fixtures/fhirData";

/**
 * Sets up mocks for a server that requires authentication.
 * Mocks the metadata endpoint to indicate SMART-on-FHIR is required.
 *
 * @param page - The Playwright page object.
 */
async function setupAuthRequiredMocks(page: Page) {
  await page.route("**/metadata", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockCapabilityStatementWithAuth),
    });
  });

  await page.route("**/.well-known/smart-configuration", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(mockSmartConfiguration),
    });
  });
}

/**
 * Makes authorisation discovery fail, so that initiating a login rejects.
 * Both the SMART configuration and the conformance statement that fhirclient
 * falls back to are made to fail, which is what a server rejecting an
 * unauthenticated discovery request looks like.
 *
 * @param page - The Playwright page object.
 */
async function breakAuthorisationDiscovery(page: Page) {
  await page.route("**/.well-known/smart-configuration", async (route) => {
    await route.fulfill({ status: 401, body: "" });
  });
  await page.route("**/metadata", async (route) => {
    await route.fulfill({ status: 500, body: "" });
  });
}

/**
 * Establishes an authenticated session by seeding fhirclient state and visiting
 * the OAuth callback, which is the only route that authenticates the
 * application.
 *
 * @param page - The Playwright page object.
 * @param returnUrl - The path to land on once authentication completes.
 */
async function authenticate(page: Page, returnUrl: string) {
  await page.route("**/token", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        access_token: "fake-access-token",
        token_type: "Bearer",
        expires_in: 3600,
        scope: "openid profile user/*.read",
      }),
    });
  });

  await page.goto("/admin");
  await page.evaluate((url) => {
    const stateKey = "test-state-key";
    sessionStorage.setItem("SMART_KEY", JSON.stringify(stateKey));
    sessionStorage.setItem(
      stateKey,
      JSON.stringify({
        clientId: "test-client-id",
        scope: "openid profile user/*.read",
        redirectUri: window.location.origin + "/admin/callback",
        serverUrl: window.location.origin + "/fhir",
        tokenUri: window.location.origin + "/token",
        key: stateKey,
      }),
    );
    sessionStorage.setItem("pathling_return_url", url);
  }, returnUrl);

  await page.goto("/admin/callback?state=test-state-key&code=fake-auth-code");
  await page.waitForURL(`**/admin${returnUrl}`);
}

/**
 * Sets up mocks for a server that does not require authentication.
 *
 * @param page - The Playwright page object.
 */
async function setupNoAuthMocks(page: Page) {
  await page.route("**/metadata", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/fhir+json",
      body: JSON.stringify(mockCapabilityStatement),
    });
  });
}

test.describe("Authentication", () => {
  test.describe("Login", () => {
    test("shows login prompt when auth required but not authenticated", async ({
      page,
    }) => {
      await setupAuthRequiredMocks(page);
      await page.goto("/admin/import");

      // Verify login required message is shown.
      await expect(
        page.getByText("You need to login before you can use this page."),
      ).toBeVisible();

      // Verify login button is present with server name.
      await expect(
        page.getByRole("button", { name: /Login to/ }),
      ).toBeVisible();
    });

    test("login button initiates OAuth flow with correct client ID", async ({
      page,
    }) => {
      await setupAuthRequiredMocks(page);

      // Track navigation attempts to the OAuth authorisation endpoint.
      let authorizeUrl: URL | null = null;

      // Intercept navigation to OAuth endpoint.
      await page.route("**/authorize**", async (route) => {
        authorizeUrl = new URL(route.request().url());
        await route.abort();
      });

      await page.goto("/admin/import");

      // Click the login button.
      await page.getByRole("button", { name: /Login to/ }).click();

      // Verify OAuth redirect was attempted with correct client ID from SMART config.
      await expect
        .poll(() => authorizeUrl !== null, {
          timeout: 5000,
          message: "Expected OAuth redirect to be attempted",
        })
        .toBe(true);

      // Verify the client_id matches the admin_ui_client_id from SMART configuration.
      expect(authorizeUrl!.searchParams.get("client_id")).toBe(
        "test-client-id",
      );
    });

    // Regression for #2676: the login button used to give no feedback at all
    // when authorisation could not be initiated.
    test("shows the failure in the login prompt when authorisation cannot start", async ({
      page,
    }) => {
      await setupAuthRequiredMocks(page);
      await page.goto("/admin/resources");

      const loginButton = page.getByRole("button", { name: /Login to/ });
      await expect(loginButton).toBeVisible();

      await breakAuthorisationDiscovery(page);
      await loginButton.click();

      // The failure is announced and the page has not navigated away.
      await expect(page.getByRole("alert")).toBeVisible();
      await expect(page).toHaveURL(/\/admin\/resources/);

      // The button is usable again, so the attempt can be retried.
      await expect(loginButton).toBeEnabled();
      await loginButton.click();
      await expect(page.getByRole("alert")).toBeVisible();
    });

    // Regression for #2676: the dialog used to close on click and report
    // nothing, leaving the user with no indication of what happened.
    test("shows the failure in the session expiry dialog and keeps it open", async ({
      page,
    }) => {
      // The clock is installed before authenticating, since that navigates, and
      // the page's timers must be under the test's control from the outset.
      await page.clock.install();

      await setupAuthRequiredMocks(page);

      // The first job list request succeeds; every later one reports that the
      // session is no longer valid.
      let jobsCalls = 0;
      await page.route("**/$jobs*", async (route) => {
        jobsCalls += 1;
        if (jobsCalls === 1) {
          await route.fulfill({
            status: 200,
            contentType: "application/fhir+json",
            body: JSON.stringify({ resourceType: "Parameters", parameter: [] }),
          });
          return;
        }
        await route.fulfill({ status: 401, body: "" });
      });

      await authenticate(page, "/jobs");
      await expect(page.getByText("No jobs to show")).toBeVisible();

      // Advancing well past the refresh interval drives the next poll, which
      // fails and so raises the expiry dialog. Driving the clock rather than
      // waiting out the interval keeps the test off the ten second floor that
      // the real interval would otherwise impose.
      await page.clock.runFor(30000);

      const dialog = page.getByRole("alertdialog");
      await expect(dialog).toBeVisible();

      await breakAuthorisationDiscovery(page);
      await dialog.getByRole("button", { name: /log in/i }).click();

      // The dialog stays open and reports the failure inside itself.
      await expect(dialog).toBeVisible();
      await expect(dialog.getByRole("alert")).toBeVisible();

      // Both actions become usable again.
      await expect(
        dialog.getByRole("button", { name: /log in/i }),
      ).toBeEnabled();
      await expect(
        dialog.getByRole("button", { name: /dismiss/i }),
      ).toBeEnabled();
    });

    // Regression for #2676: a deduplication flag that was never reset meant
    // only the first expiry in the life of the page was ever reported.
    test("raises the expiry dialog again after re-authenticating", async ({
      page,
    }) => {
      await setupAuthRequiredMocks(page);
      await page.route("**/$jobs*", async (route) => {
        await route.fulfill({ status: 401, body: "" });
      });

      await authenticate(page, "/jobs");

      const dialog = page.getByRole("alertdialog");
      await expect(dialog).toBeVisible({ timeout: 15000 });

      // Once dismissed, the login prompt stands in for the withdrawn access.
      await dialog.getByRole("button", { name: /dismiss/i }).click();
      await expect(dialog).toBeHidden();
      await expect(
        page.getByText("You need to login before you can use this page."),
      ).toBeVisible();

      // FR-013: no further job list request is made once access is withdrawn.
      let jobsAfterExpiry = 0;
      await page.route("**/$jobs*", async (route) => {
        jobsAfterExpiry += 1;
        await route.fulfill({ status: 401, body: "" });
      });
      await page.waitForTimeout(11000);
      expect(jobsAfterExpiry).toBe(0);

      // Re-authenticate, and let a further request fail.
      await authenticate(page, "/jobs");

      await expect(dialog).toBeVisible({ timeout: 15000 });
    });

    test("login prompt appears on all protected pages", async ({ page }) => {
      await setupAuthRequiredMocks(page);

      // Test multiple protected pages show login prompt.
      const protectedPages = [
        "/admin/import",
        "/admin/export",
        "/admin/resources",
      ];

      for (const pagePath of protectedPages) {
        await page.goto(pagePath);
        await expect(
          page.getByText("You need to login before you can use this page."),
        ).toBeVisible();
      }
    });
  });

  test.describe("Callback", () => {
    test("shows error when OAuth callback fails", async ({ page }) => {
      await setupAuthRequiredMocks(page);

      // Navigate to callback without proper OAuth state.
      await page.goto("/admin/callback");

      // Should show authentication failed error.
      await expect(page.getByRole("alert")).toContainText(
        "Authentication failed",
      );
    });

    test("shows error with missing OAuth parameters", async ({ page }) => {
      await setupAuthRequiredMocks(page);

      // Navigate to callback with only state but no code.
      await page.goto("/admin/callback?state=invalid-state");

      // Should show authentication failed error.
      await expect(page.getByRole("alert")).toContainText(
        "Authentication failed",
      );
    });

    test("redirects to stored return URL after successful auth", async ({
      page,
    }) => {
      await setupAuthRequiredMocks(page);

      // Mock the token endpoint.
      await page.route("**/token", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            access_token: "fake-access-token",
            token_type: "Bearer",
            expires_in: 3600,
            scope: "openid profile user/*.read",
          }),
        });
      });

      // Set up fhirclient state and return URL.
      await page.goto("/admin");
      await page.evaluate(() => {
        const stateKey = "test-state-key";

        // fhirclient stores state key reference in SMART_KEY.
        sessionStorage.setItem("SMART_KEY", JSON.stringify(stateKey));

        // fhirclient stores actual state under the state key.
        // Note: Uses the admin_ui_client_id from the mock SMART configuration.
        sessionStorage.setItem(
          stateKey,
          JSON.stringify({
            clientId: "test-client-id",
            scope: "openid profile user/*.read",
            redirectUri: window.location.origin + "/admin/callback",
            serverUrl: window.location.origin + "/fhir",
            tokenUri: window.location.origin + "/token",
            key: stateKey,
          }),
        );

        // Our return URL.
        sessionStorage.setItem("pathling_return_url", "/resources");
      });

      // Navigate to callback with matching state param.
      await page.goto(
        "/admin/callback?state=test-state-key&code=fake-auth-code",
      );

      // Wait for redirect to the stored URL.
      await page.waitForURL("**/admin/resources");

      // Verify we're on the resources page.
      await expect(page.getByText("Search resources")).toBeVisible();
    });
  });

  test.describe("No auth required", () => {
    test("allows access without login when auth not required", async ({
      page,
    }) => {
      await setupNoAuthMocks(page);
      await page.goto("/admin/import");

      // Login prompt should NOT be shown.
      await expect(
        page.getByText("You need to login before you can use this page."),
      ).not.toBeVisible();

      // Import form should be visible.
      await expect(
        page.getByRole("tab", { name: "Import from URLs" }),
      ).toBeVisible();

      // Logout button should NOT be visible (no auth means no session).
      await expect(page.getByText("Logout")).not.toBeVisible();
    });

    test("all pages accessible without auth when not required", async ({
      page,
    }) => {
      await setupNoAuthMocks(page);

      // Export page.
      await page.goto("/admin/export");
      await expect(page.getByText("New export")).toBeVisible();

      // Import page.
      await page.goto("/admin/import");
      await expect(
        page.getByRole("tab", { name: "Import from URLs" }),
      ).toBeVisible();

      // Resources page.
      await page.goto("/admin/resources");
      await expect(page.getByText("Search resources")).toBeVisible();
    });
  });

  test.describe("Server capability detection", () => {
    test("detects SMART-on-FHIR requirement from capability statement", async ({
      page,
    }) => {
      await setupAuthRequiredMocks(page);
      await page.goto("/admin/import");

      // Auth is required, so login prompt should appear.
      await expect(
        page.getByText("You need to login before you can use this page."),
      ).toBeVisible();
    });

    test("detects no auth requirement from capability statement", async ({
      page,
    }) => {
      await setupNoAuthMocks(page);
      await page.goto("/admin/import");

      // No auth required, so content should be visible.
      await expect(
        page.getByRole("tab", { name: "Import from URLs" }),
      ).toBeVisible();
    });

    test("shows loading state while checking capabilities", async ({
      page,
    }) => {
      // Delay the metadata response to observe loading state.
      await page.route("**/metadata", async (route) => {
        await new Promise((resolve) => setTimeout(resolve, 500));
        await route.fulfill({
          status: 200,
          contentType: "application/fhir+json",
          body: JSON.stringify(mockCapabilityStatement),
        });
      });

      await page.goto("/admin/import");

      // Should show loading message.
      await expect(
        page.getByText("Checking server capabilities..."),
      ).toBeVisible();
    });
  });
});

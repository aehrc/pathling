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
 * Sets up mocks for a server that does not require authentication.
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

    test("login button initiates OAuth flow", async ({ page }) => {
      await setupAuthRequiredMocks(page);

      // Track navigation attempts to the OAuth authorization endpoint.
      let oauthRedirectAttempted = false;

      // Intercept navigation to OAuth endpoint.
      await page.route("**/authorize**", async (route) => {
        oauthRedirectAttempted = true;
        await route.abort();
      });

      await page.goto("/admin/import");

      // Click the login button.
      await page.getByRole("button", { name: /Login to/ }).click();

      // Verify OAuth redirect was attempted.
      await expect
        .poll(() => oauthRedirectAttempted, {
          timeout: 5000,
          message: "Expected OAuth redirect to be attempted",
        })
        .toBe(true);
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
      await expect(page.getByText("Authentication Failed")).toBeVisible();
    });

    test("shows error with missing OAuth parameters", async ({ page }) => {
      await setupAuthRequiredMocks(page);

      // Navigate to callback with only state but no code.
      await page.goto("/admin/callback?state=invalid-state");

      // Should show authentication failed error.
      await expect(page.getByText("Authentication Failed")).toBeVisible();
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
        sessionStorage.setItem(
          stateKey,
          JSON.stringify({
            clientId: "pathling-export-ui",
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

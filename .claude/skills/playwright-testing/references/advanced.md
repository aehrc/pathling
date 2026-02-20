# Advanced Features Reference

## Contents

1. [Network Mocking](#network-mocking)
2. [API Testing](#api-testing)
3. [Authentication](#authentication)
4. [Fixtures](#fixtures)
5. [Page Object Model](#page-object-model)
6. [Multiple Pages and Contexts](#multiple-pages-and-contexts)
7. [Downloads](#downloads)
8. [Dialogs](#dialogs)

## Network Mocking

### Route Requests

```typescript
// Mock API response
await page.route("**/api/users", (route) =>
    route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([{ id: 1, name: "Mock User" }]),
    }),
);

// Shorthand for JSON
await page.route("**/api/users", (route) =>
    route.fulfill({ json: [{ id: 1, name: "Mock User" }] }),
);
```

### Modify Requests

```typescript
await page.route("**/api/**", (route) => {
    const headers = {
        ...route.request().headers(),
        "X-Custom-Header": "value",
    };
    route.continue({ headers });
});
```

### Modify Responses

```typescript
await page.route("**/api/users", async (route) => {
    const response = await route.fetch();
    const json = await response.json();
    json.push({ id: 99, name: "Injected User" });
    route.fulfill({ json });
});
```

### Abort Requests

```typescript
// Block images
await page.route("**/*.{png,jpg,jpeg,gif}", (route) => route.abort());

// Block by resource type
await page.route("**/*", (route) => {
    if (route.request().resourceType() === "image") {
        route.abort();
    } else {
        route.continue();
    }
});
```

### Error Responses

```typescript
await page.route("**/api/submit", (route) =>
    route.fulfill({
        status: 500,
        body: JSON.stringify({ error: "Server error" }),
    }),
);

await page.route("**/api/timeout", (route) => route.abort("timedout"));
```

### Wait for Responses

```typescript
const responsePromise = page.waitForResponse("**/api/data");
await page.getByRole("button", { name: "Load" }).click();
const response = await responsePromise;

expect(response.status()).toBe(200);
const data = await response.json();
```

### HAR Recording

```typescript
// Record
await page.routeFromHAR("recordings/api.har", { update: true });
await page.goto("/dashboard");
// ... interactions recorded

// Replay
await page.routeFromHAR("recordings/api.har");
```

### Context-Level Routes

```typescript
// Apply to all pages in context
await context.route("**/api/**", (route) =>
    route.fulfill({ json: { mocked: true } }),
);
```

### Remove Routes

```typescript
await page.unroute("**/api/users");
await page.unrouteAll();
```

## API Testing

### Basic Requests

```typescript
test("API test", async ({ request }) => {
    // GET
    const response = await request.get("/api/users");
    expect(response.ok()).toBeTruthy();
    const users = await response.json();

    // POST
    const createResponse = await request.post("/api/users", {
        data: { name: "New User", email: "new@example.com" },
    });
    expect(createResponse.status()).toBe(201);

    // PUT
    await request.put("/api/users/1", {
        data: { name: "Updated" },
    });

    // DELETE
    await request.delete("/api/users/1");
});
```

### Request Options

```typescript
const response = await request.post("/api/data", {
    data: { key: "value" }, // JSON body
    form: { field: "value" }, // Form data
    multipart: {
        // Multipart form
        file: {
            name: "file.txt",
            mimeType: "text/plain",
            buffer: Buffer.from("content"),
        },
    },
    headers: { "X-Custom": "header" },
    params: { page: "1" }, // Query params
    timeout: 5000,
    failOnStatusCode: true, // Throw on 4xx/5xx
});
```

### Setup via API

```typescript
test.beforeAll(async ({ request }) => {
    // Create test data
    await request.post("/api/users", {
        data: { name: "Test User" },
    });
});

test.afterAll(async ({ request }) => {
    // Cleanup
    await request.delete("/api/test-data");
});
```

### Standalone API Context

```typescript
import { request } from "@playwright/test";

test("standalone API", async () => {
    const apiContext = await request.newContext({
        baseURL: "https://api.example.com",
        extraHTTPHeaders: {
            Authorization: `Bearer ${process.env.API_TOKEN}`,
        },
    });

    const response = await apiContext.get("/data");
    await apiContext.dispose();
});
```

## Authentication

### Save Storage State

```typescript
// After login
await page.context().storageState({ path: "auth.json" });
```

### Reuse in Config

```typescript
// playwright.config.ts
use: {
  storageState: 'auth.json',
}
```

### Setup Project Pattern

```typescript
// playwright.config.ts
projects: [
    {
        name: "setup",
        testMatch: /auth.setup\.ts/,
    },
    {
        name: "chromium",
        use: {
            ...devices["Desktop Chrome"],
            storageState: "playwright/.auth/user.json",
        },
        dependencies: ["setup"],
    },
];
```

```typescript
// auth.setup.ts
import { test as setup, expect } from "@playwright/test";

const authFile = "playwright/.auth/user.json";

setup("authenticate", async ({ page }) => {
    await page.goto("/login");
    await page.getByLabel("Email").fill("user@example.com");
    await page.getByLabel("Password").fill("password");
    await page.getByRole("button", { name: "Sign in" }).click();
    await expect(page.getByText("Dashboard")).toBeVisible();
    await page.context().storageState({ path: authFile });
});
```

### Multiple Roles

```typescript
// playwright.config.ts
projects: [
    { name: "setup", testMatch: /.*\.setup\.ts/ },
    {
        name: "admin",
        use: { storageState: "playwright/.auth/admin.json" },
        dependencies: ["setup"],
        testMatch: "**/admin/**",
    },
    {
        name: "user",
        use: { storageState: "playwright/.auth/user.json" },
        dependencies: ["setup"],
        testMatch: "**/user/**",
    },
];
```

### Per-Test Auth

```typescript
test.use({ storageState: "admin-auth.json" });

test("admin feature", async ({ page }) => {
    // Runs with admin auth
});
```

### API Authentication

```typescript
// auth.setup.ts
setup("authenticate via API", async ({ request }) => {
    const response = await request.post("/api/login", {
        data: { email: "user@example.com", password: "password" },
    });
    const { token } = await response.json();

    // Save to storage state
    await request.storageState({ path: "auth.json" });
});
```

### Per-Worker Auth

```typescript
// fixtures.ts
import { test as base } from "@playwright/test";

const users = ["user1@example.com", "user2@example.com", "user3@example.com"];

export const test = base.extend({
    account: async ({}, use, testInfo) => {
        const email = users[testInfo.parallelIndex % users.length];
        await use({ email, password: "password" });
    },
});
```

## Fixtures

### Built-in Fixtures

- `page` — Isolated page per test
- `context` — Browser context containing page
- `browser` — Shared browser instance
- `browserName` — 'chromium' | 'firefox' | 'webkit'
- `request` — API request context

### Custom Fixtures

```typescript
// fixtures.ts
import { test as base, Page } from "@playwright/test";

type Fixtures = {
    adminPage: Page;
    userPage: Page;
};

export const test = base.extend<Fixtures>({
    adminPage: async ({ browser }, use) => {
        const context = await browser.newContext({
            storageState: "admin-auth.json",
        });
        const page = await context.newPage();
        await use(page);
        await context.close();
    },

    userPage: async ({ browser }, use) => {
        const context = await browser.newContext({
            storageState: "user-auth.json",
        });
        const page = await context.newPage();
        await use(page);
        await context.close();
    },
});

export { expect } from "@playwright/test";
```

### Fixture with Setup/Teardown

```typescript
const test = base.extend<{ todoPage: TodoPage }>({
    todoPage: async ({ page }, use) => {
        // Setup
        const todoPage = new TodoPage(page);
        await todoPage.goto();
        await todoPage.createDefaultTodos();

        // Provide to test
        await use(todoPage);

        // Teardown
        await todoPage.deleteAllTodos();
    },
});
```

### Worker-Scoped Fixtures

Shared across tests in same worker:

```typescript
const test = base.extend<{}, { dbConnection: Database }>({
    dbConnection: [
        async ({}, use) => {
            const db = await Database.connect();
            await use(db);
            await db.close();
        },
        { scope: "worker" },
    ],
});
```

### Auto Fixtures

Run automatically without explicit use:

```typescript
const test = base.extend({
    logger: [
        async ({}, use) => {
            console.log("Test starting");
            await use();
            console.log("Test finished");
        },
        { auto: true },
    ],
});
```

### Fixture Options

```typescript
const test = base.extend<{ locale: string }>({
    locale: ["en-AU", { option: true }],
});

// Override in test
test.use({ locale: "en-US" });
```

## Page Object Model

### Basic POM

```typescript
// pages/login.page.ts
import { Page, Locator } from "@playwright/test";

export class LoginPage {
    readonly page: Page;
    readonly emailInput: Locator;
    readonly passwordInput: Locator;
    readonly submitButton: Locator;

    constructor(page: Page) {
        this.page = page;
        this.emailInput = page.getByLabel("Email");
        this.passwordInput = page.getByLabel("Password");
        this.submitButton = page.getByRole("button", { name: "Sign in" });
    }

    async goto() {
        await this.page.goto("/login");
    }

    async login(email: string, password: string) {
        await this.emailInput.fill(email);
        await this.passwordInput.fill(password);
        await this.submitButton.click();
    }
}
```

### Use in Tests

```typescript
import { test, expect } from "@playwright/test";
import { LoginPage } from "./pages/login.page";

test("user can login", async ({ page }) => {
    const loginPage = new LoginPage(page);
    await loginPage.goto();
    await loginPage.login("user@example.com", "password");
    await expect(page.getByText("Dashboard")).toBeVisible();
});
```

### POM as Fixture

```typescript
// fixtures.ts
import { test as base } from "@playwright/test";
import { LoginPage } from "./pages/login.page";
import { DashboardPage } from "./pages/dashboard.page";

type Pages = {
    loginPage: LoginPage;
    dashboardPage: DashboardPage;
};

export const test = base.extend<Pages>({
    loginPage: async ({ page }, use) => {
        await use(new LoginPage(page));
    },
    dashboardPage: async ({ page }, use) => {
        await use(new DashboardPage(page));
    },
});
```

```typescript
// login.spec.ts
import { test, expect } from "./fixtures";

test("login flow", async ({ loginPage, dashboardPage }) => {
    await loginPage.goto();
    await loginPage.login("user@example.com", "password");
    await expect(dashboardPage.welcomeMessage).toBeVisible();
});
```

## Multiple Pages and Contexts

### New Page in Same Context

```typescript
const newPage = await context.newPage();
await newPage.goto("/other-page");
```

### New Context (Isolated)

```typescript
const newContext = await browser.newContext();
const newPage = await newContext.newPage();
// Has separate cookies, storage
```

### Handle Popups

```typescript
const popupPromise = page.waitForEvent("popup");
await page.getByRole("link", { name: "Open popup" }).click();
const popup = await popupPromise;
await popup.waitForLoadState();
await expect(popup.getByText("Popup content")).toBeVisible();
```

### Multiple Contexts

```typescript
test("multi-user interaction", async ({ browser }) => {
    const userContext = await browser.newContext({
        storageState: "user-auth.json",
    });
    const adminContext = await browser.newContext({
        storageState: "admin-auth.json",
    });

    const userPage = await userContext.newPage();
    const adminPage = await adminContext.newPage();

    // Interact as both users
    await userPage.goto("/submit-request");
    await adminPage.goto("/admin/requests");

    await userContext.close();
    await adminContext.close();
});
```

## Downloads

### Wait for Download

```typescript
const downloadPromise = page.waitForEvent("download");
await page.getByRole("button", { name: "Download" }).click();
const download = await downloadPromise;

// Save to specific path
await download.saveAs("/path/to/save.pdf");

// Get suggested filename
const filename = download.suggestedFilename();

// Get download path
const path = await download.path();
```

### Configure Downloads

```typescript
// playwright.config.ts
use: {
  acceptDownloads: true,
}
```

## Dialogs

### Handle Alerts

```typescript
page.on("dialog", (dialog) => dialog.accept());
await page.getByRole("button", { name: "Delete" }).click();
```

### Handle Confirms

```typescript
page.on("dialog", (dialog) => {
    expect(dialog.message()).toContain("Are you sure?");
    dialog.accept();
});
```

### Handle Prompts

```typescript
page.on("dialog", (dialog) => {
    dialog.accept("User input");
});
```

### Dismiss Dialog

```typescript
page.on("dialog", (dialog) => dialog.dismiss());
```

### One-Time Handler

```typescript
page.once("dialog", (dialog) => dialog.accept());
await page.getByRole("button", { name: "Confirm" }).click();
```

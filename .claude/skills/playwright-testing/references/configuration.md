# Configuration Reference

## Contents

1. [Basic Configuration](#basic-configuration)
2. [Test Options](#test-options)
3. [Browser Options](#browser-options)
4. [Projects](#projects)
5. [Parallel Execution](#parallel-execution)
6. [Retries](#retries)
7. [Reporters](#reporters)
8. [Web Server](#web-server)
9. [Global Setup/Teardown](#global-setupteardown)
10. [CLI Commands](#cli-commands)

## Basic Configuration

```typescript
// playwright.config.ts
import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
    testDir: "./tests",
    fullyParallel: true,
    forbidOnly: !!process.env.CI,
    retries: process.env.CI ? 2 : 0,
    workers: process.env.CI ? 2 : undefined,
    reporter: "html",
    use: {
        baseURL: "http://localhost:3000",
        trace: "on-first-retry",
    },
    projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
```

## Test Options

### Test Directory and Files

```typescript
testDir: './tests',                    // Test files location
testMatch: '**/*test.ts',              // Glob pattern for test files
testIgnore: '**/*.skip.ts',            // Ignore pattern
```

### Timeouts

```typescript
timeout: 30000,                        // Per-test timeout (30s default)
globalTimeout: 600000,                 // Total test run limit
expect: {
  timeout: 5000,                       // Assertion timeout (5s default)
  toHaveScreenshot: { timeout: 10000 },
  toMatchSnapshot: { timeout: 10000 },
},
```

### Output

```typescript
outputDir: './test-results',           // Artifacts folder
preserveOutput: 'failures-only',       // 'always' | 'never' | 'failures-only'
snapshotDir: './snapshots',            // Screenshot snapshots
snapshotPathTemplate: '{testDir}/__snapshots__/{testFilePath}/{arg}{ext}',
```

### Behaviour

```typescript
forbidOnly: true,                      // Fail if test.only exists
maxFailures: 10,                       // Stop after N failures (0 = unlimited)
repeatEach: 1,                         // Run each test N times
grep: /login/,                         // Filter tests by title
grepInvert: /skip/,                    // Exclude tests by title
shard: { current: 1, total: 3 },       // Distribute tests
```

## Browser Options

Configure in `use` section:

```typescript
use: {
  // Browser settings
  headless: true,
  channel: 'chrome',                   // 'chrome' | 'msedge' | 'chrome-beta' etc.
  launchOptions: {
    slowMo: 100,                       // Slow down actions
    args: ['--disable-web-security'],
  },

  // Context settings
  viewport: { width: 1280, height: 720 },
  ignoreHTTPSErrors: true,
  locale: 'en-AU',
  timezoneId: 'Australia/Sydney',
  geolocation: { latitude: -33.8688, longitude: 151.2093 },
  permissions: ['geolocation'],
  colorScheme: 'dark',                 // 'light' | 'dark' | 'no-preference'
  reducedMotion: 'reduce',
  forcedColors: 'active',

  // Network
  baseURL: 'http://localhost:3000',
  extraHTTPHeaders: { 'X-Custom': 'value' },
  httpCredentials: { username: 'user', password: 'pass' },
  offline: false,
  proxy: { server: 'http://proxy:8080' },

  // Recording
  screenshot: 'only-on-failure',       // 'off' | 'on' | 'only-on-failure'
  video: 'retain-on-failure',          // 'off' | 'on' | 'retain-on-failure'
  trace: 'on-first-retry',             // 'off' | 'on' | 'retain-on-failure' | 'on-first-retry'

  // Timeouts
  actionTimeout: 10000,
  navigationTimeout: 30000,

  // Test IDs
  testIdAttribute: 'data-testid',

  // Storage state
  storageState: 'auth.json',
}
```

## Projects

Run tests across different configurations:

```typescript
projects: [
    // Desktop browsers
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "firefox", use: { ...devices["Desktop Firefox"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },

    // Mobile browsers
    { name: "mobile-chrome", use: { ...devices["Pixel 5"] } },
    { name: "mobile-safari", use: { ...devices["iPhone 12"] } },

    // Branded browsers
    {
        name: "chrome",
        use: { ...devices["Desktop Chrome"], channel: "chrome" },
    },
    { name: "edge", use: { ...devices["Desktop Edge"], channel: "msedge" } },

    // Custom configuration
    {
        name: "admin",
        use: {
            ...devices["Desktop Chrome"],
            storageState: "admin-auth.json",
        },
        testMatch: "**/admin/*.spec.ts",
    },
];
```

### Project Dependencies

```typescript
projects: [
    {
        name: "setup",
        testMatch: /global.setup\.ts/,
    },
    {
        name: "chromium",
        use: { ...devices["Desktop Chrome"] },
        dependencies: ["setup"],
    },
];
```

## Parallel Execution

### Workers

```typescript
workers: 4,                            // Fixed count
workers: '50%',                        // Percentage of CPU cores
workers: process.env.CI ? 2 : undefined,
```

### Parallelisation Modes

```typescript
fullyParallel: true,                   // All tests in parallel
```

Per-file control:

```typescript
test.describe.configure({ mode: "parallel" });
test.describe.configure({ mode: "serial" });
```

### Sharding

Distribute across machines:

```bash
npx playwright test --shard=1/3
npx playwright test --shard=2/3
npx playwright test --shard=3/3
```

Access worker info:

```typescript
test("example", async ({ page }, testInfo) => {
    console.log(testInfo.workerIndex); // 0, 1, 2...
    console.log(testInfo.parallelIndex); // Unique per worker
});
```

## Retries

```typescript
retries: 2,                            // Retry failed tests twice
```

Per-project:

```typescript
projects: [
    {
        name: "chromium",
        retries: 3,
    },
];
```

Per-file:

```typescript
test.describe.configure({ retries: 2 });
```

Access retry info:

```typescript
test.beforeEach(async ({ page }, testInfo) => {
    if (testInfo.retry > 0) {
        // Clean up before retry
    }
});
```

## Reporters

### Built-in Reporters

```typescript
reporter: 'list',                      // Line per test (default local)
reporter: 'dot',                       // Minimal dots (default CI)
reporter: 'line',                      // Single progress line
reporter: 'html',                      // Interactive HTML report
reporter: 'json',                      // JSON output
reporter: 'junit',                     // JUnit XML
reporter: 'github',                    // GitHub Actions annotations
reporter: 'blob',                      // For merging sharded results
```

### Multiple Reporters

```typescript
reporter: [
  ['list'],
  ['html', { outputFolder: 'reports/html' }],
  ['junit', { outputFile: 'reports/junit.xml' }],
],
```

### Reporter Options

```typescript
reporter: [
    [
        "html",
        {
            outputFolder: "playwright-report",
            open: "never", // 'always' | 'never' | 'on-failure'
        },
    ],
    [
        "json",
        {
            outputFile: "test-results.json",
        },
    ],
    [
        "junit",
        {
            outputFile: "junit.xml",
            embedAnnotationsAsProperties: true,
        },
    ],
];
```

## Web Server

Auto-start dev server before tests:

```typescript
webServer: {
  command: 'npm run dev',
  url: 'http://localhost:3000',
  reuseExistingServer: !process.env.CI,
  timeout: 120000,
  stdout: 'pipe',
  stderr: 'pipe',
}
```

Multiple servers:

```typescript
webServer: [
    {
        command: "npm run dev:frontend",
        url: "http://localhost:3000",
    },
    {
        command: "npm run dev:api",
        url: "http://localhost:4000",
    },
];
```

## Global Setup/Teardown

```typescript
globalSetup: './global-setup.ts',
globalTeardown: './global-teardown.ts',
```

Example global setup:

```typescript
// global-setup.ts
import { chromium, FullConfig } from "@playwright/test";

export default async function globalSetup(config: FullConfig) {
    const browser = await chromium.launch();
    const page = await browser.newPage();

    // Perform login
    await page.goto("http://localhost:3000/login");
    await page.getByLabel("Email").fill("admin@example.com");
    await page.getByLabel("Password").fill("password");
    await page.getByRole("button", { name: "Login" }).click();

    // Save auth state
    await page.context().storageState({ path: "auth.json" });
    await browser.close();
}
```

## CLI Commands

### Run Tests

```bash
npx playwright test                    # All tests
npx playwright test login.spec.ts      # Specific file
npx playwright test tests/auth/        # Directory
npx playwright test -g "login"         # Filter by title
npx playwright test --project=chromium # Specific project
npx playwright test --headed           # Show browser
npx playwright test --debug            # Step-through debugger
npx playwright test --ui               # Interactive UI mode
```

### Execution Control

```bash
npx playwright test --workers=4        # Worker count
npx playwright test --retries=2        # Retry count
npx playwright test --max-failures=5   # Stop after failures
npx playwright test --repeat-each=3    # Run each test N times
npx playwright test --shard=1/3        # Sharding
npx playwright test --last-failed      # Re-run failures only
```

### Output Control

```bash
npx playwright test --reporter=list
npx playwright test --reporter=json --output=results.json
npx playwright test --trace=on
npx playwright test --screenshot=on
npx playwright test --video=on
```

### Update Snapshots

```bash
npx playwright test --update-snapshots
npx playwright test -u
```

### Utilities

```bash
npx playwright show-report             # View HTML report
npx playwright show-trace trace.zip    # View trace file
npx playwright codegen                 # Record tests
npx playwright codegen http://localhost:3000
npx playwright install                 # Install browsers
npx playwright install chromium
npx playwright install --with-deps     # With system dependencies
```

### Environment Variables

```bash
PWDEBUG=1 npx playwright test         # Debug mode
PWDEBUG=console npx playwright test   # Console debug helpers
CI=true npx playwright test           # CI mode
```

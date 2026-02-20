# Assertions Reference

## Contents

1. [Auto-Retrying Assertions](#auto-retrying-assertions)
2. [Non-Retrying Assertions](#non-retrying-assertions)
3. [Soft Assertions](#soft-assertions)
4. [Polling and Retrying](#polling-and-retrying)
5. [Custom Matchers](#custom-matchers)
6. [Configuration](#configuration)

## Auto-Retrying Assertions

These assertions wait and retry until condition is met (default timeout: 5 seconds).

### Locator State

```typescript
await expect(locator).toBeVisible();
await expect(locator).toBeHidden();
await expect(locator).toBeAttached();
await expect(locator).toBeEnabled();
await expect(locator).toBeDisabled();
await expect(locator).toBeEditable();
await expect(locator).toBeFocused();
await expect(locator).toBeChecked();
await expect(locator).toBeEmpty();
await expect(locator).toBeInViewport();
```

### Locator Content

```typescript
await expect(locator).toHaveText("Hello");
await expect(locator).toHaveText(/hello/i);
await expect(locator).toHaveText(["Item 1", "Item 2"]); // For lists
await expect(locator).toContainText("partial");
await expect(locator).toHaveValue("input text");
await expect(locator).toHaveValues(["opt1", "opt2"]); // Multi-select
await expect(locator).toHaveCount(5);
```

### Locator Attributes

```typescript
await expect(locator).toHaveAttribute("href", "/home");
await expect(locator).toHaveAttribute("href", /^\//);
await expect(locator).toHaveClass("btn-primary");
await expect(locator).toHaveClass(/active/);
await expect(locator).toContainClass("btn");
await expect(locator).toHaveId("submit-btn");
await expect(locator).toHaveCSS("color", "rgb(255, 0, 0)");
await expect(locator).toHaveJSProperty("checked", true);
```

### Accessibility

```typescript
await expect(locator).toHaveAccessibleName("Submit form");
await expect(locator).toHaveAccessibleDescription("Click to submit");
await expect(locator).toHaveRole("button");
```

### Page Assertions

```typescript
await expect(page).toHaveURL("https://example.com");
await expect(page).toHaveURL(/dashboard/);
await expect(page).toHaveTitle("Home Page");
await expect(page).toHaveTitle(/home/i);
```

### Visual Comparison

```typescript
await expect(locator).toHaveScreenshot("button.png");
await expect(page).toHaveScreenshot("page.png");
await expect(locator).toMatchAriaSnapshot(`
  - button "Submit"
  - textbox "Email"
`);
```

### Response Assertions

```typescript
await expect(response).toBeOK(); // Status 200-299
```

### Negation

```typescript
await expect(locator).not.toBeVisible();
await expect(locator).not.toHaveText("Error");
await expect(page).not.toHaveURL(/error/);
```

### Custom Timeout

```typescript
await expect(locator).toBeVisible({ timeout: 10000 });
```

## Non-Retrying Assertions

Test once without retry—use for static values.

### Equality

```typescript
expect(value).toBe(5); // Strict equality (===)
expect(obj).toEqual({ key: "value" }); // Deep equality
expect(obj).toStrictEqual({ key: "v" }); // Deep + type checking
```

### Truthiness

```typescript
expect(value).toBeTruthy();
expect(value).toBeFalsy();
expect(value).toBeNull();
expect(value).toBeUndefined();
expect(value).toBeDefined();
expect(value).toBeNaN();
```

### Numbers

```typescript
expect(num).toBeGreaterThan(5);
expect(num).toBeGreaterThanOrEqual(5);
expect(num).toBeLessThan(10);
expect(num).toBeLessThanOrEqual(10);
expect(0.1 + 0.2).toBeCloseTo(0.3, 5); // Floating point
```

### Strings

```typescript
expect(str).toMatch(/pattern/);
expect(str).toContain("substring");
```

### Collections

```typescript
expect(arr).toContain("item");
expect(arr).toContainEqual({ id: 1 }); // Deep equality
expect(arr).toHaveLength(3);
expect(obj).toHaveProperty("key.nested", "value");
expect(obj).toMatchObject({ partial: "match" });
```

### Functions

```typescript
expect(() => fn()).toThrow();
expect(() => fn()).toThrow("error message");
expect(() => fn()).toThrow(/pattern/);
expect(obj).toBeInstanceOf(MyClass);
```

## Soft Assertions

Continue test execution after failures. Collect all failures before marking test as failed.

```typescript
await expect.soft(locator).toHaveText("Expected");
await expect.soft(locator).toBeVisible();

// Check if any soft assertions failed
if (test.info().errors.length > 0) {
    // Handle accumulated failures
}
```

With custom message:

```typescript
expect.soft(value, "Should be valid ID").toBe(123);
```

## Polling and Retrying

### expect.poll()

Convert synchronous assertions to polling:

```typescript
await expect
    .poll(async () => {
        const response = await page.request.get("/api/status");
        return response.status();
    })
    .toBe(200);

// With options
await expect
    .poll(
        async () => {
            return await page.evaluate(() => window.loadComplete);
        },
        {
            intervals: [100, 200, 500],
            timeout: 10000,
            message: "Should complete loading",
        },
    )
    .toBe(true);
```

### expect.toPass()

Retry entire code blocks:

```typescript
await expect(async () => {
    const response = await page.request.get("/api/data");
    expect(response.status()).toBe(200);
    const data = await response.json();
    expect(data.items).toHaveLength(10);
}).toPass({
    intervals: [1000, 2000, 5000],
    timeout: 30000,
});
```

Note: `toPass` has timeout of 0 by default.

## Custom Matchers

### Define Custom Matcher

```typescript
import { expect as baseExpect } from "@playwright/test";
import type { Locator } from "@playwright/test";

export const expect = baseExpect.extend({
    async toHaveAmount(
        locator: Locator,
        expected: number,
        options?: { timeout?: number },
    ) {
        const actual = await locator.getAttribute("data-amount");
        const pass = actual === String(expected);

        return {
            pass,
            message: () =>
                pass
                    ? `Expected amount not to be ${expected}`
                    : `Expected amount ${expected}, got ${actual}`,
        };
    },
});
```

### Use Custom Matcher

```typescript
import { test, expect } from "./fixtures";

test("check amount", async ({ page }) => {
    await expect(page.locator(".cart")).toHaveAmount(4);
});
```

### Merge Multiple Extensions

```typescript
import { mergeExpects } from "@playwright/test";
import { expect as dbExpect } from "./db-expects";
import { expect as a11yExpect } from "./a11y-expects";

export const expect = mergeExpects(dbExpect, a11yExpect);
```

## Configuration

### Configure Timeout

```typescript
// playwright.config.ts
export default defineConfig({
    expect: {
        timeout: 10000, // 10 seconds for auto-retrying assertions
    },
});
```

### Pre-configured Expect

```typescript
// Slow assertions
const slowExpect = expect.configure({ timeout: 30000 });
await slowExpect(locator).toBeVisible();

// Soft by default
const softExpect = expect.configure({ soft: true });
await softExpect(locator).toHaveText("Value");
```

### Custom Messages

```typescript
await expect(locator, "Submit button should be visible").toBeVisible();
expect(value, "User ID should match session").toBe(sessionUserId);
```

## Asymmetric Matchers

Flexible pattern matching:

```typescript
expect(obj).toEqual({
    id: expect.any(Number),
    name: expect.any(String),
    email: expect.stringContaining("@"),
    tags: expect.arrayContaining(["active"]),
    metadata: expect.objectContaining({ version: 1 }),
});

expect(value).toEqual(expect.anything()); // Not null/undefined
expect(num).toEqual(expect.closeTo(0.3, 2));
expect(str).toEqual(expect.stringMatching(/^user_/));
```

## Best Practices

1. **Always use auto-retrying assertions for web elements** — Prevents flaky tests
2. **Add descriptive messages** — Helps debugging failures
3. **Use soft assertions for comprehensive validation** — Collect multiple failures
4. **Configure appropriate timeouts** — Default 5s may be too short for slow pages
5. **Prefer specific assertions** — `toHaveText` over `toContainText` when possible

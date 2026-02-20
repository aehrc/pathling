# Locators Reference

## Contents

1. [Built-in Locators](#built-in-locators)
2. [Chaining and Filtering](#chaining-and-filtering)
3. [List Operations](#list-operations)
4. [Legacy Locators](#legacy-locators)
5. [Framework-Specific Locators](#framework-specific-locators)

## Built-in Locators

### Role-Based (Most Recommended)

```typescript
page.getByRole("button", { name: "Submit" });
page.getByRole("link", { name: "Home" });
page.getByRole("textbox", { name: "Email" });
page.getByRole("checkbox", { name: "Subscribe" });
page.getByRole("heading", { level: 1 });
page.getByRole("listitem");
page.getByRole("dialog");
page.getByRole("tab", { selected: true });
```

Options:

- `name` — Accessible name (exact or regex)
- `exact` — Whether name match is exact (default: false)
- `checked` — For checkboxes/radios
- `disabled` — Match disabled state
- `expanded` — For expandable elements
- `level` — For headings (1-6)
- `pressed` — For toggle buttons
- `selected` — For options/tabs

### Form Labels

```typescript
page.getByLabel("Email address");
page.getByLabel("Password", { exact: true });
page.getByLabel(/remember me/i);
```

Associates form controls via `<label>` elements or `aria-labelledby`.

### Text Content

```typescript
page.getByText("Welcome");
page.getByText("Welcome", { exact: true });
page.getByText(/welcome/i);
```

Matches by substring by default. Use `exact: true` for full match.

### Placeholder

```typescript
page.getByPlaceholder("Enter your email");
page.getByPlaceholder(/search/i);
```

### Alt Text

```typescript
page.getByAltText("Company logo");
page.getByAltText(/profile/i);
```

For `<img>` and `<area>` elements.

### Title Attribute

```typescript
page.getByTitle("Close dialog");
page.getByTitle(/tooltip/i);
```

### Test ID

```typescript
page.getByTestId("submit-button");
page.getByTestId("user-menu");
```

Uses `data-testid` by default. Configure attribute name:

```typescript
// playwright.config.ts
use: {
    testIdAttribute: "data-test-id";
}
```

## Chaining and Filtering

### Chain Locators

Narrow scope by chaining:

```typescript
// Find button within modal
page.locator(".modal").getByRole("button", { name: "Confirm" });

// Find link within navigation
page.getByRole("navigation").getByRole("link", { name: "Products" });

// Within a specific list item
page.getByRole("listitem").filter({ hasText: "Apple" }).getByRole("button");
```

### Filter by Text

```typescript
// Contains text
page.getByRole("listitem").filter({ hasText: "Product" });

// Does not contain text
page.getByRole("listitem").filter({ hasNotText: "Out of stock" });

// Regex match
page.getByRole("listitem").filter({ hasText: /\$\d+/ });
```

### Filter by Child Element

```typescript
// Has specific child
page.getByRole("listitem").filter({ has: page.getByRole("button") });

// Does not have child
page.getByRole("listitem").filter({ hasNot: page.locator(".sold-out") });
```

### Combine Locators

```typescript
// AND — Both conditions must match
page.getByRole("button").and(page.getByText("Submit"));

// OR — Either condition matches
page.getByRole("button", { name: "Save" }).or(
    page.getByRole("button", { name: "Submit" }),
);
```

## List Operations

### Count Elements

```typescript
await expect(page.getByRole("listitem")).toHaveCount(5);

const count = await page.getByRole("listitem").count();
```

### Access Specific Items

```typescript
page.getByRole("listitem").first();
page.getByRole("listitem").last();
page.getByRole("listitem").nth(2); // Zero-indexed
```

### Iterate All Items

```typescript
const items = await page.getByRole("listitem").all();
for (const item of items) {
    console.log(await item.textContent());
}
```

### Strictness

Locators enforce single-match by default. Multiple matches throw error:

```typescript
// Throws if multiple buttons match
await page.getByRole("button").click();

// Explicitly get first
await page.getByRole("button").first().click();

// Or filter to single match
await page.getByRole("button", { name: "Submit" }).click();
```

## Legacy Locators

Avoid these—they break with DOM changes.

### CSS Selectors

```typescript
page.locator("button.primary");
page.locator("#submit-btn");
page.locator('[data-state="active"]');
```

### XPath

```typescript
page.locator('xpath=//button[@type="submit"]');
page.locator('//div[contains(@class, "modal")]');
```

### CSS Pseudo-Classes

```typescript
page.locator("button:visible");
page.locator('button:has-text("Submit")');
page.locator("div:has(span.icon)");
page.locator(':is(button, a):has-text("Click")');
```

## Framework-Specific Locators

Only work with unminified builds.

### React

```typescript
page.locator("_react=BookItem");
page.locator('_react=BookItem[author="Jane"]');
page.locator("_react=BookItem[author=/jane/i]");
```

### Vue

```typescript
page.locator("_vue=book-item");
page.locator('_vue=book-item[author="Jane"]');
```

## Debugging Locators

### Playwright Inspector

```bash
npx playwright test --debug
```

Use "Pick Locator" to identify elements visually.

### VS Code Extension

Click "Pick locator" in the testing sidebar.

### Console Helpers

```javascript
// In browser DevTools with PWDEBUG=console
playwright.$("text=Submit"); // Query selector
playwright.inspect("[data-testid]"); // Highlight element
playwright.selector(element); // Generate selector
```

### Evaluate Locator

```typescript
const count = await page.getByRole("button").count();
console.log(`Found ${count} buttons`);

const text = await page.getByRole("heading").first().textContent();
console.log(`Heading: ${text}`);
```

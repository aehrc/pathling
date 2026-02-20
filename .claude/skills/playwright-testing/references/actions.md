# Actions Reference

## Contents

1. [Text Input](#text-input)
2. [Click Actions](#click-actions)
3. [Form Controls](#form-controls)
4. [Keyboard](#keyboard)
5. [Mouse](#mouse)
6. [File Upload](#file-upload)
7. [Drag and Drop](#drag-and-drop)
8. [Navigation](#navigation)
9. [Waiting](#waiting)
10. [Frames](#frames)

## Text Input

### fill()

Clear field and enter text:

```typescript
await page.getByLabel("Email").fill("user@example.com");
await page.getByRole("textbox").fill(""); // Clear field
```

### pressSequentially()

Type character by character (triggers key events):

```typescript
await page.getByLabel("Search").pressSequentially("query", { delay: 100 });
```

### clear()

Clear input field:

```typescript
await page.getByLabel("Name").clear();
```

## Click Actions

### Basic Click

```typescript
await page.getByRole("button", { name: "Submit" }).click();
```

### Double Click

```typescript
await page.getByText("Edit").dblclick();
```

### Right Click

```typescript
await page.getByText("Item").click({ button: "right" });
```

### Click with Modifiers

```typescript
await page.getByRole("link").click({ modifiers: ["Shift"] });
await page.getByRole("link").click({ modifiers: ["Control"] }); // Ctrl+Click
await page.getByRole("link").click({ modifiers: ["Meta"] }); // Cmd+Click (Mac)
await page.getByRole("link").click({ modifiers: ["Alt"] });
```

### Click at Position

```typescript
await page.getByRole("canvas").click({ position: { x: 100, y: 50 } });
```

### Force Click

Bypass actionability checks:

```typescript
await page.getByRole("button").click({ force: true });
```

### Click Count

```typescript
await page.getByText("Word").click({ clickCount: 3 }); // Triple-click to select
```

## Form Controls

### Checkbox

```typescript
await page.getByLabel("Subscribe").check();
await page.getByLabel("Subscribe").uncheck();
await page.getByLabel("Subscribe").setChecked(true);
await page.getByLabel("Subscribe").setChecked(false);
```

### Radio Button

```typescript
await page.getByLabel("Option A").check();
```

### Select Dropdown

```typescript
// By value
await page.getByLabel("Country").selectOption("au");

// By label text
await page.getByLabel("Country").selectOption({ label: "Australia" });

// Multiple selection
await page.getByLabel("Colors").selectOption(["red", "blue"]);
```

### Focus

```typescript
await page.getByLabel("Email").focus();
```

## Keyboard

### Press Key

```typescript
await page.keyboard.press("Enter");
await page.keyboard.press("Tab");
await page.keyboard.press("Escape");
await page.keyboard.press("ArrowDown");
await page.keyboard.press("Backspace");
```

### Key Combinations

```typescript
await page.keyboard.press("Control+a"); // Select all
await page.keyboard.press("Control+c"); // Copy
await page.keyboard.press("Control+v"); // Paste
await page.keyboard.press("Meta+s"); // Cmd+S (Mac)
await page.keyboard.press("Shift+Tab");
```

### Type Text

```typescript
await page.keyboard.type("Hello World");
await page.keyboard.type("slow typing", { delay: 100 });
```

### Key Down/Up

```typescript
await page.keyboard.down("Shift");
await page.keyboard.press("ArrowDown");
await page.keyboard.press("ArrowDown");
await page.keyboard.up("Shift");
```

### On Locator

```typescript
await page.getByRole("textbox").press("Enter");
await page.getByRole("textbox").press("Control+a");
```

## Mouse

### Move

```typescript
await page.mouse.move(100, 200);
```

### Click at Coordinates

```typescript
await page.mouse.click(100, 200);
await page.mouse.click(100, 200, { button: "right" });
await page.mouse.dblclick(100, 200);
```

### Button Press/Release

```typescript
await page.mouse.down();
await page.mouse.move(200, 300);
await page.mouse.up();
```

### Scroll

```typescript
await page.mouse.wheel(0, 500); // Scroll down
await page.mouse.wheel(0, -500); // Scroll up
```

### Hover

```typescript
await page.getByRole("menuitem").hover();
await page.getByText("Tooltip trigger").hover();
```

## File Upload

### Single File

```typescript
await page.getByLabel("Upload").setInputFiles("path/to/file.pdf");
```

### Multiple Files

```typescript
await page.getByLabel("Upload").setInputFiles(["file1.pdf", "file2.pdf"]);
```

### Directory

```typescript
await page.getByLabel("Upload").setInputFiles("path/to/directory");
```

### Buffer (In-Memory)

```typescript
await page.getByLabel("Upload").setInputFiles({
    name: "test.txt",
    mimeType: "text/plain",
    buffer: Buffer.from("file content"),
});
```

### Clear Selection

```typescript
await page.getByLabel("Upload").setInputFiles([]);
```

### File Chooser Event

```typescript
const fileChooserPromise = page.waitForEvent("filechooser");
await page.getByRole("button", { name: "Upload" }).click();
const fileChooser = await fileChooserPromise;
await fileChooser.setFiles("path/to/file.pdf");
```

## Drag and Drop

### Simple Drag

```typescript
await page.getByText("Drag me").dragTo(page.getByText("Drop here"));
```

### Manual Drag

```typescript
await page.locator("#source").hover();
await page.mouse.down();
await page.locator("#target").hover();
await page.mouse.up();
```

### With Coordinates

```typescript
const source = page.locator("#source");
const target = page.locator("#target");

const sourceBox = await source.boundingBox();
const targetBox = await target.boundingBox();

await page.mouse.move(
    sourceBox.x + sourceBox.width / 2,
    sourceBox.y + sourceBox.height / 2,
);
await page.mouse.down();
await page.mouse.move(
    targetBox.x + targetBox.width / 2,
    targetBox.y + targetBox.height / 2,
);
await page.mouse.up();
```

## Navigation

### Go To URL

```typescript
await page.goto("https://example.com");
await page.goto("/relative/path"); // Uses baseURL
await page.goto("https://example.com", { waitUntil: "networkidle" });
```

Wait options:

- `'load'` — Wait for load event (default)
- `'domcontentloaded'` — Wait for DOMContentLoaded
- `'networkidle'` — Wait until no network requests for 500ms
- `'commit'` — Wait for response received

### Reload

```typescript
await page.reload();
await page.reload({ waitUntil: "networkidle" });
```

### History Navigation

```typescript
await page.goBack();
await page.goForward();
```

### Get Current URL/Title

```typescript
const url = page.url();
const title = await page.title();
```

## Waiting

### Wait for URL

```typescript
await page.waitForURL("**/dashboard");
await page.waitForURL(/\/dashboard$/);
```

### Wait for Load State

```typescript
await page.waitForLoadState("load");
await page.waitForLoadState("domcontentloaded");
await page.waitForLoadState("networkidle");
```

### Wait for Locator State

```typescript
await page.getByRole("dialog").waitFor();
await page.getByRole("dialog").waitFor({ state: "visible" });
await page.getByRole("dialog").waitFor({ state: "hidden" });
await page.getByRole("dialog").waitFor({ state: "attached" });
await page.getByRole("dialog").waitFor({ state: "detached" });
```

### Wait for Response

```typescript
const responsePromise = page.waitForResponse("**/api/data");
await page.getByRole("button", { name: "Load" }).click();
const response = await responsePromise;
```

### Wait for Request

```typescript
const requestPromise = page.waitForRequest("**/api/submit");
await page.getByRole("button", { name: "Submit" }).click();
const request = await requestPromise;
```

### Wait for Function

```typescript
await page.waitForFunction(() => window.dataLoaded === true);
await page.waitForFunction(([a, b]) => a + b === 10, [3, 7]);
```

### Wait for Timeout

Use sparingly—prefer explicit waits:

```typescript
await page.waitForTimeout(1000); // 1 second
```

## Frames

### Access Frame

```typescript
// By name or URL
const frame = page.frame({ name: "myframe" });
const frame = page.frame({ url: /example\.com/ });

// By frame locator
const frameLocator = page.frameLocator('iframe[name="editor"]');
await frameLocator.getByRole("button").click();
```

### Frame Locator

```typescript
const frame = page.frameLocator("#iframe");
await frame.getByLabel("Email").fill("user@example.com");
await frame.getByRole("button", { name: "Submit" }).click();
```

### Nested Frames

```typescript
const outerFrame = page.frameLocator("#outer");
const innerFrame = outerFrame.frameLocator("#inner");
await innerFrame.getByRole("button").click();
```

## Auto-Waiting

All actions auto-wait for:

1. **Visibility** — Element is visible
2. **Stability** — Element position is stable (not animating)
3. **Enabled** — Element is not disabled
4. **Receivable** — Element can receive events (not obscured)

Override with `force: true` when needed:

```typescript
await locator.click({ force: true });
await locator.fill("text", { force: true });
```

## Actionability Timeout

Default action timeout: 30 seconds. Configure globally:

```typescript
// playwright.config.ts
use: {
  actionTimeout: 10000,  // 10 seconds
}
```

Or per-action:

```typescript
await locator.click({ timeout: 5000 });
```

# Radix Primitives Patterns

## Available Primitives

| Component      | Package                         | Purpose                         |
| -------------- | ------------------------------- | ------------------------------- |
| Accordion      | @radix-ui/react-accordion       | Collapsible content sections    |
| AlertDialog    | @radix-ui/react-alert-dialog    | Modal requiring acknowledgement |
| AspectRatio    | @radix-ui/react-aspect-ratio    | Maintain aspect ratio           |
| Avatar         | @radix-ui/react-avatar          | User avatar with fallback       |
| Checkbox       | @radix-ui/react-checkbox        | Checkbox input                  |
| Collapsible    | @radix-ui/react-collapsible     | Show/hide content               |
| ContextMenu    | @radix-ui/react-context-menu    | Right-click menu                |
| Dialog         | @radix-ui/react-dialog          | Modal dialog                    |
| DropdownMenu   | @radix-ui/react-dropdown-menu   | Dropdown menu                   |
| HoverCard      | @radix-ui/react-hover-card      | Hover-triggered card            |
| Label          | @radix-ui/react-label           | Accessible label                |
| Menubar        | @radix-ui/react-menubar         | Horizontal menu bar             |
| NavigationMenu | @radix-ui/react-navigation-menu | Site navigation                 |
| Popover        | @radix-ui/react-popover         | Floating content                |
| Progress       | @radix-ui/react-progress        | Progress indicator              |
| RadioGroup     | @radix-ui/react-radio-group     | Radio button group              |
| ScrollArea     | @radix-ui/react-scroll-area     | Custom scrollbar                |
| Select         | @radix-ui/react-select          | Select dropdown                 |
| Separator      | @radix-ui/react-separator       | Visual divider                  |
| Slider         | @radix-ui/react-slider          | Range slider                    |
| Switch         | @radix-ui/react-switch          | Toggle switch                   |
| Tabs           | @radix-ui/react-tabs            | Tabbed content                  |
| Toast          | @radix-ui/react-toast           | Toast notifications             |
| Toggle         | @radix-ui/react-toggle          | Toggle button                   |
| ToggleGroup    | @radix-ui/react-toggle-group    | Toggle button group             |
| Toolbar        | @radix-ui/react-toolbar         | Toolbar with items              |
| Tooltip        | @radix-ui/react-tooltip         | Hover tooltip                   |

## Common Patterns

### Dialog with Form

```tsx
import * as Dialog from "@radix-ui/react-dialog";
import { useState } from "react";

function EditDialog() {
    const [open, setOpen] = useState(false);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        await saveData();
        setOpen(false);
    };

    return (
        <Dialog.Root open={open} onOpenChange={setOpen}>
            <Dialog.Trigger asChild>
                <button>Edit</button>
            </Dialog.Trigger>
            <Dialog.Portal>
                <Dialog.Overlay className="dialog-overlay" />
                <Dialog.Content className="dialog-content">
                    <Dialog.Title>Edit Item</Dialog.Title>
                    <form onSubmit={handleSubmit}>
                        <input name="title" />
                        <div className="button-group">
                            <Dialog.Close asChild>
                                <button type="button">Cancel</button>
                            </Dialog.Close>
                            <button type="submit">Save</button>
                        </div>
                    </form>
                </Dialog.Content>
            </Dialog.Portal>
        </Dialog.Root>
    );
}
```

### Dropdown Menu with Icons

```tsx
import * as DropdownMenu from "@radix-ui/react-dropdown-menu";
import { CheckIcon, ChevronRightIcon } from "@radix-ui/react-icons";

function Menu() {
    const [bookmarked, setBookmarked] = useState(false);

    return (
        <DropdownMenu.Root>
            <DropdownMenu.Trigger asChild>
                <button>Menu</button>
            </DropdownMenu.Trigger>
            <DropdownMenu.Portal>
                <DropdownMenu.Content
                    className="dropdown-content"
                    sideOffset={5}
                >
                    <DropdownMenu.Item className="dropdown-item">
                        New Tab
                    </DropdownMenu.Item>

                    <DropdownMenu.CheckboxItem
                        className="dropdown-item"
                        checked={bookmarked}
                        onCheckedChange={setBookmarked}
                    >
                        <DropdownMenu.ItemIndicator className="item-indicator">
                            <CheckIcon />
                        </DropdownMenu.ItemIndicator>
                        Bookmarked
                    </DropdownMenu.CheckboxItem>

                    <DropdownMenu.Sub>
                        <DropdownMenu.SubTrigger className="dropdown-item">
                            More
                            <ChevronRightIcon />
                        </DropdownMenu.SubTrigger>
                        <DropdownMenu.Portal>
                            <DropdownMenu.SubContent className="dropdown-content">
                                <DropdownMenu.Item className="dropdown-item">
                                    Save Page As...
                                </DropdownMenu.Item>
                            </DropdownMenu.SubContent>
                        </DropdownMenu.Portal>
                    </DropdownMenu.Sub>

                    <DropdownMenu.Separator className="separator" />

                    <DropdownMenu.Item className="dropdown-item destructive">
                        Delete
                    </DropdownMenu.Item>

                    <DropdownMenu.Arrow className="dropdown-arrow" />
                </DropdownMenu.Content>
            </DropdownMenu.Portal>
        </DropdownMenu.Root>
    );
}
```

### Accordion

```tsx
import * as Accordion from "@radix-ui/react-accordion";
import { ChevronDownIcon } from "@radix-ui/react-icons";

function FAQ() {
    return (
        <Accordion.Root type="single" collapsible>
            <Accordion.Item value="item-1">
                <Accordion.Header>
                    <Accordion.Trigger className="accordion-trigger">
                        What is Radix?
                        <ChevronDownIcon
                            className="accordion-chevron"
                            aria-hidden
                        />
                    </Accordion.Trigger>
                </Accordion.Header>
                <Accordion.Content className="accordion-content">
                    Radix is a component library for building accessible UIs.
                </Accordion.Content>
            </Accordion.Item>
            <Accordion.Item value="item-2">
                <Accordion.Header>
                    <Accordion.Trigger className="accordion-trigger">
                        Is it accessible?
                        <ChevronDownIcon
                            className="accordion-chevron"
                            aria-hidden
                        />
                    </Accordion.Trigger>
                </Accordion.Header>
                <Accordion.Content className="accordion-content">
                    Yes, it follows WAI-ARIA patterns.
                </Accordion.Content>
            </Accordion.Item>
        </Accordion.Root>
    );
}
```

CSS for accordion animation:

```css
.accordion-content {
    overflow: hidden;
}

.accordion-content[data-state="open"] {
    animation: slideDown 300ms ease-out;
}

.accordion-content[data-state="closed"] {
    animation: slideUp 300ms ease-out;
}

@keyframes slideDown {
    from {
        height: 0;
    }
    to {
        height: var(--radix-accordion-content-height);
    }
}

@keyframes slideUp {
    from {
        height: var(--radix-accordion-content-height);
    }
    to {
        height: 0;
    }
}

.accordion-chevron {
    transition: transform 300ms;
}

.accordion-trigger[data-state="open"] .accordion-chevron {
    transform: rotate(180deg);
}
```

### Tabs with URL Sync

```tsx
import * as Tabs from "@radix-ui/react-tabs";
import { useSearchParams } from "react-router-dom";

function SettingsTabs() {
    const [searchParams, setSearchParams] = useSearchParams();
    const tab = searchParams.get("tab") ?? "general";

    return (
        <Tabs.Root
            value={tab}
            onValueChange={(value) => setSearchParams({ tab: value })}
        >
            <Tabs.List className="tabs-list">
                <Tabs.Trigger value="general" className="tab-trigger">
                    General
                </Tabs.Trigger>
                <Tabs.Trigger value="security" className="tab-trigger">
                    Security
                </Tabs.Trigger>
                <Tabs.Trigger value="billing" className="tab-trigger">
                    Billing
                </Tabs.Trigger>
            </Tabs.List>
            <Tabs.Content value="general" className="tab-content">
                General settings...
            </Tabs.Content>
            <Tabs.Content value="security" className="tab-content">
                Security settings...
            </Tabs.Content>
            <Tabs.Content value="billing" className="tab-content">
                Billing settings...
            </Tabs.Content>
        </Tabs.Root>
    );
}
```

### Toast Notifications

```tsx
import * as Toast from "@radix-ui/react-toast";
import { useState, useCallback } from "react";

function ToastDemo() {
    const [open, setOpen] = useState(false);

    const showToast = useCallback(() => {
        setOpen(true);
    }, []);

    return (
        <Toast.Provider swipeDirection="right">
            <button onClick={showToast}>Save</button>

            <Toast.Root
                className="toast-root"
                open={open}
                onOpenChange={setOpen}
                duration={5000}
            >
                <Toast.Title className="toast-title">Saved!</Toast.Title>
                <Toast.Description className="toast-description">
                    Your changes have been saved.
                </Toast.Description>
                <Toast.Action asChild altText="Undo">
                    <button className="toast-action">Undo</button>
                </Toast.Action>
                <Toast.Close className="toast-close">×</Toast.Close>
            </Toast.Root>

            <Toast.Viewport className="toast-viewport" />
        </Toast.Provider>
    );
}
```

### Select with Groups

```tsx
import * as Select from "@radix-ui/react-select";
import {
    CheckIcon,
    ChevronDownIcon,
    ChevronUpIcon,
} from "@radix-ui/react-icons";

function CountrySelect() {
    return (
        <Select.Root>
            <Select.Trigger className="select-trigger">
                <Select.Value placeholder="Select country" />
                <Select.Icon>
                    <ChevronDownIcon />
                </Select.Icon>
            </Select.Trigger>
            <Select.Portal>
                <Select.Content className="select-content">
                    <Select.ScrollUpButton className="select-scroll-button">
                        <ChevronUpIcon />
                    </Select.ScrollUpButton>
                    <Select.Viewport className="select-viewport">
                        <Select.Group>
                            <Select.Label className="select-label">
                                Oceania
                            </Select.Label>
                            <SelectItem value="au">Australia</SelectItem>
                            <SelectItem value="nz">New Zealand</SelectItem>
                        </Select.Group>
                        <Select.Separator className="select-separator" />
                        <Select.Group>
                            <Select.Label className="select-label">
                                Europe
                            </Select.Label>
                            <SelectItem value="uk">United Kingdom</SelectItem>
                            <SelectItem value="de">Germany</SelectItem>
                        </Select.Group>
                    </Select.Viewport>
                    <Select.ScrollDownButton className="select-scroll-button">
                        <ChevronDownIcon />
                    </Select.ScrollDownButton>
                </Select.Content>
            </Select.Portal>
        </Select.Root>
    );
}

function SelectItem({
    children,
    value,
}: {
    children: React.ReactNode;
    value: string;
}) {
    return (
        <Select.Item value={value} className="select-item">
            <Select.ItemText>{children}</Select.ItemText>
            <Select.ItemIndicator className="select-indicator">
                <CheckIcon />
            </Select.ItemIndicator>
        </Select.Item>
    );
}
```

## Styling with Tailwind CSS

### Dialog Example

```tsx
<Dialog.Portal>
    <Dialog.Overlay className="fixed inset-0 bg-black/50 data-[state=open]:animate-fade-in" />
    <Dialog.Content className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 bg-white rounded-lg p-6 shadow-xl w-full max-w-md data-[state=open]:animate-scale-in">
        <Dialog.Title className="text-lg font-semibold">Title</Dialog.Title>
        <Dialog.Description className="text-gray-600 mt-2">
            Description text here.
        </Dialog.Description>
        <Dialog.Close className="absolute top-4 right-4 text-gray-400 hover:text-gray-600">
            ×
        </Dialog.Close>
    </Dialog.Content>
</Dialog.Portal>
```

Tailwind config for animations:

```js
// tailwind.config.js
module.exports = {
    theme: {
        extend: {
            keyframes: {
                "fade-in": {
                    from: { opacity: "0" },
                    to: { opacity: "1" },
                },
                "scale-in": {
                    from: {
                        opacity: "0",
                        transform: "translate(-50%, -50%) scale(0.96)",
                    },
                    to: {
                        opacity: "1",
                        transform: "translate(-50%, -50%) scale(1)",
                    },
                },
            },
            animation: {
                "fade-in": "fade-in 150ms ease-out",
                "scale-in": "scale-in 150ms ease-out",
            },
        },
    },
};
```

## Keyboard Interactions Reference

| Component    | Keys                                                          |
| ------------ | ------------------------------------------------------------- |
| Dialog       | Esc (close), Tab (focus trap)                                 |
| DropdownMenu | Enter/Space (open/select), Arrow keys (navigate), Esc (close) |
| Tabs         | Arrow keys (switch tabs), Home/End (first/last)               |
| Accordion    | Enter/Space (toggle), Arrow keys (navigate)                   |
| Select       | Enter/Space (open/select), Arrow keys (navigate), Esc (close) |
| Slider       | Arrow keys (adjust), Home/End (min/max)                       |

## Data Attributes Reference

Most primitives expose these data attributes for styling:

| Attribute          | Values                                             | Purpose              |
| ------------------ | -------------------------------------------------- | -------------------- |
| `data-state`       | open, closed, checked, unchecked, active, inactive | Current state        |
| `data-disabled`    | (present when disabled)                            | Disabled state       |
| `data-highlighted` | (present when highlighted)                         | Keyboard/hover focus |
| `data-orientation` | horizontal, vertical                               | Layout direction     |
| `data-side`        | top, right, bottom, left                           | Positioning side     |
| `data-align`       | start, center, end                                 | Alignment            |

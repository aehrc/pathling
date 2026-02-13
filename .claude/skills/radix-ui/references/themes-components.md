# Radix Themes Component Reference

## Layout

### Box

Base layout element with margin/padding props.

```tsx
<Box p="4" m="2" style={{ background: "var(--gray-3)" }}>
    Content
</Box>
```

### Flex

Flexbox container.

| Prop        | Values                                   |
| ----------- | ---------------------------------------- |
| `direction` | row, column, row-reverse, column-reverse |
| `align`     | start, center, end, baseline, stretch    |
| `justify`   | start, center, end, between              |
| `gap`       | "1" - "9"                                |
| `wrap`      | wrap, nowrap, wrap-reverse               |

```tsx
<Flex direction="column" gap="3" align="center">
    <Box>Item 1</Box>
    <Box>Item 2</Box>
</Flex>
```

### Grid

CSS Grid container.

```tsx
<Grid columns="3" gap="4" rows="repeat(2, 64px)">
    <Box>1</Box>
    <Box>2</Box>
    <Box>3</Box>
</Grid>
```

### Container

Constrains content width with auto margins.

```tsx
<Container size="2">
    <Text>Centered content</Text>
</Container>
```

### Section

Vertical spacing section.

```tsx
<Section size="2">
    <Heading>Section Title</Heading>
</Section>
```

## Typography

### Heading

```tsx
<Heading size="6" weight="bold" align="center">
    Page Title
</Heading>
```

Sizes: "1" - "9"

### Text

```tsx
<Text size="2" weight="medium" color="gray">
    Body text
</Text>
```

### Code

```tsx
<Code variant="soft" size="2">
    npm install
</Code>
```

### Blockquote, Em, Strong, Quote

Standard text formatting components.

## Form Components

### Button

| Prop           | Values                                        | Default |
| -------------- | --------------------------------------------- | ------- |
| `variant`      | classic, solid, soft, surface, outline, ghost | solid   |
| `size`         | "1", "2", "3", "4"                            | "2"     |
| `color`        | theme colors                                  | accent  |
| `highContrast` | boolean                                       | false   |
| `loading`      | boolean                                       | false   |

```tsx
<Button variant="soft" size="3" loading={isLoading}>
    <PlusIcon /> Add Item
</Button>
```

### TextField

```tsx
<TextField.Root placeholder="Search..." size="2">
    <TextField.Slot>
        <MagnifyingGlassIcon />
    </TextField.Slot>
</TextField.Root>
```

### TextArea

```tsx
<TextArea placeholder="Enter description..." size="2" />
```

### Select

```tsx
<Select.Root defaultValue="apple">
    <Select.Trigger />
    <Select.Content>
        <Select.Item value="apple">Apple</Select.Item>
        <Select.Item value="orange">Orange</Select.Item>
    </Select.Content>
</Select.Root>
```

### Checkbox

```tsx
<Text as="label" size="2">
    <Flex gap="2">
        <Checkbox defaultChecked />
        Accept terms
    </Flex>
</Text>
```

### RadioGroup

```tsx
<RadioGroup.Root defaultValue="1">
    <Flex gap="2" direction="column">
        <Text as="label" size="2">
            <Flex gap="2">
                <RadioGroup.Item value="1" /> Option 1
            </Flex>
        </Text>
        <Text as="label" size="2">
            <Flex gap="2">
                <RadioGroup.Item value="2" /> Option 2
            </Flex>
        </Text>
    </Flex>
</RadioGroup.Root>
```

### Switch

```tsx
<Text as="label" size="2">
    <Flex gap="2">
        <Switch defaultChecked /> Dark mode
    </Flex>
</Text>
```

### Slider

```tsx
<Slider defaultValue={[50]} />
```

## Data Display

### Table

```tsx
<Table.Root>
    <Table.Header>
        <Table.Row>
            <Table.ColumnHeaderCell>Name</Table.ColumnHeaderCell>
            <Table.ColumnHeaderCell>Email</Table.ColumnHeaderCell>
        </Table.Row>
    </Table.Header>
    <Table.Body>
        <Table.Row>
            <Table.Cell>John</Table.Cell>
            <Table.Cell>john@example.com</Table.Cell>
        </Table.Row>
    </Table.Body>
</Table.Root>
```

### Badge

```tsx
<Badge color="green" variant="soft">
    Active
</Badge>
```

### Avatar

```tsx
<Avatar src="/avatar.png" fallback="JD" size="3" />
```

### Card

```tsx
<Card size="2">
    <Flex gap="3" align="center">
        <Avatar fallback="T" />
        <Box>
            <Text weight="bold">Card Title</Text>
            <Text color="gray">Description</Text>
        </Box>
    </Flex>
</Card>
```

### Callout

```tsx
<Callout.Root color="blue">
    <Callout.Icon>
        <InfoCircledIcon />
    </Callout.Icon>
    <Callout.Text>Important information here.</Callout.Text>
</Callout.Root>
```

## Overlays

### Dialog

```tsx
<Dialog.Root>
    <Dialog.Trigger>
        <Button>Open Dialog</Button>
    </Dialog.Trigger>
    <Dialog.Content maxWidth="450px">
        <Dialog.Title>Edit Profile</Dialog.Title>
        <Dialog.Description size="2" mb="4">
            Make changes to your profile.
        </Dialog.Description>
        <Flex gap="3" mt="4" justify="end">
            <Dialog.Close>
                <Button variant="soft" color="gray">
                    Cancel
                </Button>
            </Dialog.Close>
            <Dialog.Close>
                <Button>Save</Button>
            </Dialog.Close>
        </Flex>
    </Dialog.Content>
</Dialog.Root>
```

### AlertDialog

Similar to Dialog but for confirmations with required action.

### DropdownMenu

```tsx
<DropdownMenu.Root>
    <DropdownMenu.Trigger>
        <Button variant="soft">Options</Button>
    </DropdownMenu.Trigger>
    <DropdownMenu.Content>
        <DropdownMenu.Item>Edit</DropdownMenu.Item>
        <DropdownMenu.Item>Duplicate</DropdownMenu.Item>
        <DropdownMenu.Separator />
        <DropdownMenu.Item color="red">Delete</DropdownMenu.Item>
    </DropdownMenu.Content>
</DropdownMenu.Root>
```

### ContextMenu

Right-click menu with same API as DropdownMenu.

### Popover

```tsx
<Popover.Root>
    <Popover.Trigger>
        <Button variant="soft">Info</Button>
    </Popover.Trigger>
    <Popover.Content>
        <Text>Popover content here.</Text>
    </Popover.Content>
</Popover.Root>
```

### HoverCard

```tsx
<HoverCard.Root>
    <HoverCard.Trigger>
        <Link>@username</Link>
    </HoverCard.Trigger>
    <HoverCard.Content>
        <Flex gap="4">
            <Avatar fallback="U" />
            <Box>
                <Heading size="3">User Name</Heading>
                <Text color="gray">Bio information</Text>
            </Box>
        </Flex>
    </HoverCard.Content>
</HoverCard.Root>
```

### Tooltip

```tsx
<Tooltip content="Add to library">
    <IconButton>
        <PlusIcon />
    </IconButton>
</Tooltip>
```

## Navigation

### Tabs

```tsx
<Tabs.Root defaultValue="account">
    <Tabs.List>
        <Tabs.Trigger value="account">Account</Tabs.Trigger>
        <Tabs.Trigger value="settings">Settings</Tabs.Trigger>
    </Tabs.List>
    <Box pt="3">
        <Tabs.Content value="account">
            <Text>Account settings</Text>
        </Tabs.Content>
        <Tabs.Content value="settings">
            <Text>App settings</Text>
        </Tabs.Content>
    </Box>
</Tabs.Root>
```

### Link

```tsx
<Link href="/about" size="2" weight="medium">
    About Us
</Link>
```

## Utility

### Separator

```tsx
<Separator size="4" />
```

### ScrollArea

```tsx
<ScrollArea type="always" scrollbars="vertical" style={{ height: 200 }}>
    <Box p="2">Long content...</Box>
</ScrollArea>
```

### Skeleton

```tsx
<Skeleton loading={isLoading}>
    <Text>Content appears when loaded</Text>
</Skeleton>
```

### Spinner

```tsx
<Spinner size="3" />
```

### Progress

```tsx
<Progress value={75} />
```

## Dark Mode

Use the `appearance` prop on Theme:

```tsx
<Theme appearance="dark">
    <MyApp />
</Theme>
```

Or use system preference:

```tsx
<Theme appearance="inherit">
    <MyApp />
</Theme>
```

## CSS Variables

Access theme tokens via CSS variables:

```css
.custom {
    color: var(--accent-9);
    background: var(--gray-3);
    border-radius: var(--radius-2);
    padding: var(--space-3);
}
```

Color scales: `--gray-1` through `--gray-12`, `--accent-1` through `--accent-12`

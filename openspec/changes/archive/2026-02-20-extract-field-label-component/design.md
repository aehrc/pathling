## Context

Form fields across the UI use an inline `<Text as="label" size="2"
weight="medium" mb="1">` pattern for labels. The `FieldGuidance` component was
recently extracted to centralise helper text rendering. The label pattern is the
remaining piece of per-field boilerplate that can be consolidated.

## Goals / Non-goals

**Goals:**

- Single component for all form field labels.
- Support an "optional" marker suffix (e.g. "Elements (optional)").
- Zero visual change after substitution.

**Non-goals:**

- Wrapping label + control + guidance into a single compound "form field"
  component. That is a larger refactor for another time.
- Adding `htmlFor` / accessibility wiring. The existing codebase does not use it
  consistently and that is a separate concern.

## Decisions

**Component API mirrors FieldGuidance.** A simple props interface
(`children`, `optional`, `mb`) keeps it consistent with the existing
`FieldGuidance` pattern. The `optional` boolean appends " (optional)" to the
label text, eliminating the need for callers to encode that string themselves.

**Bottom margin defaults to `"1"`.** This matches the most common usage. The few
places that use `mb="2"` or wrap in a `Flex` with custom spacing can override
via the `mb` prop.

**No `htmlFor` prop.** Adding accessibility attributes is desirable but out of
scope for this extraction, which is a pure refactor with no behavioural change.

## Risks / Trade-offs

- **Low risk:** This is a mechanical extraction with no logic changes. Each
  substitution can be verified visually.

## Context

The `ResourceSearchForm` component maintains local state for the selected
resource type, FHIRPath filter rows, and search parameter rows. When the user
changes the resource type, a `useEffect` hook detects the change and removes any
parameter selections that are no longer valid for the new type. However, it
leaves parameter values and FHIRPath filter expressions intact, which creates a
confusing experience.

## Goals / Non-Goals

**Goals:**

- Reset all filter rows and parameter rows to their initial empty state when the
  resource type changes.

**Non-Goals:**

- Persisting search state across resource type changes (e.g. remembering filters
  per type).
- Changing the initial default state of the form.

## Decisions

Replace the existing `useEffect` that selectively clears invalid parameters with
a simpler reset that returns filters and parameter rows to their initial state
(one empty row each). This is simpler than the current logic and matches user
expectations — switching resource type is a fresh search context.

The `idCounter` ref should also be reset so that IDs restart cleanly.

## Risks / Trade-offs

- Users who intentionally share filter expressions across resource types will
  need to re-enter them. This is an acceptable trade-off given the confusion
  caused by stale values.

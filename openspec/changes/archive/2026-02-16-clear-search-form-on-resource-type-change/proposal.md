## Why

When the user changes the resource type on the resources page, the search
parameter rows and FHIRPath filter expressions remain populated with values from
the previous resource type. This is confusing because those values are likely
irrelevant to the new resource type, and the user must manually clear them
before entering new search criteria.

## What Changes

- Reset all search parameter rows (name and value) to a single empty row when
  the resource type changes.
- Reset all FHIRPath filter rows to a single empty row when the resource type
  changes.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `search-parameter-form`: The form currently only clears invalid parameter
  names on resource type change. The new behaviour fully resets all form fields
  (parameter rows and filter rows) to their initial empty state.

## Impact

- `ui/src/components/resources/ResourceSearchForm.tsx` - The resource type
  change handler needs to reset filters and parameter rows instead of selectively
  clearing invalid parameters.

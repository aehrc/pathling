## Why

The top line of each search result card in the admin UI has a hard-coded
`maxWidth: "400px"` on the resource ID element. This prevents the row of items
(badge, ID, action icons) from filling the full horizontal width of the card,
leaving wasted space on wider viewports.

See: https://github.com/aehrc/pathling/issues/2559

## What changes

- Remove the `maxWidth` constraint from the resource ID text element in
  `ResourceCard`, allowing the flex layout to naturally distribute space across
  the full card width.

## Capabilities

### New capabilities

_None._

### Modified capabilities

_None — this is a cosmetic fix to inline styles, not a change to any
spec-level behaviour._

## Impact

- `ui/src/components/resources/ResourceCard.tsx` — inline style change only.
- No API, dependency, or behavioural changes.

## Context

The `ResourceCard` component renders each search result in the admin UI. The top
row uses a flex layout: a resource type badge, the resource ID (with
`flexGrow: 1`), and action icon buttons. A hard-coded `maxWidth: "400px"` on the
ID text element prevents flex from distributing the remaining space, leaving a
visible gap on wider viewports.

## Goals / Non-goals

**Goals:**

- Allow the top row of each search result card to fill its full available width.
- Truncate long resource IDs with an ellipsis so they don't break the layout.

**Non-goals:**

- Redesigning the card layout or changing any other visual aspect.

## Decisions

Remove `maxWidth: "400px"` from the resource ID `Text` element in
`ResourceCard.tsx`. The existing `overflow: "hidden"`, `textWrap: "nowrap"`, and
`textOverflow: "ellipsis"` styles already handle truncation correctly. Since
`overflow: "hidden"` is set, the CSS spec clamps the flex item's automatic
minimum size to zero, so the element will shrink within the flex container and
the ellipsis triggers naturally from the available space.

## Risks / Trade-offs

- Minimal risk. The ellipsis behaviour is preserved by the existing overflow
  styles. The only change is that the truncation point is now determined by the
  flex layout rather than a fixed 400px cap.

## Context

The Pathling UI uses a repeated pattern of `<Text size="1" color="gray" mt="1">` for field guidance text across ~10 components. There is no shared abstraction; each site duplicates the same Radix UI props.

## Goals / Non-goals

**Goals:**

- Eliminate duplication of field guidance text styling across all form components.
- Provide a single component that encodes the semantic intent of "field guidance".

**Non-goals:**

- Changing the visual appearance of guidance text.
- Handling validation messages or error states.
- Covering non-guidance uses of `<Text size="1" color="gray">` (e.g. column sub-labels in `ImportForm`, metadata labels in cards).

## Decisions

### Component API

The component accepts `children` (the guidance text) and an optional `mt` prop (defaulting to `"1"`) to handle the few cases where spacing varies (e.g. `SqlOnFhirForm` uses `mt="2"`, empty-state text in `ExportOptions` uses no margin).

**Rationale**: A minimal API avoids over-abstraction. The `mt` prop covers the only meaningful variation across use sites.

### File location

Place the component at `ui/src/components/FieldGuidance.tsx`, alongside other shared form-related components.

**Rationale**: This is a general-purpose form component, not specific to any feature. The flat `components/` directory matches existing project structure.

## Risks / Trade-offs

- **Risk**: Some `<Text size="1" color="gray">` instances are not field guidance (card metadata, column labels). These must not be converted.
  **Mitigation**: Only convert instances that semantically represent help text below form fields.

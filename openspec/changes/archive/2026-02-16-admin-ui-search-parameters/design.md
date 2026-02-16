## Context

The Pathling admin UI provides a resource search page that currently only
supports FHIRPath filter expressions. The server already fully implements
standard FHIR search parameters (token, string, date, number, quantity,
reference, URI), declares them in the CapabilityStatement, and supports
combining them with FHIRPath filters via AND logic.

The existing UI architecture uses:

- **Radix UI Themes** for all components (Select, TextField, Button, Card,
  etc.)
- **TanStack Query** for server state management
- **React Router** for navigation
- A `useServerCapabilities` hook that fetches and parses the
  CapabilityStatement, but currently discards search parameter information

## Goals / Non-goals

**Goals:**

- Allow users to search using standard FHIR search parameters through the UI.
- Dynamically populate available search parameters from the server's
  CapabilityStatement for the selected resource type.
- Allow combining standard search parameters with FHIRPath filters.
- Display the search parameter type alongside each parameter name for user
  guidance.

**Non-goals:**

- Type-specific input widgets (e.g., date pickers for date parameters, coding
  system/code split inputs for token parameters). All values will be entered as
  free text matching FHIR search value syntax.
- Search parameter modifier support (`:exact`, `:not`, etc.) in the UI.
- OR logic within a single parameter (comma-separated values) — users can type
  commas manually but there is no dedicated UI for it.
- Pagination controls — the existing `_count=10` default remains.

## Decisions

### Extract search parameters from CapabilityStatement

The `parseCapabilities` function in `useServerCapabilities.ts` already iterates
over `rest[].resource[]` but currently skips the `searchParam` array. We will
extend `ResourceCapability` to include search parameters and extract them during
parsing.

**Alternative considered**: Fetching search parameters via a separate API call
(e.g., `SearchParameter` resources). Rejected because the CapabilityStatement
already contains everything we need and is already fetched on app load.

### Search parameter form as a section within the existing form

The search parameter inputs will be added as a new section within
`ResourceSearchForm`, positioned between the resource type selector and the
FHIRPath filters section. This keeps both search mechanisms visible and
accessible.

**Alternative considered**: A tabbed interface switching between "Standard
search" and "FHIRPath search" modes. Rejected because the server supports using
both simultaneously, and hiding one behind a tab would obscure this capability.

### Free text input for all parameter types

All search parameter values will use a single `TextField` input regardless of
parameter type (token, date, string, etc.). The parameter type will be displayed
as a badge next to the parameter name for guidance.

**Alternative considered**: Type-specific input widgets (date picker, system/code
split fields, numeric inputs). Rejected as over-engineering for the initial
implementation — users familiar with FHIR search syntax can type the correct
value format directly.

### Parameters sent as standard query parameters

When standard search parameters are provided, they are sent as regular URL query
parameters on the resource type endpoint (e.g.,
`GET /Patient?gender=male&birthdate=gt2000`). If FHIRPath filters are also
provided, the request uses the `_query=fhirPath` named query with both filter
parameters and standard parameters.

The existing `search()` function in `rest.ts` already accepts a generic `params`
record. No changes to the REST client are needed.

### Dynamic parameter list filtered by resource type

When the user changes the selected resource type, the available search
parameters update automatically from the CapabilityStatement data. The parameter
select dropdown only shows parameters for the currently selected resource type.

## Risks / Trade-offs

- **User confusion between search modes**: Users might not understand when to
  use standard parameters versus FHIRPath filters. → Mitigation: Clear section
  headings and help text explaining the difference.
- **Large parameter lists**: Some resource types have many search parameters
  (e.g., Patient has ~30). → Mitigation: Use a Select dropdown for parameter
  selection rather than displaying all at once.
- **Value format errors**: Users may enter values in incorrect FHIR search
  format. → Mitigation: The server returns descriptive 400 errors which are
  already displayed in the UI.

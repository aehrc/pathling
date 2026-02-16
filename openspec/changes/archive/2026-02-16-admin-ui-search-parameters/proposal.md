## Why

The Pathling server already supports standard FHIR search parameters (token,
string, date, number, quantity, reference, URI) alongside FHIRPath filters, but
the admin UI only exposes FHIRPath filter inputs. Users who are familiar with
standard FHIR search syntax have no way to use it through the UI, forcing them
to learn FHIRPath or use external tools.

## What changes

- Add a search parameter input section to the resource search form, allowing
  users to select from the server's declared search parameters for the chosen
  resource type and provide values.
- Fetch available search parameters from the server's CapabilityStatement
  metadata endpoint, filtered by the selected resource type.
- Send standard search parameters as query parameters alongside any FHIRPath
  filters (combined with AND logic, matching the server's existing behaviour).
- Update the search form layout to clearly separate standard search parameters
  from FHIRPath filters, so users can use either or both.

## Capabilities

### New capabilities

- `search-parameter-form`: UI form controls for selecting and entering values
  for standard FHIR search parameters, fetched dynamically from the server's
  CapabilityStatement.

### Modified capabilities

_(none)_

## Impact

- **UI components**: `ResourceSearchForm` will be extended with new controls;
  new sub-components will be created for search parameter input rows.
- **API layer**: The existing `search()` function in `rest.ts` already supports
  generic `params` — no changes needed to the REST client.
- **Types**: `SearchRequest` will be extended to carry standard search
  parameters alongside FHIRPath filters.
- **Server capabilities hook**: The existing `useServerCapabilities` hook
  already fetches the CapabilityStatement; search parameter data will need to be
  extracted from it.
- **Server**: No server changes required — standard search parameters are
  already fully implemented and declared in the CapabilityStatement.

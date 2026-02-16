## 1. Data layer

- [x] 1.1 Extend `ResourceCapability` to include `searchParams: { name: string; type: string }[]` and update `parseCapabilities` to extract search parameters from the CapabilityStatement
- [x] 1.2 Write unit tests for `parseCapabilities` covering resources with search parameters, resources with no search parameters, and missing fields
- [x] 1.3 Extend `SearchRequest` interface to include `params: Record<string, string[]>` for standard search parameters

## 2. Search form UI

- [x] 2.1 Write unit tests for the search parameter row behaviour: adding/removing rows, parameter dropdown population on resource type change, clearing invalid selections on type change, disabled remove button for single row
- [x] 2.2 Add a search parameters section to `ResourceSearchForm` with parameter name dropdown, value text input, add/remove buttons, type badge, and help text
- [x] 2.3 Pass `searchParams` from `ResourceCapability` through the component hierarchy to the search form (from `Resources` page → `ResourceSearchForm`)

## 3. Search execution

- [x] 3.1 Write unit tests for search submission: params included in request, empty rows excluded, multiple values for same parameter sent as repeated params, combination with FHIRPath filters
- [x] 3.2 Update `handleSubmit` in `ResourceSearchForm` to populate `params` on the `SearchRequest` from search parameter rows
- [x] 3.3 Update `useFhirPathSearch` hook to pass `params` through to the `search()` REST function

## 4. End-to-end testing

- [x] 4.1 Add Playwright test for searching with standard search parameters
- [x] 4.2 Add Playwright test for combining standard parameters with FHIRPath filters

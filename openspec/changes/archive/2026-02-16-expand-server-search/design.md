## Context

The Pathling server currently supports search only through FHIRPath expressions
via a `_query=fhirPath` named query. The `SearchExecutor` manually parses
FHIRPath expressions and evaluates them using `DatasetEvaluator`. Meanwhile,
the `fhirpath` module already contains `SearchColumnBuilder` with full support
for standard FHIR search parameters, and the library API exposes this through
`PathlingContext.searchToColumn()`. The server does not use any of this
infrastructure.

The `ConformanceProvider` currently declares only a single `filter` search
parameter (type STRING) for all resource types, making the server
non-discoverable for standard FHIR search use cases.

## Goals / Non-goals

**Goals:**

- Support standard FHIR search parameters directly on resource type endpoints
  (e.g., `GET /Patient?gender=male`).
- Allow combining standard search parameters with FHIRPath `filter` parameters
  in a single request.
- Declare all supported search parameters per resource type in the
  CapabilityStatement.
- Preserve backwards compatibility with the existing `_query=fhirPath` named
  query.

**Non-goals:**

- Chaining, reverse chaining, `_include`, or `_revinclude` support.
- Composite search parameter type support.
- Special search parameter type support.
- Sorting via `_sort`.
- `_summary`, `_elements`, or `_total` support.

## Decisions

### Use `SearchColumnBuilder` for both standard and FHIRPath filter evaluation

**Decision:** Replace the manual FHIRPath evaluation in `SearchExecutor` with
`SearchColumnBuilder`, using `fromQueryString()` for standard parameters and
`fromExpression()` for FHIRPath filter expressions.

**Rationale:** `SearchColumnBuilder` already handles both cases correctly,
operating in flat schema mode with eager column construction. Using it
eliminates the manual parser/evaluator orchestration currently in
`SearchExecutor` and ensures consistent behaviour between the library API and
the server. The builder works directly with flat Pathling-encoded datasets,
which is exactly what the server uses.

**Alternative considered:** Keep `SearchExecutor`'s current FHIRPath evaluation
and add a separate path for standard parameters. Rejected because it would
duplicate logic and risk inconsistencies.

### HAPI integration: method routing and parameter capture

**Decision:** Use HAPI's `@Search(allowUnknownParams = true)` attribute and
`@RawParam` annotation to capture standard search parameters without declaring
them individually.

**Rationale:** HAPI's `@Search` annotation has an `allowUnknownParams`
attribute (default `false`). When set to `true`, the method is invoked even
when the request contains URL parameters not declared via `@OptionalParam` or
`@RequiredParam`. HAPI's `@RawParam` annotation captures unclaimed parameters
into a `Map<String, List<String>>`, automatically excluding:

- Parameters starting with `_` (HAPI/FHIR control parameters).
- Parameters already declared via `@OptionalParam` on the same method (e.g.,
  `filter`).

The `@RawParam` map keys retain the full parameter name including any modifier
suffix (e.g., `gender:not` stays as `gender:not`). This is because
`RawParamsParameter.translateQueryParametersIntoServerArgument()` uses the
original request parameter name as the map key (line 75 of
`RawParamsParameter.java`), even though it extracts `QualifierDetails`
internally for filtering against declared parameters.

This approach avoids the need to declare `@OptionalParam` for every possible
search parameter across every resource type and eliminates manual parameter
filtering that would be needed with `RequestDetails.getParameters()`.

**Alternatives considered:**

HAPI Plain Server discovers search parameters through reflection at startup.
`RestfulServer.findResourceMethods()` scans provider classes, and
`BaseMethodBinding.bindMethod()` inspects method annotations. For `@Search`
methods, `MethodUtil.getResourceParameters()` extracts each
`@OptionalParam`/`@RequiredParam` into a `SearchParameter` object. These are
stored in `SearchMethodBinding.myOptionalParamNames` and
`myRequiredParamNames`, which are then used for request validation in
`SearchMethodBinding.incomingServerRequestMatchesMethod()`. That method checks
every incoming request parameter against the declared lists — unknown
parameters cause the method to return `MethodMatchEnum.NONE` (rejection)
unless `allowUnknownParams` is `true` (which downgrades to `APPROXIMATE`
instead).

Importantly, HAPI's CapabilityStatement generation (via
`ServerCapabilityStatementProvider`) and request parameter validation are
**decoupled systems**. The CapabilityStatement is derived from method bindings
plus an optional `ISearchParamRegistry`, while validation operates purely on
the binding's declared parameter lists. Pathling already overrides the
CapabilityStatement with a custom `ConformanceProvider`, so HAPI's
auto-generated metadata is unused regardless of approach.

1. **Declaring `@OptionalParam` for each search parameter.** Rejected for
   several reasons:

    - **Scale:** There are hundreds of search parameters across all FHIR R4
      resource types. Declaring them all as `@OptionalParam` on a single method
      is impractical and would produce an unwieldy method signature.
    - **Resource-specificity:** Each resource type has different search
      parameters. `SearchProvider` is a prototype-scoped Spring bean instantiated
      once per resource type, but its `@Search` method signature is fixed at
      compile time. A single method would need to declare the union of all
      parameters across all resource types, with most being irrelevant for any
      given instance.
    - **Redundant CapabilityStatement:** Declaring `@OptionalParam` annotations
      would cause HAPI to auto-generate search parameter metadata that Pathling
      never uses (since it has a custom `ConformanceProvider`), while we would
      still need to populate the custom CapabilityStatement from our own
      registry — creating two sources of truth with no benefit.

2. **Generating provider classes dynamically at runtime.** We could use a
   bytecode generation library (e.g., Byte Buddy) to create resource-specific
   provider subclasses with the correct `@OptionalParam` annotations per
   resource type. Rejected because it introduces a runtime bytecode
   manipulation dependency, is fragile across HAPI version upgrades (annotation
   contracts may change), difficult to test and debug, and still suffers from
   the redundant CapabilityStatement problem.

3. **Intercepting method bindings via `SERVER_PROVIDER_METHOD_BOUND`.** HAPI
   fires the `SERVER_PROVIDER_METHOD_BOUND` pointcut at startup for each
   method binding discovered during provider scanning. The interceptor receives
   the `BaseMethodBinding` and can return a replacement. In theory, we could
   intercept each `SearchMethodBinding`, wrap or replace it with a custom
   subclass that overrides `incomingServerRequestMatchesMethod()` to accept
   parameters from the Pathling search parameter registry. However, this
   operates on HAPI-internal classes (`SearchMethodBinding`,
   `BaseQueryParameter`) that are not part of the public API. Constructing a
   valid `SearchMethodBinding` programmatically requires reproducing the
   annotation-processing pipeline, and the parameter lists are frozen at
   construction time with no mutation API. The approach would be tightly
   coupled to HAPI internals and likely to break across versions.

4. **Modifying bindings via reflection after creation.** After HAPI creates
   the `SearchMethodBinding`, we could use Java reflection to mutate
   `myOptionalParamNames` to include the parameters from our registry. This
   avoids bytecode generation but is equally fragile — it depends on the exact
   field names of HAPI internal classes and would break silently if HAPI
   renames or restructures these fields.

The chosen approach (`allowUnknownParams = true` + `@RawParam`) is the
simplest and most maintainable. It uses only HAPI's public API, avoids
coupling to internal class structure, and delegates all parameter validation
to `SearchColumnBuilder` which already handles unknown parameter errors with
clear diagnostics. The trade-off is that HAPI itself does not perform
parameter validation — but since Pathling's `SearchColumnBuilder` provides
equivalent validation against its own registry, this is acceptable.

#### SearchProvider method structure

`SearchProvider` currently has three pieces:

1. `@Search` — unfiltered search (no parameters).
2. `@Search(queryName = "fhirPath")` — FHIRPath filter search with
   `@OptionalParam(name = "filter") StringAndListParam`.
3. `buildSearchExecutor()` — shared constructor call.

The new structure will be:

1. **`@Search(allowUnknownParams = true)`** — handles all non-named-query
   searches, replacing the current unparameterised `search()` method. Accepts
   `@RawParam Map<String, List<String>>` to capture standard search parameters.
   When the map is null or empty, returns all resources (equivalent to the
   current unfiltered search). When standard parameters are present, delegates
   to `SearchExecutor` with the parameter map.

2. **`@Search(queryName = "fhirPath", allowUnknownParams = true)`** — handles
   FHIRPath filter searches. Retains the existing `@OptionalParam(name =
"filter") StringAndListParam` for backwards compatibility. Also accepts
   `@RawParam Map<String, List<String>>` to capture any additional standard
   search parameters that accompany the FHIRPath filters. This enables combined
   queries like
   `GET /Patient?gender=male&_query=fhirPath&filter=birthDate > @1980`.

The current unparameterised `search()` method is removed rather than kept
alongside the new method. HAPI's method matching ranks `EXACT` matches (all
declared params present) above `APPROXIMATE` matches (unknown params tolerated).
Keeping both methods would create ambiguity: the unparameterised method would
get an `EXACT` match for parameterless requests, while the new method with
`@RawParam` and `allowUnknownParams = true` would match as `APPROXIMATE`. By
merging them into a single method, routing is unambiguous regardless of whether
standard parameters are present.

HAPI routes requests based on the `_query` parameter: if `_query=fhirPath` is
present, method 2 is selected; otherwise method 1 is selected. The
`allowUnknownParams = true` flag ensures neither method rejects requests
containing parameters it doesn't explicitly declare.

#### Parameter extraction via @RawParam

The `@RawParam` annotation automatically handles the filtering that would
otherwise need to be done manually. HAPI's `RawParamsParameter` implementation:

1. Skips all parameters starting with `_` (`_count`, `_offset`, `_format`,
   `_query`, etc.).
2. Skips parameters already claimed by `@OptionalParam` on the same method
   (e.g., `filter`).
3. Passes through unclaimed parameters with their original names, including any
   modifier suffix (e.g., `gender:not` is a single map key).

The resulting `Map<String, List<String>>` is reconstructed into a query string
for `SearchColumnBuilder.fromQueryString()`. For example, if the `@RawParam`
map contains `{gender: ["male"], birthdate: ["ge1990-01-01"]}`, the
reconstructed query string would be `gender=male&birthdate=ge1990-01-01`.

If a parameter has multiple values in the list (from a repeated URL parameter
like `?birthdate=ge1990&birthdate=le2000`), each value becomes a separate
`key=value` pair, preserving FHIR's AND semantics. Modifier suffixes on map
keys (e.g., `gender:not`) are preserved in the query string, which
`FhirSearch.fromQueryString()` parses correctly via its `parseParameterCode()`
method.

### SearchExecutor refactoring

**Decision:** `SearchExecutor` will accept two optional inputs: a standard
search parameter query string and a `StringAndListParam` for FHIRPath filters.
It will use `SearchColumnBuilder` for both.

**Current flow:**

1. Read flat dataset from `DataSource`.
2. Create `DatasetEvaluator` and `Parser`.
3. For each FHIRPath filter, parse → evaluate → extract boolean column.
4. Combine with AND/OR logic.
5. Join filtered IDs back to flat dataset.

**New flow:**

1. Read flat dataset from `DataSource`.
2. Create `SearchColumnBuilder` with default registry.
3. If standard search parameters are present, call
   `builder.fromQueryString(resourceType, queryString)` to get a column.
4. If FHIRPath filters are present, iterate the `StringAndListParam` structure
   preserving the existing AND/OR combining semantics:
    - For each `StringOrListParam` group, call
      `builder.fromExpression(resourceType, expression)` for each individual
      `StringParam` and combine the resulting columns with OR.
    - Combine the OR groups with AND.
      Note: `fromExpression()` handles a single expression only. The AND/OR
      combining loop from `StringAndListParam` must be preserved in
      `SearchExecutor` — it is not delegated to `SearchColumnBuilder`.
5. Combine the standard column and FHIRPath column with AND.
6. Apply the combined filter to the flat dataset.

The key simplification is that `SearchColumnBuilder` produces Spark `Column`
objects that work directly on flat Pathling-encoded datasets — no
`DatasetEvaluator`, `Parser`, `ResourceCollection`, or `left_semi` join are
needed. The filter column can be applied directly via `dataset.filter(column)`.

**Column compatibility:** `SearchColumnBuilder` creates a
`SingleResourceEvaluator` in flat schema mode, which generates Column
references as direct column access (e.g., `col("gender")` rather than
`col("Patient").getField("gender")`). This is the same flat schema that
`DataSource.read()` produces for Pathling-encoded datasets. The
`left_semi` join in the current implementation exists because
`DatasetEvaluator` may produce a dataset with a different column layout; since
`SearchColumnBuilder` operates in flat schema mode and produces standalone
`Column` expressions (not bound to a specific dataset), the join is no longer
necessary — the columns reference the same names present in the flat dataset.

### Combine standard and FHIRPath filters with AND logic

**Decision:** Standard search parameters and FHIRPath filter expressions are
combined using boolean AND. Within standard parameters, FHIR's combining rules
apply (repeated parameters = AND, comma-separated values = OR).

**Rationale:** This matches FHIR search combining semantics and is intuitive
for users: "give me patients where gender is male AND that match this FHIRPath
expression".

### Populate CapabilityStatement from the search parameter registry

**Decision:** `ConformanceProvider` will load the `SearchParameterRegistry` and
declare each parameter for each resource type in the CapabilityStatement,
including the correct parameter type.

**Rationale:** The registry already contains the complete set of supported
parameters. Deriving the CapabilityStatement from it ensures accuracy and avoids
manual maintenance.

#### CapabilityStatement integration

`ConformanceProvider` currently constructs
`CapabilityStatementRestResourceComponent` for each resource type in
`buildResources()`. The change adds search parameter declarations within the
existing `if (ops.isSearchEnabled())` block.

The `SearchParameterRegistry` is loaded via
`SearchParameterRegistry.fromInputStream()` using the bundled
`/fhir/R4/search-parameters.json` (same resource `SearchColumnBuilder` uses
with `withDefaultRegistry()`). For each resource type, `registry.getParameter()`
is called to retrieve all parameters, and each is added as a
`CapabilityStatementRestResourceSearchParamComponent` with the correct
`SearchParamType` mapping.

The search parameter type mapping from the registry's `SearchParameterType`
enum to FHIR's `Enumerations.SearchParamType` is:

- `TOKEN` → `SearchParamType.TOKEN`
- `STRING` → `SearchParamType.STRING`
- `DATE` → `SearchParamType.DATE`
- `NUMBER` → `SearchParamType.NUMBER`
- `QUANTITY` → `SearchParamType.QUANTITY`
- `REFERENCE` → `SearchParamType.REFERENCE`
- `URI` → `SearchParamType.URI`

The existing `filter` parameter declaration for the FHIRPath named query is
retained alongside the new standard parameter declarations.

`SearchParameterRegistry` currently only provides
`getParameter(ResourceType, code)` for individual lookups. A new method
`getParameters(ResourceType)` is needed to return all parameters for a resource
type. This is a small addition: the internal structure is already
`Map<ResourceType, Map<String, SearchParameterDefinition>>`, so the new method
returns `Map.copyOf()` of the inner map (or an empty map if the resource type
has no entries). This is a prerequisite for the CapabilityStatement work.

### Keep `_query=fhirPath` as the mechanism for FHIRPath filters

**Decision:** FHIRPath-based filtering continues to require the
`_query=fhirPath` named query with `filter` parameters. Standard search
parameters work without `_query`.

**Rationale:** This maintains backwards compatibility. The `_query` parameter
clearly signals that FHIRPath is being used, and this distinction is important
because FHIRPath filter expressions use different syntax from standard search
values. Users can combine both by including `_query=fhirPath` with `filter`
parameters alongside standard parameters.

## Worked example

This example traces a single request through the entire stack to show how the
components interact. The request combines two standard search parameters with a
FHIRPath filter expression.

### Request

```
GET /Patient?gender=male&birthdate=ge1990-01-01&_query=fhirPath&filter=name.given.count()%20%3E%200&_count=10
```

The intent: find male patients born on or after 1990 who have at least one given
name, returning 10 results per page.

### Step 1: HAPI method routing

HAPI's `RestfulServer` receives the HTTP request and inspects the URL
parameters. It sees `_query=fhirPath`, so it looks for a `@Search` method with
`queryName = "fhirPath"` on the `SearchProvider` instance registered for the
`Patient` resource type.

HAPI finds the method:

```java
@Search(queryName = "fhirPath", allowUnknownParams = true)
public IBundleProvider search(
    @Nullable @OptionalParam(name = "filter") final StringAndListParam filters,
    @Nullable @RawParam final Map<String, List<String>> standardParams)
```

HAPI recognises `filter` (declared via `@OptionalParam`) and `_query`, `_count`
(built-in HAPI parameters). `gender` and `birthdate` are unknown, but
`allowUnknownParams = true` downgrades the match from `EXACT` to `APPROXIMATE`
rather than rejecting it. Since this is the only matching method for the named
query, HAPI selects it.

HAPI parses `filter` into a `StringAndListParam` containing one
`StringOrListParam` with one `StringParam` whose value is the URL-decoded string
`name.given.count() > 0`. HAPI also processes `_count=10` internally for
pagination.

### Step 2: @RawParam captures standard parameters

HAPI's `RawParamsParameter` processes the request parameters, automatically
excluding `_`-prefixed parameters (`_query`, `_count`) and the `filter`
parameter (claimed by `@OptionalParam`). The `@RawParam` map receives:

```
{
  "gender":    ["male"],
  "birthdate": ["ge1990-01-01"]
}
```

`SearchProvider` reconstructs this into a query string:
`gender=male&birthdate=ge1990-01-01`.

If a parameter had multiple values in the list (e.g., `birthdate` →
`["ge1990-01-01", "le2000-01-01"]` from a repeated URL parameter), each value
becomes a separate `key=value` pair in the query string, preserving FHIR's AND
semantics for repeated parameters. Comma-separated OR values (e.g.,
`gender=male,female`) remain within a single string value and are handled later
by `FhirSearch`.

`SearchProvider` passes both the query string and the `StringAndListParam`
filters to the `SearchExecutor` constructor.

### Step 3: SearchExecutor reads the dataset

`SearchExecutor` calls `dataSource.read("Patient")` to get the flat
Pathling-encoded `Dataset<Row>`. This dataset has one row per Patient resource,
with columns like `id`, `gender`, `birthDate`, `name`, etc.

### Step 4: SearchExecutor builds the standard search filter column

`SearchExecutor` creates a `SearchColumnBuilder` via
`SearchColumnBuilder.withDefaultRegistry(fhirContext)`, which loads the bundled
R4 search parameter definitions from `/fhir/R4/search-parameters.json` into a
`SearchParameterRegistry`.

It calls `builder.fromQueryString(ResourceType.PATIENT, "gender=male&birthdate=ge1990-01-01")`.

Inside `fromQueryString`:

1. `FhirSearch.fromQueryString("gender=male&birthdate=ge1990-01-01")` splits on
   `&` and parses each pair, producing a `FhirSearch` with two criteria:

    - `SearchCriterion(parameterCode="gender", modifier=null, values=["male"])`
    - `SearchCriterion(parameterCode="birthdate", modifier=null, values=["ge1990-01-01"])`

2. `fromSearch()` creates a `SingleResourceEvaluator` in flat schema mode for
   `PATIENT`, then processes each criterion:

    **Criterion 1: `gender=male`**

    - `registry.getParameter(PATIENT, "gender")` returns a
      `SearchParameterDefinition` with type `TOKEN` and expression `gender`.
    - The FHIRPath expression `gender` is parsed and evaluated by the
      `SingleResourceEvaluator`. In flat schema mode, this produces a
      `ColumnRepresentation` referencing the `gender` column directly.
    - The FHIR type is resolved to `FHIRDefinedType.CODE`.
    - `SearchParameterType.TOKEN.createFilter(null, CODE)` returns a
      `TokenSearchFilter`.
    - `TokenSearchFilter.buildFilter(genderColumn, ["male"])` produces a Spark
      `Column` expression equivalent to: `gender = 'male'`.

    **Criterion 2: `birthdate=ge1990-01-01`**

    - `registry.getParameter(PATIENT, "birthdate")` returns a definition with
      type `DATE` and expression `birthDate`.
    - The FHIRPath expression `birthDate` evaluates to a column referencing
      `birthDate`.
    - The FHIR type is resolved to `FHIRDefinedType.DATE`.
    - `SearchParameterType.DATE.createFilter(null, DATE)` returns a
      `DateSearchFilter`.
    - `DateSearchFilter.buildFilter(birthDateColumn, ["ge1990-01-01"])` parses
      the `ge` prefix and produces a Spark `Column` expression equivalent to:
      `birthDate >= '1990-01-01'`.

3. The two criterion columns are combined with AND:
   `(gender = 'male') AND (birthDate >= '1990-01-01')`.

`fromQueryString` returns this combined column. Call it `standardColumn`.

### Step 5: SearchExecutor builds the FHIRPath filter column

`SearchExecutor` processes the `StringAndListParam` filters. There is one
`StringOrListParam` containing one `StringParam` with value
`name.given.count() > 0`.

It calls `builder.fromExpression(ResourceType.PATIENT, "name.given.count() > 0")`.

Inside `fromExpression`:

1. The `Parser` parses `name.given.count() > 0` into a FHIRPath AST.
2. The `SingleResourceEvaluator` evaluates this against the Patient schema in
   flat schema mode, producing a Spark `Column` expression that counts the given
   name elements and compares to zero.

The result is a boolean `Column`. Call it `fhirPathColumn`.

If the `StringAndListParam` contained multiple `StringOrListParam` groups (AND
between groups) each containing multiple `StringParam` entries (OR within a
group), `SearchExecutor` would combine them accordingly — mirroring the existing
combining logic from the current implementation.

### Step 6: SearchExecutor combines and applies the filter

The standard and FHIRPath columns are combined with AND:

```
finalFilter = coalesce(standardColumn AND fhirPathColumn, false)
```

The `coalesce(..., false)` ensures that null results (e.g., from missing
elements) are treated as non-matches.

The filter is applied to the flat dataset:

```
result = flatDataset.filter(finalFilter)
```

This produces a filtered `Dataset<Row>` containing only Patient rows where all
three conditions are true: gender is male, birthDate is on or after 1990-01-01,
and at least one given name exists.

The result is cached if configured (`cacheResults` flag from server
configuration).

### Step 7: HAPI pagination and response encoding

HAPI calls `SearchExecutor.size()` to get the total count (lazy — only computed
on first call via `result.count()`).

HAPI then calls `SearchExecutor.getResources(0, 10)` (based on `_count=10`).
The method applies offset/limit logic to the filtered dataset, encodes the rows
into HAPI `Patient` objects using `FhirEncoders`, and returns them.

HAPI wraps the results in a `Bundle` with `type: searchset`, sets
`Bundle.total` from `size()`, and adds pagination links (`self`, `next` if more
results exist). The response is serialised as `application/fhir+json` and
returned to the client.

### Error handling for search parameter exceptions

**Decision:** The search parameter exceptions (`UnknownSearchParameterException`,
`InvalidModifierException`, `InvalidSearchParameterException`) must be changed
to extend `InvalidUserInputError` instead of plain `RuntimeException`.

**Rationale:** The server's `ErrorHandlingInterceptor` maps exception types to
HTTP status codes. `InvalidUserInputError` maps to `400 Bad Request` via
`InvalidRequestException`. The search exceptions currently extend
`RuntimeException`, which would fall through to the generic `Throwable` catch
and return `500 Internal Server Error` — wrong for user input validation
failures.

Since `fhirpath` transitively depends on `utilities` (via `terminology`),
`InvalidUserInputError` is available at compile time. Changing the exception
hierarchy is the cleanest fix: it ensures consistent 400 responses without
requiring catch-and-wrap logic in `SearchExecutor`.

### ViewDefinition search with standard parameters

**Decision:** When a client sends standard search parameters for a non-registry
resource type (e.g., `GET /ViewDefinition?name=foo`), the server returns a
`400 Bad Request` with a clear error message.

**Rationale:** `SearchColumnBuilder.fromQueryString()` delegates to
`SearchParameterRegistry.getParameter()`, which returns empty for resource types
not in the registry. `UnknownSearchParameterException` is thrown with a message
identifying the parameter and resource type. Since this exception will extend
`InvalidUserInputError` (see above), the error handler maps it to 400.

No special-casing is needed in `SearchProvider` or `SearchExecutor` — the
existing error path handles it correctly once the exception hierarchy is fixed.

## Risks / Trade-offs

- **Unknown parameters:** With `allowUnknownParams = true`, HAPI will not
  reject requests with unrecognised parameters at the framework level. Instead,
  `SearchColumnBuilder` will throw when it encounters an unknown parameter name
  during query string processing. Since the search exceptions extend
  `InvalidUserInputError`, the `ErrorHandlingInterceptor` maps them to
  `400 Bad Request` with an `OperationOutcome` containing the parameter name.

- **Underscore-prefixed search parameters:** `@RawParam` automatically filters
  out all parameters starting with `_`. FHIR defines some standard search
  parameters with underscore prefixes (e.g., `_id`, `_lastUpdated`, `_tag`,
  `_profile`, `_security`). These will not be supported as search parameters in
  this change. → Mitigation: This is acceptable as an initial limitation. These
  parameters can be added later by maintaining an explicit allowlist of
  supported underscore-prefixed parameters.

- **Parameter conflicts:** Some standard search parameter names could
  theoretically conflict with HAPI built-in parameters. → Mitigation: HAPI
  processes its own parameters (those starting with `_`) before the provider
  method is called, so standard FHIR search parameters (which don't start
  with `_`) should not conflict.

- **CapabilityStatement size:** Declaring all search parameters for all
  resource types will significantly increase the CapabilityStatement size. →
  Mitigation: This is standard practice for FHIR servers and clients expect it.

- **ViewDefinition search:** The current implementation also supports search on
  the custom `ViewDefinition` resource type. Standard FHIR search parameters
  don't apply to ViewDefinition since it's not in the registry. Sending
  standard parameters for ViewDefinition returns `400 Bad Request` with a
  message identifying the unsupported parameter. ViewDefinition continues to
  support FHIRPath filters only.

## Context

The Pathling server implements the FHIR Bulk Data Access specification for exporting large datasets. Currently, `_typeFilter` is listed in `ExportOperationValidator.UNSUPPORTED_QUERY_PARAMS` and is rejected (strict mode) or silently ignored (lenient mode).

The server already has mature infrastructure for translating FHIR search parameters into SparkSQL Column expressions. `PathlingContext.searchToColumn()` provides a public API that takes a resource type and a search query string, returning a SparkSQL Column expression. This is backed by `SearchColumnBuilder` and `FhirSearch` in the `fhirpath` module, and is the same infrastructure used by the `SearchProvider` for the standard FHIR search operation. `PathlingContext` is already injected into `ExportExecutor`. The bulk export execution pipeline operates on `QueryableDataSource`, which supports per-resource-type dataset transformations via its `map(BiFunction<String, Dataset<Row>, Dataset<Row>>)` method.

The `_typeFilter` parameter format is `[ResourceType]?[search-params]` (e.g., `MedicationRequest?status=active`). Multiple `_typeFilter` values targeting the same resource type are combined with OR logic per the specification.

## Goals / Non-goals

**Goals:**

- Accept and validate `_typeFilter` values on all three export endpoints.
- Parse the resource type prefix and search query from each `_typeFilter` value.
- Apply search filters during export execution using `PathlingContext.searchToColumn()`.
- Support all search parameter types already supported by the search infrastructure (token, string, date, number, quantity, reference, URI).
- Handle lenient mode consistently with existing parameter handling patterns.
- Implicitly include resource types referenced in `_typeFilter` when `_type` is not specified, per the Bulk Data specification.

**Non-goals:**

- Supporting `_include` or `_sort` within `_typeFilter` queries (explicitly excluded by the specification).
- Supporting composite or special search parameter types (not yet implemented in the search infrastructure).
- Extending the search infrastructure with new capabilities; only using what already exists.

## Decisions

### 1. Use `PathlingContext.searchToColumn()` for filter translation

**Decision:** Use the existing `PathlingContext.searchToColumn()` method to translate `_typeFilter` search queries into SparkSQL Column expressions. `PathlingContext` is already injected into `ExportExecutor`, so no new dependencies are needed.

**Rationale:** This is the public API for search-to-column translation, backed by `SearchColumnBuilder` and `FhirSearch`. It handles all standard FHIR search parameter types, modifiers, complex type expansion (HumanName, Address), and escaping. Building a separate filter mechanism would duplicate logic and diverge from the behaviour of the standard search operation.

**Alternative considered:** Implementing a simpler string-based filter mechanism. Rejected because it would not support the full range of FHIR search semantics.

### 2. Parse `_typeFilter` in `ExportOperationValidator`

**Decision:** Parse and validate `_typeFilter` values in the validator, producing a `Map<String, List<String>>` keyed by resource type code. Each value is a list of search query strings (multiple filters for the same type are OR'd). The raw query strings are stored rather than parsed `FhirSearch` objects, since `PathlingContext.searchToColumn()` accepts strings directly.

**Rationale:** Keeps validation and parsing co-located with existing parameter processing. The parsed searches can be validated early (e.g., unknown resource types, invalid search parameters) before the async job starts.

**Alternative considered:** Parsing in `ExportExecutor`. Rejected because validation errors should be reported synchronously before the 202 Accepted response, consistent with how `_type` validation works.

### 3. Store parsed type filters in `ExportRequest`

**Decision:** Add a `Map<String, List<String>> typeFilters` field to the `ExportRequest` record, keyed by resource type code. Values are search query strings (e.g., `"code=8867-4&date=ge2024-01-01"`).

**Rationale:** The record already carries all other parsed export parameters. Storing the query strings keeps the request record simple and aligns with `PathlingContext.searchToColumn()` which accepts strings directly.

### 4. Apply filters in `ExportExecutor` using `QueryableDataSource.map()`

**Decision:** Add an `applyTypeFilters` step in the `ExportExecutor.execute()` pipeline, after resource type filtering and date filtering but before patient compartment filtering. For each resource type with filters, call `PathlingContext.searchToColumn()` for each query string and combine with OR, then apply as a dataset filter.

**Rationale:** Placing the filter after resource type filtering ensures we only build search columns for resource types that are actually being exported. Placing it before patient compartment filtering maintains the existing pipeline ordering where broad filters apply first.

### 5. Implicit resource type inclusion from `_typeFilter`

**Decision:** When `_type` is not specified but `_typeFilter` is, the resource types mentioned in `_typeFilter` are treated as the effective type filter. When both are specified, `_typeFilter` entries for types not in `_type` are silently ignored in lenient mode or rejected in strict mode.

**Rationale:** This follows the Bulk Data specification: "If no `_type` parameter is provided, the server SHALL include resources of all types that are referenced in any `_typeFilter` parameter."

## Risks / Trade-offs

- **Unknown search parameters:** Some search parameters may not be supported by the search infrastructure (e.g., composite, special types). If a `_typeFilter` references an unsupported parameter, the server will return an error in strict mode or skip the filter in lenient mode.

- **Performance:** Complex `_typeFilter` queries with many criteria may generate large SparkSQL expressions. Mitigation: Spark's query optimiser handles predicate pushdown effectively, and the filter is applied before data is written to the output files.

- **No server-side search parameter documentation:** The server does not currently advertise supported search parameters in its CapabilityStatement. Clients may need to discover supported parameters through trial and error. This is an existing limitation and out of scope for this change.

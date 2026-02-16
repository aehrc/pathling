## 0. Prerequisite: SearchParameterRegistry iteration API

- [x] 0.1 Add a `getParameters(ResourceType)` method to
      `SearchParameterRegistry` that returns all search parameter definitions for a
      given resource type as a `Map<String, SearchParameterDefinition>` (or empty
      map if the resource type has no entries)
- [x] 0.2 Write unit tests for the new `getParameters(ResourceType)` method

## 0b. Prerequisite: Fix search exception hierarchy

- [x] 0b.1 Change `UnknownSearchParameterException`,
      `InvalidModifierException`, and `InvalidSearchParameterException` to extend
      `InvalidUserInputError` instead of `RuntimeException`. The `fhirpath` module
      has access to `InvalidUserInputError` via its transitive dependency on
      `utilities` through `terminology`.
- [x] 0b.2 Verify that existing `SearchColumnBuilder` tests still pass after
      the exception hierarchy change

## 1. Tests for standard search parameter support

- [x] 1.1 Write unit tests for `SearchExecutor` verifying standard search
      parameters produce correct filtered results (token, string, date, number,
      quantity, reference, URI types)
- [x] 1.2 Write unit tests for `SearchExecutor` verifying FHIR combining
      semantics (AND between repeated parameters, OR within comma-separated values)
- [x] 1.3 Write unit tests for `SearchExecutor` verifying combined standard
      parameters and FHIRPath filters produce correct results
- [x] 1.4 Write unit tests for `SearchExecutor` verifying backwards
      compatibility with FHIRPath-only search via `_query=fhirPath`
- [x] 1.5 Write unit tests for `SearchExecutor` verifying error handling for
      unknown parameters and unsupported modifiers, including that the exceptions
      propagate as `InvalidUserInputError` (which maps to HTTP 400 via
      `ErrorHandlingInterceptor`)
- [x] 1.6 Write unit tests for `SearchExecutor` verifying that standard
      parameters on non-registry resource types (e.g., ViewDefinition) produce a
      clear `400 Bad Request` error

## 2. Tests for CapabilityStatement

- [x] 2.1 Write unit tests for `ConformanceProvider` verifying search
      parameters from the registry are declared per resource type with correct types
- [x] 2.2 Write unit tests for `ConformanceProvider` verifying the `filter`
      parameter for FHIRPath search continues to be declared

## 3. SearchProvider changes

- [x] 3.1 Remove the current unparameterised `search()` method and replace it
      with a single `@Search(allowUnknownParams = true)` method that accepts
      `@RawParam Map<String, List<String>>` for standard search parameters. When
      the map is null or empty, return all resources (equivalent to the current
      unfiltered search).
- [x] 3.2 Add `@RawParam Map<String, List<String>>` to the
      `@Search(queryName = "fhirPath", allowUnknownParams = true)` method alongside
      the existing `@OptionalParam(name = "filter") StringAndListParam`
- [x] 3.3 Implement query string reconstruction from the `@RawParam` map:
      iterate entries, joining each key-value pair with `=` and pairs with `&`.
      For multi-valued entries (repeated URL parameters), emit separate `key=value`
      pairs for each value. Modifier suffixes in map keys (e.g., `gender:not`) are
      preserved as-is.
- [x] 3.4 Pass both the reconstructed query string (or null if empty) and
      FHIRPath filter expressions to `SearchExecutor`

## 4. SearchExecutor changes

- [x] 4.1 Refactor `SearchExecutor` to accept an optional standard search
      parameter query string alongside the existing optional FHIRPath filters
- [x] 4.2 ~~Replace manual FHIRPath evaluation with
      `SearchColumnBuilder.fromExpression()`~~ — Kept `DatasetEvaluator` for
      FHIRPath filters because `fromExpression()` doesn't support truthy semantics
      (non-boolean collections) and `ResourceType.fromCode()` fails for custom types
      like ViewDefinition. Used hybrid approach instead.
- [x] 4.3 Use `SearchColumnBuilder.fromQueryString()` to build filter columns
      from the standard search parameter query string
- [x] 4.4 ~~Combine and apply directly via `dataset.filter(column)`~~ — Standard
      params use direct `filter()`, FHIRPath filters use `left_semi` join (required
      by DatasetEvaluator's separate dataset). Both are combined sequentially.

## 5. ConformanceProvider changes

- [x] 5.1 Load the `SearchParameterRegistry` using
      `SearchParameterRegistry.fromInputStream()` with the bundled
      `/fhir/R4/search-parameters.json` resource (same resource
      `SearchColumnBuilder` uses)
- [x] 5.2 For each resource type, call `registry.getParameters(resourceType)`
      (the new iteration method from task 0.1) and add each parameter to the
      CapabilityStatement as a
      `CapabilityStatementRestResourceSearchParamComponent` with the correct
      `SearchParamType` mapping
- [x] 5.3 Retain the existing `filter` parameter declaration for FHIRPath
      search alongside the new standard parameters

## 6. Integration tests

- [x] 6.1 Write an integration test that exercises standard search parameters
      through the full FHIR REST stack (HTTP request to Bundle response), verifying
      both successful filtering and correct HTTP 400 error responses for unknown
      parameters
- [x] 6.2 Write an integration test verifying HAPI method routing: requests
      without `_query` are routed to the standard search method, requests with
      `_query=fhirPath` are routed to the FHIRPath method, and both handle the
      presence or absence of standard parameters correctly

## 7. Documentation

- [x] 7.1 Update the server search documentation
      (`site/docs/server/operations/search.md`) to cover:
    - Supported standard search parameter types (token, string, date, number,
      quantity, reference, URI) with examples for each
    - Supported modifiers (`:not`, `:exact`, `:below`, `:above`, resource type
      modifiers for references) with examples
    - Combined usage of standard parameters with FHIRPath filters
      (`_query=fhirPath` with both `filter` and standard parameters)
    - Limitations: `_id`, `_lastUpdated`, `_tag`, `_profile`, `_security` and
      other underscore-prefixed parameters are not yet supported; composite and
      special parameter types are not supported
    - Error behaviour: unknown parameters and unsupported modifiers return
      `400 Bad Request` with an `OperationOutcome`

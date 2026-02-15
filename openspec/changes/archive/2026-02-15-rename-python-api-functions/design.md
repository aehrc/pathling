## Context

The R API currently exposes two functions for converting FHIR expressions to Spark columns:

- `pc_fhirpath_to_column` - converts FHIRPath expressions
- `pc_search_to_column` - converts FHIR search expressions

These functions use the `pc_` prefix (likely short for "PathlingContext"), which is not descriptive or discoverable. Most other functions in the R API use the `pathling_` prefix (e.g., `pathling_connect`, `pathling_filter`, `pathling_with_column`), creating an inconsistency.

The functions are implemented in `lib/R/R/context.R` and are used by:

- `pathling_filter` - calls both functions internally depending on expression type
- `pathling_with_column` - calls `pc_fhirpath_to_column` internally
- User code - direct invocations in examples and documentation

## Goals / Non-Goals

**Goals:**

- Rename `pc_fhirpath_to_column` to `pathling_fhirpath_to_column`
- Rename `pc_search_to_column` to `pathling_search_to_column`
- Update all internal usages and tests
- Maintain consistent naming across the R API

**Non-Goals:**

- Adding deprecation warnings or backwards compatibility aliases (this is a breaking change)
- Changing the Python or Java APIs (this only affects R)
- Modifying the underlying Java method names or behavior

## Decisions

### Use simple rename without deprecation period

**Decision**: Perform a direct rename without deprecation aliases.

**Rationale**: The R API is relatively new and the project follows semantic versioning. This change will be part of a major version increment. Adding deprecation aliases would:

- Complicate the API surface
- Require additional maintenance
- Delay the consistency improvement

**Alternatives considered**:

- Add `.Deprecated()` warnings: Adds complexity for a relatively new API
- Keep both names: Creates confusion and bloats the namespace

### Update internal usages atomically

**Decision**: Update `pathling_filter` and `pathling_with_column` in the same commit as the function renames.

**Rationale**: These functions are tightly coupled. Updating them together ensures:

- No intermediate broken state
- Easier code review
- Simpler git history

**Alternatives considered**:

- Separate commits: Would require temporary aliases or result in broken intermediate states

## Risks / Trade-offs

**Risk**: Users with existing code will experience breaking changes
→ **Mitigation**: This will be part of a major version increment per semantic versioning. Release notes will clearly document the rename and provide migration guidance.

**Risk**: Examples in external documentation or blog posts will be outdated
→ **Mitigation**: Update all official documentation and examples. External content is outside our control but will be addressed through version-specific documentation.

**Trade-off**: Breaking change vs long-term maintainability
→ **Decision**: Accept the breaking change for long-term API consistency and discoverability.

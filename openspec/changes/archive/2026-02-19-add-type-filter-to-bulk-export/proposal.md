## Why

The `_typeFilter` parameter is a standard part of the FHIR Bulk Data Access specification (v3.0.0) that allows clients to apply FHIR search queries to filter exported resources. Pathling already supports the `_type` parameter for coarse resource type filtering, but lacks the ability to apply fine-grained search criteria during export (e.g., only active medications, or observations from a specific date range). This limits the usefulness of bulk export for clients that need targeted subsets of data without downloading and post-filtering entire resource collections. The server currently rejects `_typeFilter` in strict mode and ignores it in lenient mode.

## What changes

- Parse and validate `_typeFilter` values in the export operation validator, extracting the resource type prefix and search query string from each filter (format: `[ResourceType]?[search-params]`).
- Add `_typeFilter` to the `ExportRequest` record so that parsed type filters are available during execution.
- Add a `_typeFilter` parameter to all three export provider signatures (`SystemExportProvider`, `PatientExportProvider`, `GroupExportProvider`).
- Apply type filters during export execution in `ExportExecutor` using the existing `SearchColumnBuilder` infrastructure, which already translates FHIR search queries into SparkSQL Column expressions.
- Remove `_typeFilter` from the `UNSUPPORTED_QUERY_PARAMS` set.
- Multiple `_typeFilter` values targeting the same resource type are combined with OR logic, per the specification.
- When `_typeFilter` is provided without a corresponding `_type` entry, the resource types from `_typeFilter` are implicitly included, per the specification.

## Capabilities

### New capabilities

- `bulk-export-type-filter`: Support for the `_typeFilter` parameter in the bulk export operation, enabling FHIR search-based filtering of exported resources.

### Modified capabilities

(none)

## Impact

- **Server module**: Changes to export providers, validator, request record, and executor.
- **API surface**: New query parameter accepted on all three `$export` endpoints.
- **Dependencies**: Leverages existing `SearchColumnBuilder` and `FhirSearch` from the `fhirpath` module (no new dependencies).
- **Backwards compatibility**: Fully backwards compatible. Existing requests without `_typeFilter` behave identically.

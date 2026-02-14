## Why

Extensions on FHIR elements nested inside choice types (e.g., `value[x]`,
`effective[x]`, `onset[x]`) are silently dropped during encoding. This is
caused by a hash mismatch in `flattenExtensions` where `children()` returns
property names with the `[x]` suffix but `getProperty()` expects the base name
without it. Reported in GitHub issue #2538.

## What changes

- Fix `flattenExtensions` in `SerializerBuilder.scala` to use
  `Property.getValues()` directly instead of the broken
  `children()` → `getProperty()` hash-based lookup.
- Add test data with extensions on Coding elements within choice types.
- Add encoder tests verifying extensions on choice-type children are preserved
  through serialisation and deserialisation.
- Add FHIRPath tests verifying `coding.extension('url')` returns values for
  extensions nested inside choice types.

## Capabilities

### New capabilities

_None._

### Modified capabilities

_None — this is a bug fix to existing extension encoding behaviour, not a
requirements change._

## Impact

- **Encoders module**: `SerializerBuilder.scala` —
  `SerializerBuilderProcessor.flattenExtensions` method.
- **Affected element types**: Any FHIR element reached through a choice type
  (`value[x]`, `effective[x]`, `onset[x]`, `medication[x]`, etc.) where the
  element or its descendants carry extensions.
- **No API changes**: The fix restores expected behaviour; no public API
  modifications.

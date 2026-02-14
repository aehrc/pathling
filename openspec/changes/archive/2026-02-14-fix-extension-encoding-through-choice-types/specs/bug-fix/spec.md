## ADDED Requirements

### Requirement: Extensions on elements within choice types are encoded

The encoder SHALL collect extensions from FHIR elements nested within choice
type properties (e.g., `value[x]`, `effective[x]`, `onset[x]`) into the
resource-level `_extension` map, using the element's `_fid` as the key.

#### Scenario: Extension on Coding within valueCodeableConcept

- **WHEN** an Observation has a `valueCodeableConcept` with a Coding that
  carries an extension (e.g., `ordinalValue`)
- **THEN** the resource's `_extension` map SHALL contain an entry keyed by the
  Coding's `_fid`, with the Coding's extensions as the value

#### Scenario: FHIRPath extension function on Coding within choice type

- **WHEN** a FHIRPath expression navigates to
  `value.ofType(CodeableConcept).coding.extension('url').value.ofType(decimal)`
- **THEN** the expression SHALL return the extension value (not null)

#### Scenario: Extensions at multiple nesting levels within choice types

- **WHEN** a resource has extensions both at the resource level and on elements
  nested within a choice type
- **THEN** the `_extension` map SHALL contain entries for all elements with
  extensions, regardless of whether they are reached through a choice type

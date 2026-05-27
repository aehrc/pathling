## ADDED Requirements

### Requirement: Quantity literal structs have concrete field types

Quantity literal columns produced by `QuantityEncoding.encodeLiteral` SHALL have
every struct field typed to its declared schema type (as defined by
`QuantityEncoding.dataType()`). No field SHALL have Spark `NullType` (`VOID`)
even when its runtime value is null.

#### Scenario: Indefinite calendar duration literal has concrete types

- **WHEN** encoding the FHIRPath literal `1 year` (indefinite calendar duration
  with no canonical form)
- **THEN** the resulting struct column's `_value_canonicalized` field has type
  `STRUCT<value: DECIMAL(38,0), scale: INT>` (not `VOID`), and `id`,
  `comparator`, `_fid` fields have their declared types (not `VOID`)

#### Scenario: UCUM quantity literal has concrete types

- **WHEN** encoding the FHIRPath literal `3 'min'` (canonicalisable UCUM
  quantity)
- **THEN** all struct fields have their declared types (not `VOID`)

#### Scenario: repeat($this) over combined indefinite Quantity literals succeeds

- **WHEN** evaluating `(1 year).combine(12 months).repeat($this)`
- **THEN** the expression returns a collection containing two Quantity values
  (1 year and 12 months) without error

#### Scenario: repeat($this) over combined UCUM Quantity literals succeeds

- **WHEN** evaluating `(3 'min').combine(180 seconds).repeat($this)`
- **THEN** the expression returns a collection containing one Quantity value
  (the two are equal after canonicalisation, so deduplication collapses them)

### Requirement: FlexiDecimal null literals have concrete type

`FlexiDecimalSupport.toLiteral(null)` SHALL return a column of type
`FlexiDecimal.DATA_TYPE` (not `VOID`).

#### Scenario: toLiteral with null produces typed null

- **WHEN** calling `FlexiDecimalSupport.toLiteral(null)`
- **THEN** the returned column's data type is
  `STRUCT<value: DECIMAL(38,0), scale: INT>`, not `NullType`

# ViewDefinition Structure Reference

Complete element reference for SQL on FHIR ViewDefinitions.

## Top-Level Elements

| Element        | Card. | Type            | Description                                                 |
| -------------- | ----- | --------------- | ----------------------------------------------------------- |
| `resourceType` | 1..1  | string          | Always "ViewDefinition"                                     |
| `url`          | 0..1  | uri             | Canonical identifier (required for ShareableViewDefinition) |
| `name`         | 0..1  | string          | Computer-friendly name (pattern: `^[A-Za-z][A-Za-z0-9_]*$`) |
| `title`        | 0..1  | string          | Human-readable title                                        |
| `status`       | 1..1  | code            | draft \| active \| retired \| unknown                       |
| `experimental` | 0..1  | boolean         | For testing purposes only                                   |
| `description`  | 0..1  | markdown        | Natural language description                                |
| `resource`     | 1..1  | code            | FHIR resource type to project                               |
| `fhirVersion`  | 0..\* | code            | Target FHIR version(s)                                      |
| `constant`     | 0..\* | BackboneElement | Reusable constant definitions                               |
| `where`        | 0..\* | BackboneElement | Resource filtering criteria                                 |
| `select`       | 1..\* | BackboneElement | Column and row definitions                                  |

## constant Element

Define values referenced via `%name` in FHIRPath expressions.

| Element    | Card. | Type   | Description                                         |
| ---------- | ----- | ------ | --------------------------------------------------- |
| `name`     | 1..1  | string | Reference name (pattern: `^[A-Za-z][A-Za-z0-9_]*$`) |
| `value[x]` | 1..1  | \*     | Constant value (see types below)                    |

**Supported value[x] types:**

- `valueBase64Binary`
- `valueBoolean`
- `valueCanonical`
- `valueCode`
- `valueDate`
- `valueDateTime`
- `valueDecimal`
- `valueId`
- `valueInstant`
- `valueInteger`
- `valueInteger64`
- `valueOid`
- `valueString`
- `valuePositiveInt`
- `valueTime`
- `valueUnsignedInt`
- `valueUri`
- `valueUrl`
- `valueUuid`

## where Element

Filter which resources are included in the view.

| Element       | Card. | Type   | Description                                    |
| ------------- | ----- | ------ | ---------------------------------------------- |
| `path`        | 1..1  | string | FHIRPath expression (must evaluate to boolean) |
| `description` | 0..1  | string | Human-readable explanation                     |

Multiple where elements are ANDed together.

## select Element

Define columns and row iteration. Each select can contain columns directly or nested select structures.

| Element         | Card. | Type            | Description                                  |
| --------------- | ----- | --------------- | -------------------------------------------- |
| `column`        | 0..\* | BackboneElement | Output column definitions                    |
| `select`        | 0..\* | BackboneElement | Nested selection (recursive)                 |
| `forEach`       | 0..1  | string          | FHIRPath producing one row per match         |
| `forEachOrNull` | 0..1  | string          | Like forEach but keeps null row if empty     |
| `repeat`        | 0..\* | string          | FHIRPath expressions for recursive traversal |
| `unionAll`      | 0..\* | BackboneElement | Combine multiple selections                  |

**Constraint**: Only one of `forEach`, `forEachOrNull`, or `repeat` may be specified per select.

## column Element

Define an output column within a select.

| Element       | Card. | Type            | Description                                            |
| ------------- | ----- | --------------- | ------------------------------------------------------ |
| `name`        | 1..1  | string          | Column identifier (pattern: `^[A-Za-z][A-Za-z0-9_]*$`) |
| `path`        | 1..1  | string          | FHIRPath expression for column value                   |
| `description` | 0..1  | string          | Human-readable explanation                             |
| `collection`  | 0..1  | boolean         | True if column may contain arrays                      |
| `type`        | 0..1  | uri             | FHIR StructureDefinition URI for type                  |
| `tag`         | 0..\* | BackboneElement | Implementation-specific metadata                       |

**Type URIs for primitives:**

- `http://hl7.org/fhirpath/System.String` or just `string`
- `http://hl7.org/fhirpath/System.Boolean` or just `boolean`
- `http://hl7.org/fhirpath/System.Integer` or just `integer`
- `http://hl7.org/fhirpath/System.Decimal` or just `decimal`
- `http://hl7.org/fhirpath/System.Date` or just `date`
- `http://hl7.org/fhirpath/System.DateTime` or just `dateTime`
- `http://hl7.org/fhirpath/System.Time` or just `time`

For FHIR types, use the short code: `id`, `code`, `uri`, `instant`, etc.

## unionAll Element

Combine multiple selection branches. All branches must produce identical column schemas.

| Element          | Card. | Type | Description                                         |
| ---------------- | ----- | ---- | --------------------------------------------------- |
| (same as select) |       |      | Each unionAll item has the same structure as select |

## repeat Element

Enable recursive traversal of nested structures to any depth. Takes an array of FHIRPath expressions that define paths to recursively follow.

**How repeat works:**

1. Start at the current context (resource root or forEach context)
2. Evaluate each path expression in the array
3. For each result, recursively apply the same path patterns
4. Continue until no more matches are found
5. Union all results from all depth levels

**Use cases:**

- QuestionnaireResponse items with nested `item` and `answer.item` paths
- Hierarchical structures with unknown or variable depth
- Any recursive FHIR element pattern

**Key difference from forEach:**

- `forEach` iterates over a single level of a collection
- `repeat` recursively traverses to arbitrary depth, collecting all matches

## Function Precedence

When multiple keywords appear at the same level, they are evaluated in this order:

1. `forEach` / `forEachOrNull` / `repeat`
2. `select` (nested)
3. `unionAll`
4. `column`

## Naming Constraints

All names (`name` on ViewDefinition, constants, and columns) must:

- Start with a letter (A-Z, a-z)
- Contain only letters, numbers, and underscores
- Not start with an underscore

Pattern: `^[A-Za-z][A-Za-z0-9_]*$`

## ShareableViewDefinition Requirements

For portable, shareable definitions, these additional elements are required:

| Element       | Requirement                            |
| ------------- | -------------------------------------- |
| `url`         | Required - canonical identifier        |
| `name`        | Required - computer-friendly name      |
| `fhirVersion` | Required - at least one target version |
| `column.type` | Required on all columns                |

## TabularViewDefinition Constraints

For scalar/CSV-compatible output:

1. **no-collections**: `collection` must be false or absent on all columns
2. **primitives-only**: Column types must be primitive types only

Allowed primitive types: `base64Binary`, `boolean`, `canonical`, `code`, `dateTime`, `decimal`, `id`, `instant`, `integer`, `integer64`, `markdown`, `oid`, `string`, `positiveInt`, `time`, `unsignedInt`, `url`, `uuid`

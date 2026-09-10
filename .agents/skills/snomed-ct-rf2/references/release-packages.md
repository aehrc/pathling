# Release Packages and File Conventions

This document describes release types, file naming conventions, common file features, data types, and the history mechanism.

## Release types

A SNOMED CT release includes three distinct release types.

| Release type | Description                                                                                                                                              |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Full         | Contains every version of every component and reference set member ever released. Provides complete history.                                             |
| Snapshot     | Contains only the most recent version of every component and reference set member as at the release date.                                                |
| Delta        | Contains only rows representing component versions and reference set member versions created since the previous release date. Cannot be used standalone. |

**Practical notes**:

- Full releases allow access to all historical versions and support change management.
- Snapshot releases optimise access to the current version but do not provide history.
- Delta releases identify new and changed components and can be used to update a previous Full release to the new Full release.
- Delta and Snapshot views can be generated from a Full release.
- Organizations maintaining SNOMED CT extensions are required to provide the Full release type.
- SNOMED International no longer distributes Delta files directly in release packages. A Delta Generation Tool is available for creating custom deltas between two release dates.

## File naming convention

Pattern:

```
[FileType]_[ContentType]_[ContentSubType]_[CountryNamespace]_[VersionDate].[Extension]
```

### FileType element

Composed of up to three sub-elements: Status + Type + Format.

| Sub-element | Values    | Description                                |
| ----------- | --------- | ------------------------------------------ |
| Status      | `<blank>` | General release file                       |
| Status      | `x`       | Provisional (alpha/beta)                   |
| Status      | `z`       | Archival or unsupported                    |
| Type        | `sct`     | Terminology data file                      |
| Type        | `der`     | Derivative work data file (reference sets) |
| Type        | `doc`     | Documentation                              |
| Type        | `res`     | Implementation resource data file          |
| Type        | `tls`     | Implementation resource tool               |
| Format      | `1`       | Release Format 1 (deprecated)              |
| Format      | `2`       | Release Format 2                           |
| Format      | `<blank>` | Not specific to a release version          |

Examples:

- `sct2` - RF2 terminology data file.
- `der2` - RF2 derivative/reference set file.
- `xder2` - Provisional RF2 derivative file.

### ContentType element

Mandatory. Describes the content and purpose of the file. 2-48 alphanumeric characters in camel case.

**For `sct` FileType**:

| Value              | Usage                                               |
| ------------------ | --------------------------------------------------- |
| Concept            | Concept file specification                          |
| Relationship       | Inferred/normal form relationships                  |
| StatedRelationship | Stated relationships (likely to be phased out)      |
| Description        | Synonyms and FSNs (max 255 chars)                   |
| TextDefinition     | Definitions (max 4096 chars)                        |
| Identifier         | Identifier file specification                       |
| sRefset            | OWL Expression reference set (single string column) |

**For `der` FileType**:

| Value     | Usage                              |
| --------- | ---------------------------------- |
| Refset    | Simple reference set               |
| cRefset   | One additional component column    |
| ciRefset  | One component + one integer column |
| sRefset   | One additional string column       |
| ssRefset  | Two additional string columns      |
| sssRefset | Three additional string columns    |

Pattern letters for `der` content types:

- `c` = Component identifier (SCTID)
- `i` = Signed integer
- `s` = UTF-8 text string

### ContentSubType element

Mandatory. Provides additional information including release type and language/dialect.

Sub-elements:

- **Summary** (optional) - Short camel case summary of file usage.
- **ReleaseType** - `Full`, `Snapshot`, or `Delta`.
- **LanguageCode** (optional) - ISO 639-1 language code, optionally with dialect (e.g. `en`, `en-US`, `en-GB`).

If Summary is present, ReleaseType follows immediately without separator. If LanguageCode is present, it is the final sub-element, separated from the preceding sub-element by a hyphen.

Examples:

- `AttributeValueSnapshot` - Summary=AttributeValue, ReleaseType=Snapshot.
- `Snapshot-en` - ReleaseType=Snapshot, Language=English.
- `LanguageSnapshot-en` - Summary=Language, ReleaseType=Snapshot, Language=English.

### CountryNamespace element

Mandatory. Identifies the organization responsible for the file.

| Sub-element | Values                 | Description                           |
| ----------- | ---------------------- | ------------------------------------- |
| CountryCode | `INT`                  | SNOMED International                  |
| CountryCode | `AA` to `ZZ`           | ISO-3166 alpha-2 country code for NRC |
| CountryCode | `<blank>`              | Extension provider that is not an NRC |
| NamespaceId | `0000000` to `9999999` | 7-digit namespace identifier          |
| NamespaceId | `<blank>`              | No namespace restriction indicated    |

At least one of CountryCode or NamespaceId must be present.

### VersionDate element

Mandatory. Format: `YYYYMMDD` (ISO-8601).

For data files and Current documentation, this should match the SNOMED CT version date.

### File extension

| FileType | Extension              | Description             |
| -------- | ---------------------- | ----------------------- |
| sct, der | `txt`                  | Plain text UTF-8        |
| doc      | `pdf`                  | Default document format |
| res      | `txt`, `zip`, or other | Plain text or archive   |
| tls      | any                    | Tool-dependent          |

### Naming examples

- `sct2_Concept_Snapshot_INT_20180131.txt`
- `der2_cRefset_AttributeValueSnapshot_INT_20180131.txt`
- `sct2_Description_Snapshot-en_INT_20180131.txt`
- `der2_ciRefset_LanguageSnapshot-en_INT_20180131.txt`

## Common features of all release files

### General structure

- UTF-8 encoded, tab-delimited text files.
- Each line, including the final line, ends with CR (hex 0D) followed by LF (hex 0A).
- First line contains column names in lowerCamelCase.
- Field names use lowerCamelCase: `id`, `term`, `typeId`, `relationshipGroup`, `definitionStatusId`.

### Data types

| Data type | Description                                                   |
| --------- | ------------------------------------------------------------- |
| SCTID     | SNOMED CT identifier, 6-18 decimal digits. See sctid.md.      |
| UUID      | 128-bit identifier in standard hyphenated hexadecimal format. |
| Integer   | 32-bit signed integer.                                        |
| String    | UTF-8 text of specified length.                               |
| Boolean   | Integer value: `1` = true, `0` = false.                       |
| Time      | Date: `YYYYMMDD`. DateTime: `YYYYMMDDThhmmssZ` (UTC).         |

### Concept enumerations

Concept enumerations are sets of SNOMED CT concept identifiers used to represent values for properties. They allow human-readable meanings to be accessed via descriptions. The concepts used are typically subtype children of metadata hierarchy concepts.

Common concept enumeration roots:

- `900000000000443000` - Module (values for moduleId)
- `900000000000444006` - Definition status (values for definitionStatusId)
- `900000000000446008` - Description type (values for description typeId)
- `900000000000447004` - Case significance (values for caseSignificanceId)
- `900000000000449001` - Characteristic type (values for characteristicTypeId)
- `900000000000450001` - Modifier (values for modifierId)
- `900000000000453004` - Identifier scheme (values for identifierSchemeId)

### Fields present in all release files

The first four columns are present in every release file:

| Field         | Data type     | Purpose                                                                                | Mutable | Part of PK                      |
| ------------- | ------------- | -------------------------------------------------------------------------------------- | ------- | ------------------------------- |
| id            | SCTID or UUID | Uniquely identifies the component or reference set member.                             | NO      | YES (Full/Snapshot)             |
| effectiveTime | Time          | Inclusive date this row's state became current.                                        | YES     | YES (Full), Optional (Snapshot) |
| active        | Boolean       | Whether the component was active from effectiveTime.                                   | YES     | NO                              |
| moduleId      | SCTID         | Module this component is maintained in. Descendant of `900000000000443000 \|Module\|`. | YES     | NO                              |

- Components use `id` = SCTID.
- Reference set members use `id` = UUID.

### Meaning of the active field

| Component type | Active value | Behaviour                                                                                                                                       |
| -------------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Concept        | True         | Concept intended for active use. Active descriptions and relationships are valid.                                                               |
| Concept        | False        | Concept not intended for active use (concept permanence). Valid descriptions remain. Cannot be source/destination/type of active relationships. |
| Description    | True         | Term is a valid description of the concept. May refer to inactive concept.                                                                      |
| Description    | False        | Term is not valid and should not be associated with the concept.                                                                                |
| Relationship   | True         | Valid association between source and destination. Inactive concepts cannot be source/destination/type.                                          |
| Relationship   | False        | Relationship is not valid. May be redundant/inferable. May refer to active or inactive components.                                              |
| Refset member  | True         | Member contains valid information for the referenced component. Referenced component may be active or inactive.                                 |
| Refset member  | False        | Member is not valid and should be ignored.                                                                                                      |

### History mechanism

RF2 uses an append-only "log style" data model for full traceability. Once released, a row is never changed.

To change a component:

1. Append a new row with the same `id`.
2. Set `active` = `1` and `effectiveTime` to the release date.
3. Update the mutable fields as needed.

To inactivate a component:

1. Append a new row with the same `id`.
2. Set `active` = `0` and `effectiveTime` to the release date.
3. Copy other fields from the final valid version (value is ignored when inactive).

If an immutable field must change:

1. Inactivate the existing component.
2. Create a new component with a new `id`.

Key notes:

- Previously written records are never amended.
- Only the most recently amended record before a release is appended; intermediate edits during authoring are not preserved.
- The `effectiveTime` must be the release date (or earlier). Pre-releases may use a future scheduled release date.
- Future activation dates should be represented using reference sets, not future `effectiveTime` values.

### Module identification

Each component is managed in a module identified by its `moduleId`.

- A module is a group of components and/or reference set members managed, maintained, and distributed as a unit.
- All components in the same module share the same `moduleId`.
- A component is part of only one module at any given time.
- Components may be moved between modules by creating a revised version with a different `moduleId`.
- Extension providers must create at least one module and apply its `moduleId` to all components in their extension.
- All modules except `900000000000012004 \|SNOMED CT model component module\|` have dependencies on other modules specified in the Module Dependency Reference Set.

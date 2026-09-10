# Component File Specifications

This document details the field specifications for each SNOMED CT RF2 component file.

## Concept file

File pattern: `sct2_Concept_*.txt`

| Field              | Data type | Purpose                                                                         | Mutable | Part of PK                      |
| ------------------ | --------- | ------------------------------------------------------------------------------- | ------- | ------------------------------- |
| id                 | SCTID     | Uniquely identifies the concept.                                                | NO      | YES (Full/Snapshot)             |
| effectiveTime      | Time      | Inclusive date this version became current.                                     | YES     | YES (Full), Optional (Snapshot) |
| active             | Boolean   | Whether the concept was active from effectiveTime.                              | YES     | NO                              |
| moduleId           | SCTID     | Module maintaining this concept. Descendant of `900000000000443000 \|Module\|`. | YES     | NO                              |
| definitionStatusId | SCTID     | Primitive or defined. Descendant of `900000000000444006 \|Definition status\|`. | YES     | NO                              |

**Rules**:

- Only one concept record with the same `id` is current at any time (most recent `effectiveTime`).
- When a concept is inactivated, a new row is added with `active` = `0` and `definitionStatusId` set to Primitive.
- All relationships with the concept as `sourceId` must also be inactivated.
- Historical Association Reference Sets model associations from the inactive concept to other concepts.
- Active descriptions associated with the inactive concept are added to `900000000000490003 \|Description inactivation indicator reference set\|` with value `900000000000495008 \|Concept non-current\|`.

**Concept enumeration for definitionStatusId**:

- `900000000000073002` - Sufficiently defined by necessary conditions definition status.
- `900000000000074008` - Not sufficiently defined by necessary conditions definition status (primitive).

## Description file

File pattern: `sct2_Description_*.txt` (synonyms and FSNs, max 255 chars) or `sct2_TextDefinition_*.txt` (definitions, max 4096 chars)

| Field              | Data type | Purpose                                                                      | Mutable | Part of PK                      |
| ------------------ | --------- | ---------------------------------------------------------------------------- | ------- | ------------------------------- |
| id                 | SCTID     | Uniquely identifies the description.                                         | NO      | YES (Full/Snapshot)             |
| effectiveTime      | Time      | Inclusive date this version became current.                                  | YES     | YES (Full), Optional (Snapshot) |
| active             | Boolean   | Whether the description was active from effectiveTime.                       | YES     | NO                              |
| moduleId           | SCTID     | Module maintaining this description.                                         | YES     | NO                              |
| conceptId          | SCTID     | Concept this description applies to.                                         | NO      | NO                              |
| languageCode       | String    | ISO-639-1 two-character language code.                                       | NO      | NO                              |
| typeId             | SCTID     | Description type. Descendant of `900000000000446008 \|Description type\|`.   | NO      | NO                              |
| term               | String    | The description text (UTF-8).                                                | YES     | NO                              |
| caseSignificanceId | SCTID     | Case significance. Descendant of `900000000000447004 \|Case significance\|`. | YES     | NO                              |

**Rules**:

- `conceptId`, `languageCode`, and `typeId` are immutable. If they need to change, inactivate the description and create a new one with a new `id`.
- Each concept must have at least one active synonym (`900000000000013009`) and at least one active FSN (`900000000000003001`).
- The `term` field has an overall maximum of 32Kb, but specific description types have configurable maxima defined in the Description Format Reference Set (`900000000000538005`).
- Control characters (TAB, CR, LF) must not appear in plain text or limited HTML format terms.

**Concept enumeration for typeId**:

- `900000000000003001` - Fully specified name.
- `900000000000013009` - Synonym.
- `900000000000550004` - Definition.

**Concept enumeration for caseSignificanceId**:

- `900000000000017005` - Entire term case sensitive.
- `900000000000020002` - Only initial character case insensitive.
- `900000000000448009` - Entire term case insensitive.

## Relationship file

File pattern: `sct2_Relationship_*.txt` (inferred/normal form) or `sct2_StatedRelationship_*.txt` (stated view - likely to be phased out in favor of OWL axioms)

| Field                | Data type | Purpose                                                                                                  | Mutable | Part of PK                      |
| -------------------- | --------- | -------------------------------------------------------------------------------------------------------- | ------- | ------------------------------- |
| id                   | SCTID     | Uniquely identifies the relationship.                                                                    | NO      | YES (Full/Snapshot)             |
| effectiveTime        | Time      | Inclusive date this version became current.                                                              | YES     | YES (Full), Optional (Snapshot) |
| active               | Boolean   | Whether the relationship was active from effectiveTime.                                                  | YES     | NO                              |
| moduleId             | SCTID     | Module maintaining this relationship.                                                                    | YES     | NO                              |
| sourceId             | SCTID     | Source concept (the concept being defined).                                                              | NO      | NO                              |
| destinationId        | SCTID     | Destination concept (the value of the attribute).                                                        | NO      | NO                              |
| relationshipGroup    | Integer   | Groups logically associated relationships. `0` = ungrouped.                                              | YES     | NO                              |
| typeId               | SCTID     | Relationship type. Must be `116680003 \|Is a\|` or a subtype of `410662002 \|Concept model attribute\|`. | NO      | NO                              |
| characteristicTypeId | SCTID     | Defining, qualifying, etc. Descendant of `900000000000449001 \|Characteristic type\|`.                   | YES     | NO                              |
| modifierId           | SCTID     | DL restriction type. Descendant of `900000000000450001 \|Modifier\|`.                                    | YES     | NO                              |

**Rules**:

- `sourceId`, `destinationId`, `typeId` are immutable. If they need to change, inactivate and create a new relationship with a new `id`.
- `relationshipGroup` is an unsigned integer, not limited to single digits, not guaranteed sequential, and not unique across concepts.
- All relationships with the same `sourceId` and non-zero `relationshipGroup` are logically grouped.
- Since 2018, all released relationships have `characteristicTypeId` = `900000000000011006 \|Inferred relationship\|` because stated views are represented using OWL axioms.
- The only current value for `modifierId` is `900000000000451002 \|Some\|`.

**Concept enumeration for characteristicTypeId**:

- `900000000000006009` - Defining relationship (supertype of inferred and stated; not applied to released relationships).
- `900000000000011006` - Inferred relationship (used for all released relationships since 2018).
- `900000000000010007` - Stated relationship (no longer used since 2018).
- `900000000000225001` - Qualifying relationship (not used since 2012).
- `900000000000227009` - Additional relationship (not used since 2012).

**Concept enumeration for modifierId**:

- `900000000000451002` - Some (the only value currently used).

## Concrete Value file

File pattern: `sct2_RelationshipConcreteValues_*.txt`

This file is structurally identical to the Relationship file except `destinationId` is replaced with a `value` column for concrete data types (integer, decimal, string).

| Field                | Data type | Purpose                                                                                                   | Mutable | Part of PK                      |
| -------------------- | --------- | --------------------------------------------------------------------------------------------------------- | ------- | ------------------------------- |
| id                   | SCTID     | Uniquely identifies the relationship.                                                                     | NO      | YES (Full/Snapshot)             |
| effectiveTime        | Time      | Inclusive date this version became current.                                                               | YES     | YES (Full), Optional (Snapshot) |
| active               | Boolean   | Whether the relationship was active from effectiveTime.                                                   | YES     | NO                              |
| moduleId             | SCTID     | Module maintaining this relationship.                                                                     | YES     | NO                              |
| sourceId             | SCTID     | Source concept.                                                                                           | NO      | NO                              |
| value                | String    | Concrete value. Numbers prefixed with `#`. Strings surrounded with `"`, internal quotes escaped with `\`. | NO      | NO                              |
| relationshipGroup    | Integer   | Groups logically associated relationships.                                                                | YES     | NO                              |
| typeId               | SCTID     | Must be a subtype of `762706009 \|Concept model data attribute\|`.                                        | NO      | NO                              |
| characteristicTypeId | SCTID     | Descendant of `900000000000449001 \|Characteristic type\|`.                                               | YES     | NO                              |
| modifierId           | SCTID     | Descendant of `900000000000450001 \|Modifier\|`.                                                          | YES     | NO                              |

**Rules**:

- `value` is restricted to 4096 characters.
- `sourceId`, `typeId`, `value`, `relationshipGroup`, `characteristicTypeId`, and `modifierId` are immutable between rows with the same `id`.

## Identifier file

File pattern: `sct2_Identifier_*.txt`

This file associates alternative identifiers from various schemes with SNOMED CT components. It is not currently used in the International Release.

| Field                 | Data type | Purpose                                                                   | Mutable | Part of PK                      |
| --------------------- | --------- | ------------------------------------------------------------------------- | ------- | ------------------------------- |
| alternateIdentifier   | String    | Alternative identifier in its native scheme.                              | NO      | YES (Full/Snapshot)             |
| effectiveTime         | Time      | Inclusive date the association became current.                            | YES     | YES (Full), Optional (Snapshot) |
| active                | Boolean   | Whether the association was active from effectiveTime.                    | YES     | NO                              |
| moduleId              | SCTID     | Module that created the association.                                      | YES     | NO                              |
| identifierSchemeId    | SCTID     | Scheme concept. Descendant of `900000000000453004 \|Identifier scheme\|`. | NO      | YES (Full/Snapshot)             |
| referencedComponentId | SCTID     | SNOMED CT component associated with the alternate identifier.             | YES     | NO                              |

**Rules**:

- At any point in time, an `alternateIdentifier` within a particular scheme is associated with one and only one SNOMED CT component.
- A SNOMED CT component may have zero or more `alternateIdentifier` values within a single scheme.
- The current record is the one with the most recent `effectiveTime` for the same `identifierSchemeId` and `alternateIdentifier`.

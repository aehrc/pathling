# Reference Set Specifications

This document details SNOMED CT RF2 reference set file structures and types.

## Basic reference set member format

All reference set files follow this base structure. Additional columns are appended for specific reference set types.

File pattern: `der2_<pattern>Refset_*.txt`

| Field                 | Data type | Purpose                                                                       | Mutable | Part of PK                      |
| --------------------- | --------- | ----------------------------------------------------------------------------- | ------- | ------------------------------- |
| id                    | UUID      | Uniquely identifies this reference set member.                                | NO      | YES (Full/Snapshot)             |
| effectiveTime         | Time      | Inclusive date this version became current.                                   | YES     | YES (Full), Optional (Snapshot) |
| active                | Boolean   | Whether the member was active from effectiveTime.                             | YES     | NO                              |
| moduleId              | SCTID     | Module containing this member. Descendant of `900000000000443000 \|Module\|`. | YES     | NO                              |
| refsetId              | SCTID     | Identifies the reference set this member belongs to.                          | NO      | NO                              |
| referencedComponentId | SCTID     | SNOMED CT component included in the reference set.                            | NO      | NO                              |

**Rules**:

- `refsetId` and `referencedComponentId` are immutable. If they need to change, inactivate the member and create a new one with a new `id`.
- A component may belong to any number of reference sets.
- A component may be referenced by more than one member of the same reference set.
- Each reference set is identified by a concept in the metadata hierarchy (its `refsetId`).

## Extending the basic format

Additional columns are appended after `referencedComponentId`. Three general data types are supported:

- **Component** (`c`) - SCTID referring to a concept, description, or relationship.
- **Integer** (`i`) - Signed 32-bit integer.
- **String** (`s`) - UTF-8 text string.

The filename pattern prefix indicates the additional column types. Examples:

- `cRefset` - one additional component column (e.g. Attribute Value, Language, Association).
- `ciRefset` - one component + one integer column (e.g. Ordered Association).
- `sRefset` - one additional string column (e.g. Simple Map, Annotation).
- `ssRefset` - two additional string columns (e.g. Complex Map).
- `sssRefset` - three additional string columns.

Finer-grained interpretation of data types can be specified using the Reference Set Descriptor (`900000000000456007`).

## Language reference set

File pattern: `der2_cRefset_Language*.txt`

Purpose: indicates which descriptions are acceptable or preferred in a particular language or dialect.

Additional fields after `referencedComponentId`:

| Field           | Data type | Purpose                                                                    | Mutable |
| --------------- | --------- | -------------------------------------------------------------------------- | ------- |
| acceptabilityId | SCTID     | `900000000000548007 \|Preferred\|` or `900000000000549004 \|Acceptable\|`. | YES     |

**Rules**:

- No more than one description of a specific type associated with a single concept may have `acceptabilityId` = Preferred.
- Every active concept should have one preferred synonym in each language reference set.
- Descriptions not referenced by an active row are regarded as unacceptable.
- There is no "unacceptable" value; unreferenced descriptions are implicitly unacceptable.

Metadata hierarchy:

- `900000000000506000 \|Language type\|`
    - `900000000000507009 \|English\|`
        - `900000000000508004 \|GB English\|`
        - `900000000000509007 \|US English\|`

## Map reference sets

### Simple Map from SNOMED CT

File pattern: `der2_sRefset_SimpleMap*.txt`

Maps a SNOMED CT component to a code in another code system.

Additional fields:

| Field     | Data type | Purpose                         |
| --------- | --------- | ------------------------------- |
| mapTarget | String    | Code in the target code system. |

### Simple Map to SNOMED CT

File pattern: `der2_sRefset_SimpleMapTo*.txt`

Maps a code from another code system to a SNOMED CT component.

Additional fields:

| Field     | Data type | Purpose                     |
| --------- | --------- | --------------------------- |
| mapTarget | SCTID     | Target SNOMED CT component. |

### Complex and Extended Map from SNOMED CT

File pattern: `der2_ssRefset_ComplexMap*.txt` or `der2_sssRefset_ExtendedMap*.txt`

Supports maps that require rules, advice, and multiple mapping targets.

Additional fields for Complex Map:

| Field         | Data type | Purpose                                                            |
| ------------- | --------- | ------------------------------------------------------------------ |
| mapGroup      | Integer   | Groups alternatives for a single source concept.                   |
| mapPriority   | Integer   | Priority within the map group.                                     |
| mapRule       | String    | Machine-processable rule for map applicability.                    |
| mapAdvice     | String    | Human-readable advice.                                             |
| mapTarget     | String    | Target code.                                                       |
| correlationId | SCTID     | Correlation between source and target (optional for Extended Map). |

Extended Map adds:

| Field         | Data type | Purpose                     |
| ------------- | --------- | --------------------------- |
| mapCategoryId | SCTID     | Categorises the map target. |

### Code to Expression Reference Set

File pattern: `der2_sRefset_CodeToExpression*.txt`

Maps a code from another code system to a SNOMED CT postcoordinated expression.

Additional fields:

| Field         | Data type | Purpose                                |
| ------------- | --------- | -------------------------------------- |
| mapTarget     | String    | SNOMED CT expression.                  |
| correlationId | SCTID     | Correlation between source and target. |
| mapRule       | String    | Rule for when the map applies.         |
| mapAdvice     | String    | Human-readable advice.                 |

## Module Dependency Reference Set

File pattern: `der2_ssRefset_ModuleDependency*.txt`

Represents dependencies between SNOMED CT release modules, accounting for versioning.

Additional fields:

| Field               | Data type | Purpose                                        | Mutable |
| ------------------- | --------- | ---------------------------------------------- | ------- |
| sourceEffectiveTime | Time      | Effective time of the dependent source module. | YES     |
| targetEffectiveTime | Time      | Effective time of the target module required.  | YES     |

**Rules**:

- `moduleId` in this reference set is immutable because it represents the source module declaring the dependency.
- `referencedComponentId` is the target module.
- Dependencies are between specific module versions, not just modules.
- Dependencies are not transitive and must be explicitly stated.
- Cyclic dependencies are not allowed.
- A module dependency should only be inactivated if it is erroneous.
- At the point of release, if any component in a module has changed, a new row must be added for each dependency of that module.

## Metadata reference sets

### Reference Set Descriptor

File pattern: `der2_ciRefset_ReferenceSetDescriptor*.txt`

Defines the structure of reference sets by specifying the data type and purpose of each additional column.

Additional fields:

| Field                | Data type | Purpose                                                 |
| -------------------- | --------- | ------------------------------------------------------- |
| attributeDescription | SCTID     | Concept describing the purpose of the attribute.        |
| attributeType        | SCTID     | Concept indicating the data type of the attribute.      |
| attributeOrder       | Integer   | Zero-based order of the attribute in the reference set. |

### Description Format Reference Set

File pattern: `der2_cRefset_DescriptionFormat*.txt`

Specifies the format and maximum length of descriptions for each description type.

Additional fields:

| Field             | Data type | Purpose                                                |
| ----------------- | --------- | ------------------------------------------------------ |
| descriptionFormat | SCTID     | Format concept (e.g. plain text, limited HTML, XHTML). |
| descriptionLength | Integer   | Maximum length of the term for this description type.  |

### MRCM Reference Sets

Machine Readable Concept Model (MRCM) reference sets define domain constraints, attribute domains, attribute ranges, and module scope:

- **MRCM Domain** (`der2_cissccRefset_MRCMDomain*.txt`)
- **MRCM Attribute Domain** (`der2_cissccRefset_MRCMAttributeDomain*.txt`)
- **MRCM Attribute Range** (`der2_ssccRefset_MRCMAttributeRange*.txt`)
- **MRCM Module Scope** (`der2_cRefset_MRCMModuleScope*.txt`)

## Component and Member Annotation Reference Sets

### Component Annotation String Value Reference Set

File pattern: `der2_cRefset_ComponentAnnotationStringValue*.txt`

Attaches string annotations to SNOMED CT components.

Additional fields:

| Field      | Data type | Purpose          |
| ---------- | --------- | ---------------- |
| annotation | String    | Annotation text. |

### Member Annotation String Value Reference Set

File pattern: `der2_cRefset_MemberAnnotationStringValue*.txt`

Attaches string annotations to reference set members.

Additional fields:

| Field      | Data type | Purpose          |
| ---------- | --------- | ---------------- |
| annotation | String    | Annotation text. |

## OWL Expression Reference Set

File pattern: `sct2_sRefset_OWLExpression*.txt`

Contains stated concept definitions represented as OWL axioms and additional OWL ontology information.

Additional fields:

| Field         | Data type | Purpose                                                    |
| ------------- | --------- | ---------------------------------------------------------- |
| owlExpression | String    | OWL axiom or ontology annotation in OWL Functional Syntax. |

**Note**: Since 2018, stated views of concept definitions are represented using OWL axioms in this reference set rather than in the Stated Relationship file.

## Query Specification Reference Set

File pattern: `der2_sRefset_QuerySpecification*.txt`

Specifies queries (e.g. ECL expressions) that define the intensional content of a reference set.

Additional fields:

| Field | Data type | Purpose           |
| ----- | --------- | ----------------- |
| query | String    | Query expression. |

## Association Reference Sets

### Historical Association Reference Sets

These are `cRefset` pattern reference sets that associate inactive components with active replacements or related concepts.

Common historical association reference sets:

- `900000000000526001` - SAME AS (replaced by)
- `900000000000527005` - REPLACED BY
- `900000000000528000` - WAS A
- `900000000000529008` - SIMILAR TO
- `900000000000530003` - POSSIBLY REPLACED BY
- `900000000000531004` - MOVED TO
- `900000000000532006` - MOVED FROM
- `900000000000533001` - ALTERNATIVE

### Ordered Association Reference Set

File pattern: `der2_ciRefset_OrderedAssociation*.txt`

An association reference set with an integer `order` field to specify sequence.

Additional fields:

| Field             | Data type | Purpose                              |
| ----------------- | --------- | ------------------------------------ |
| targetComponentId | SCTID     | Target component of the association. |
| order             | Integer   | Order within the sequence.           |

### Ordered Component Reference Set

File pattern: `der2_ciRefset_OrderedComponent*.txt`

A simple reference set with an integer `order` field to specify sequence.

Additional fields:

| Field | Data type | Purpose                    |
| ----- | --------- | -------------------------- |
| order | Integer   | Order within the sequence. |

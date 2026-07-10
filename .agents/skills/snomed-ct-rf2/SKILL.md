---
name: snomed-ct-rf2
description: SNOMED CT Release Format 2 (RF2). Use when working with SNOMED CT release files, parsing or generating RF2 tab-delimited files, validating SCTIDs, or understanding component files, reference sets, and release packages. Trigger keywords include SNOMED CT RF2, release file, SCTID, concept file, description file, relationship file, refset, delta, snapshot, full release, Verhoeff check-digit, language reference set, namespace.
---

# SNOMED CT RF2

This skill provides comprehensive guidance for working with SNOMED CT Release Format 2 (RF2) files, identifiers, and reference sets.

## Quick reference

- **All RF2 files**: UTF-8 encoded, tab-delimited, CRLF line endings, header row with lowerCamelCase field names.
- **Common fields in every file**: `id`, `effectiveTime`, `active`, `moduleId`.
- **Components use SCTID** (6-18 digit decimal integer); **reference set members use UUID**.
- **Release types**: Full (all versions), Snapshot (most recent version only), Delta (changes since previous release).
- **File naming**: `[FileType]_[ContentType]_[ContentSubType]_[CountryNamespace]_[VersionDate].txt`

## Core concepts

### File format fundamentals

All SNOMED CT release files share these properties:

- UTF-8 encoded plain text.
- Tab-delimited fields.
- Each line ends with carriage return (hex 0D) followed by line feed (hex 0A), including the final line.
- First line contains lowerCamelCase column names.

For data types, common fields, the `active` field semantics, history mechanism, and module identification, see [references/release-packages.md](references/release-packages.md).

### Component files

SNOMED CT has five core component file types:

1. **Concept** - `sct2_Concept_*.txt`
2. **Description** - `sct2_Description_*.txt` or `sct2_TextDefinition_*.txt`
3. **Relationship** - `sct2_Relationship_*.txt` or `sct2_StatedRelationship_*.txt`
4. **Concrete Value** - `sct2_RelationshipConcreteValues_*.txt`
5. **Identifier** - `sct2_Identifier_*.txt`

For detailed field specifications, mutability rules, primary keys, and concept enumerations for each component type, see [references/component-files.md](references/component-files.md).

### Reference sets

Reference sets extend the basic member format with additional columns:

- **Basic format**: `id` (UUID), `effectiveTime`, `active`, `moduleId`, `refsetId`, `referencedComponentId`, plus type-specific fields.
- **Language refset**: indicates acceptable/preferred terms for a dialect.
- **Map refsets**: simple map, complex map, extended map, code to expression.
- **Module Dependency refset**: declares inter-module version dependencies.
- **Metadata refsets**: reference set descriptor, description format, MRCM domains.

For reference set formats, patterns, and detailed type specifications, see [references/reference-sets.md](references/reference-sets.md).

### SNOMED CT identifiers (SCTID)

An SCTID is a 64-bit positive integer, 6-18 decimal digits, with a Verhoeff check-digit.

Structure (right to left):

1. **Check-digit** - 1 digit (rightmost).
2. **Partition identifier** - 2 digits (second and third from right).
    - `00` = Concept (international), `01` = Description (international), `02` = Relationship (international).
    - `10` = Concept (extension), `11` = Description (extension), `12` = Relationship (extension), `16` = Postcoordinated expression (extension).
3. **Namespace identifier** - 7 digits immediately left of partition (only for long format / extension IDs).
4. **Item identifier** - remaining digits left of namespace.

For check-digit computation (Verhoeff's dihedral D5), validation logic, and identifier constraints, see [references/sctid.md](references/sctid.md).

## Workflows

### Parsing an RF2 file

1. Open the file with UTF-8 encoding.
2. Split each line on tab characters.
3. The first row contains field names in lowerCamelCase.
4. Parse `effectiveTime` as `YYYYMMDD` (or `YYYYMMDDThhmmssZ` for full ISO 8601).
5. Parse `active` as Boolean (`1` = true, `0` = false).
6. For component files, `id` is an SCTID (parse as 64-bit integer or string).
7. For reference set files, `id` is a UUID (standard hyphenated hexadecimal format).

### Determining the current state of a component (from a Full release)

1. Filter rows for the target `id`.
2. Select the row with the greatest `effectiveTime` less than or equal to the point in time under consideration.
3. If `active` = `1`, the component is current and active.
4. If `active` = `0`, the component is inactive at that point in time.
5. Do not modify or discard historical rows - they are immutable.

### Validating an SCTID

1. Verify the string contains only decimal digits and has 6-18 characters.
2. Verify there are no leading zeros (unless the value itself is "0", which is invalid for SCTID anyway).
3. Apply the Verhoeff check-digit algorithm; the final check value must be zero.
4. Optionally verify the partition identifier is a known value (`00`, `01`, `02`, `10`, `11`, `12`, `16`).
5. For long-format IDs (partition `1x`), verify a 7-digit namespace is present.

For a complete implementation of the Verhoeff check, see [references/sctid.md](references/sctid.md).

### Generating a new SCTID

1. Allocate the next item identifier within your namespace.
2. Choose the appropriate partition identifier for the component type and origin (international vs extension).
3. Combine: `[itemIdentifier][namespace][partition][checkDigit]`.
4. Compute the Verhoeff check-digit for the digits excluding the check-digit position, then append it.
5. The total length must be 6-18 digits.

## Important rules

- **Immutability**: Once a row is released, it never changes. Updates are represented by appending a new row with the same `id` and a later `effectiveTime`.
- **Inactivation**: To inactivate a component, append a new row with `active` = `0`. For concepts, all active relationships with that concept as `sourceId` must also be inactivated.
- **Mutable vs immutable fields**: Some fields (e.g. `conceptId` in Description, `sourceId`/`destinationId`/`typeId` in Relationship) are immutable. If they must change, inactivate the component and create a new one with a new `id`.
- **ModuleId**: Every component and reference set member belongs to exactly one module at any point in time. Modules are represented as descendants of `900000000000443000 |Module|`.
- **Dependencies are not transitive**: If module A depends on module B, and module B depends on module C, module A must still explicitly declare a dependency on module C in the Module Dependency Reference Set.
- **Delta files**: SNOMED International no longer distributes Delta files directly; use the Delta Generation Tool if needed.

## Reference files

- [references/component-files.md](references/component-files.md) - Detailed specifications for Concept, Description, Relationship, Concrete Value, and Identifier files.
- [references/reference-sets.md](references/reference-sets.md) - Reference set member format, language refsets, map refsets, module dependency, and metadata refsets.
- [references/sctid.md](references/sctid.md) - SCTID structure, partition identifiers, namespace identifiers, and Verhoeff check-digit algorithm.
- [references/release-packages.md](references/release-packages.md) - Release types, file naming conventions, common fields, data types, history mechanism, and module identification.

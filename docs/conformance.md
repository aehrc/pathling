# Conformance of FHIR data

A framework for talking about how far FHIR data conforms, and about what a component may promise when it
does not. It applies to every format Pathling reads: FHIR JSON, FHIR XML, and Parquet or Delta in the
[Parquet on FHIR](../specs/001-parquet-on-fhir/spec.md) layout.

This document defines terms. It does not decide behaviour. What ingest and the engine actually do with
non-conformant data is decided by the specifications, listed under [Where behaviour is
decided](#where-behaviour-is-decided).

## Conformance levels

Non-conformant content is classified by the **first** of these levels it fails. Each level assumes that the
levels before it hold. Content is **conformant at level _n_** when it passes levels 1 to _n_.

| Level               | Rule                                                             | Needs the definitions |
| ------------------- | ---------------------------------------------------------------- | --------------------- |
| 1. Container format | The file is valid in its own format.                             | No                    |
| 2. FHIR encoding    | FHIR's representation rules for the format.                      | No                    |
| 3. Structure        | The names, types and cardinality the definitions give.           | Yes                   |
| 4. Value            | The lexical space of each value's datatype.                      | Yes                   |
| 5. Valid FHIR       | Minimum cardinality, bindings, invariants, profiles, references. | Yes, and terminology  |

The levels describe which rule content breaks. They say nothing about what to do with it. That is a
separate question, covered in [Responses to non-conformance](#responses-to-non-conformance).

**Level 5 is not part of conformance in this framework.** Data that breaks only level 5 is called
conformant. Checking level 5 is a validator's concern.

### By format

| Level | JSON                                                                                            | XML                                                                          | Parquet and Delta                                                                                                                                                                                                                                                                                                |
| ----- | ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1     | Well-formed JSON, one object per line.                                                          | Well-formed XML, one root element, UTF-8.                                    | A readable Parquet or Delta file.                                                                                                                                                                                                                                                                                |
| 2     | The rules of [FHIR JSON](https://hl7.org/fhir/R4/json.html), applied directly.                  | The rules of [FHIR XML](https://hl7.org/fhir/R4/xml.html), applied directly. | The same rules, applied to the row's [JSON projection](#the-json-projection).                                                                                                                                                                                                                                    |
| 3     | Per value: known names, the JSON type the definition gives, array or singular, one choice type. | As JSON.                                                                     | Per dataset: the schema is the layout the definitions derive. Fields whose names start with `__` are layout, not content, and any producer may write them ([decision 135](../specs/001-parquet-on-fhir/evidence/decisions.md#135-an-annotation-is-defined-once-in-fhir-schema-for-the-engine-and-ingest-alike)). |
| 4     | The datatype's regex, calendar and range.                                                       | As JSON.                                                                     | The same, on the stored value.                                                                                                                                                                                                                                                                                   |
| 5     | Out of scope.                                                                                   | Out of scope.                                                                | Out of scope.                                                                                                                                                                                                                                                                                                    |

Level 2 is defined by reference to FHIR's own rules rather than restated, so it follows the FHIR version and
needs nothing from the definitions. That is why it also applies to content the definitions do not describe:
an undescribed element can still break level 2 by being `{}`.

The rules of FHIR XML that matter most in practice:

- FHIR elements are always in the namespace `http://hl7.org/fhir`, and the only other namespace that occurs is
  XHTML, in narratives.
- Exchanged content does not specify the schema or contain the schema instance namespace (no `xsi:`).
- There are no DTD references, and the encoding is UTF-8.
- Values appear only in `value` attributes, and attributes are never empty. An element has a value attribute,
  child elements of its type, or one or more extensions.
- Comments and processing instructions are not part of a resource's content.

### Examples

| Level | JSON                                                                                                                                                                                                                                                  | XML                                                                                        | Parquet                                                                      |
| ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| 1     | `{"resourceType": "Patient", "id": "a"` with no closing brace                                                                                                                                                                                         | text after the root element                                                                | a truncated file                                                             |
| 2     | `"active": null`; `"name": []`; `"maritalStatus": {}`; `"family": ""`; a duplicate key; `_given` with more groups than `given` has values; no `resourceType`                                                                                          | `<active value=""/>`; a DTD; a root outside the FHIR namespace; text inside a FHIR element | a `null` where FHIR JSON would forbid it, seen through the projection        |
| 3     | `"family": ["A", "B"]` (an array for a singular element); `"name": {"family": "Smith"}` (an object for an array); `"multipleBirthInteger": "5"` (a string for an integer); `"foo": 1` (an unknown element); `valueString` and `valueInteger` together | `<foo value="1"/>` (an unknown element)                                                    | a `birthDate` column of type `int`; a declared field missing from the schema |
| 4     | `"birthDate": "1980-02-30"`; `"birthDate": "1980-13-45"`; `"valueInteger": 99999999999`; a `positiveInt` of `0`; a `code` of `" male"`; an `id` of 70 characters                                                                                      | `<birthDate value="abc"/>`; `<valueDecimal value="1.2.3"/>`                                | the stored text `"1980-13-45"` in a `date` column                            |
| 5     | `"gender": "xyz"` (outside a required binding); an `Observation` with no `status`; a broken invariant                                                                                                                                                 | as JSON                                                                                    | as JSON                                                                      |

Two cases are easy to misplace:

- **An empty string is level 2**, for every datatype, because FHIR JSON forbids it outright. It is a length
  check, not a regex. A whitespace-only string such as `"   "` passes level 2, and only its datatype's regex
  can reject it, at level 4.
- **A wrong JSON type is level 3 even when the text would convert.** `"5"` for an integer breaks level 3,
  though the conversion to `5` loses nothing. XML has no JSON types, so the same value in XML,
  `<multipleBirthInteger value="5"/>`, conforms at level 3, and `<multipleBirthInteger value="abc"/>` fails at
  level 4.

### Datatype and element constraints

The line between levels 4 and 5 is the line between the **datatype** and the **element**.

- **Datatype constraints** (level 4) are those the FHIR datatype defines: its regex, its range and, for
  temporal types, the calendar. Each value can be checked on its own, without context.
- **Element constraints** (level 5) are those a resource or a profile adds to an element: terminology
  bindings, minimum cardinality, invariants, profiles and reference resolution. `Patient.gender` being one of
  `male`, `female`, `other` or `unknown` is an element constraint, though it looks like a value check.

### Three kinds of primitive

What it costs to check level 4, and what depends on it, differs by datatype. The primitives fall into three
groups by how they are stored.

| Kind        | Datatypes                                                                             | Stored as                                                  | What a value outside the lexical space means                                               |
| ----------- | ------------------------------------------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Native      | `boolean`, `integer`, `positiveInt`, `unsignedInt`, `base64Binary`                    | A Spark type of its own                                    | It cannot be stored as itself.                                                             |
| Parsed text | `date`, `dateTime`, `instant`, `time`, `decimal`                                      | Its text, with [annotations](../CONTEXT.md) parsed from it | The annotations, and any operation that needs the value's System type, cannot be computed. |
| Opaque text | `string`, `markdown`, `code`, `id`, `uri`, `url`, `canonical`, `oid`, `uuid`, `xhtml` | Its text                                                   | Nothing in storage or the engine depends on its form.                                      |

For parsed text, a regex is not enough: `1980-02-30` matches the `date` regex but is not a date, so the check
is the regex and a successful parse. For opaque text, a check would be a regex and nothing more, since nothing
downstream reads the form.

## The JSON projection

A Parquet on FHIR row conforms at level 2 when its mapping to JSON is FHIR JSON that conforms at level 2. The
mapping needs three rules:

- a field whose name starts with `__` is layout, not content, and is left out;
- a null field in a structure is omitted, and a null in an array is written as `null`;
- `resourceType` is stored at the root, so that the row carries it.

Level 2 needs no definitions, so these rules decide it on their own. The JSON type of a value, such as a
decimal stored as text but written as a number, is a level 3 question and comes from the definitions. The
rules are proposed to the Parquet on FHIR specification ([ticket
35](../specs/001-parquet-on-fhir/issues/35-pof-spec-json-projection.md)).

## Properties a component may promise

These are the properties used to state and compare behaviours. A specification says which of them it promises,
and for which input. Each has a name, so that it can be referred to.

### No loss on conformant input

For input conformant at levels 1 to 4, nothing is lost and every answer is correct, apart from a **closed list
of exceptions**. An exception is conformant content that a component does not keep by design, such as
`contained` resources in the Parquet on FHIR layout. The list is closed: any other loss from conformant input
is a bug. The current list is FR-002 of [spec 002](../specs/002-non-conformant-as-empty/spec.md).

### Loss is distinct from change

A value whose encoding differs from the definition, but whose content survives, has been **changed**, not
lost. `"family": 123` stored as `"123"` is a change. The same route is a **loss** when the exact text is not
kept: `"family": 1.50` passed through a double becomes `"1.5"`, and the trailing zero is gone. A property or a
report that counts losses must not count changes, and the reverse.

### No invisible loss

Data is never lost without the loss being visible. Visible means **attributable** (it names the resource, the
path and the value), **machine-readable** (a pipeline can act on it without parsing logs) and **delivered
without being asked for** (at least a summary reaches the caller). A behaviour that accepts silent loss must
say so, as Spark's non-ANSI casts do when they turn invalid text into null.

### Fail early

Where a component fails on non-conformant input, it fails **before commit** (no partial output survives),
**located** (naming the resource, the path and the value) and, in a validation run, reporting **every** error
rather than the first.

### Same input, same outcome

The outcome for a value depends only on that value, not on the format it arrived in, on the other rows in its
file, partition or batch, or on the run. Schema inference breaks this property: whether `100` is stored as `100`
or `100.0`, or whether a mismatch is visible at all, can depend on the rest of the batch.

### One grammar

Every component that parses a datatype's lexical form, ingest and the engine alike, uses the same code for it.
A value ingest accepts, the engine can read, and the reverse.

### The round-trip invariant

For data conformant to a definition set, JSON → Parquet on FHIR → JSON, and the same route through XML, is an
identity up to JSON equivalence: key order and formatting are ignored. The known gaps are the exceptions above.
Content the definitions do not describe is outside the invariant, because no FHIR mechanism carries a type for
it.

## Responses to non-conformance

The vocabulary for what a component does with a piece of non-conformant content.

**Outcomes for one piece of content:**

- **fail**: the load or the query stops with an error;
- **convert**: the content is changed to fit the definition, without changing its value;
- **absent**: the content is treated as though it were not in the input (also called **drop**);
- **keep**: the content is stored or used as received.

**Conversions** come in two strengths:

- **widen**: conversions that cannot fail for any value: a number to a string-family type keeping the token
  text, a boolean to a string, an integer to a decimal, a single value to a one-item array;
- **narrow**: conversions that depend on the value: `"5"` to an integer, `"true"` to a boolean, a one-item
  array to a single value.

No conversion changes the value itself. Trimming, case-folding, reformatting a date, flattening nested arrays
and picking the first of several values are not conversions in this sense.

**Reporting** is a separate dimension. A component either records what it changed or dropped, as
**findings**, or acts silently. Running **validation** means collecting every violation as findings without
storing or querying anything.

**Modes** are named combinations of these choices, used in earlier designs: `strict` (fail on any
non-conformance), `tolerate` (convert what converts without loss, fail on the rest), `repair` (convert, and
drop what does not convert) and `trust` (assume conformance; behaviour on anything else is undefined).

## Where behaviour is decided

- [Spec 002](../specs/002-non-conformant-as-empty/spec.md) decides what ingest and the engine do with
  non-conformant data. It replaces the modes with one behaviour.
- [Spec 001](../specs/001-parquet-on-fhir/spec.md) decides the layout and its exceptions, and records the XML
  handling decided per level.
- The evidence behind this framework, including how HAPI 8.10.0 (used by Pathling 9.9.0) and Spark's JSON
  reader treat each kind of non-conformance, is in
  [`specs/001-parquet-on-fhir/evidence/ingress-strictness/`](../specs/001-parquet-on-fhir/evidence/ingress-strictness/README.md),
  with runnable probes.

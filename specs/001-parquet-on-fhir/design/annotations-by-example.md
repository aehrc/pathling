# Annotations by example: the stored layout and the engine's forms

**Date:** 2026-11-06 · **Companion to:** [`annotations-in-the-engine.md`](annotations-in-the-engine.md) ·
**Decisions:** 140–152 in [`../evidence/decisions.md`](../evidence/decisions.md)

This document shows, on one example resource, how each annotation is stored in the Parquet on FHIR
layout and how each value is represented inside the FHIRPath engine. It illustrates the design and
decides nothing; where the two disagree, the design and the decisions govern.

## Conventions

- Each block is one stored row, or one engine element, printed as Spark prints a row in JSON. Spark
  leaves out null fields when it prints, so a field shown as `null` is shown only for clarity.
- Timestamps are `TIMESTAMP` (adjusted to UTC), stored to the microsecond (decision 138) and printed in
  ISO form. An upper bound is the last microsecond of its period, `…59.999999Z`; a value stated to the
  second or finer is exact, so its two bounds are equal.
- `DECIMAL(38,6)` values are printed with their six fractional digits, for example `72.500000`.
- A FlexiDecimal is a structure of an unscaled `DECIMAL(38,0)` and a scale: `{ value: 72500, scale: 0 }`
  is 72500, and `{ value: 18, scale: 1 }` is 1.8.
- The primitive-extensions switch (D14, decision 141) is **off**, the default, unless an example says
  otherwise.
- The values are invented for illustration.

## 1. The source resource

```json
{
  "resourceType": "Observation",
  "id": "obs1",
  "status": "final",
  "code": { "coding": [ { "system": "http://loinc.org", "code": "29463-7", "display": "Body weight" } ] },
  "effectiveDateTime": "2024-03-15T10:30+10:00",
  "_effectiveDateTime": {
    "extension": [ { "url": "http://example.org/source", "valueString": "device" } ]
  },
  "issued": "2024-03-15T10:31:12.345+10:00",
  "valueQuantity": { "value": 72.50, "unit": "kg", "system": "http://unitsofmeasure.org", "code": "kg" },
  "component": [
    { "code": { "text": "height" },
      "valueQuantity": { "value": 180, "unit": "cm", "system": "http://unitsofmeasure.org", "code": "cm" } },
    { "code": { "text": "note" } }
  ]
}
```

## 2. The stored layout

The layout is the PoF layout as the specification defines it, read through named accessors (D12,
decision 146). Where annotations live and how they are shaped are Pathling's recommendations to the
specification's owner (D4–D6, decision 149), and the engine is designed against them meanwhile.

### 2.1 The annotated row

```json
{
  "id": "obs1",
  "status": "final",
  "code": { "coding": [ { "system": "http://loinc.org", "code": "29463-7", "display": "Body weight" } ] },

  "effectiveDateTime": "2024-03-15T10:30+10:00",
  "_effectiveDateTime": {
    "id": null,
    "extension": [ { "url": "http://example.org/source", "valueString": "device" } ],
    "__start": "2024-03-15T00:30:00.000Z",
    "__end":   "2024-03-15T00:30:59.999999Z"
  },

  "issued": "2024-03-15T10:31:12.345+10:00",
  "_issued": {
    "__start": "2024-03-15T00:31:12.345Z",
    "__end":   "2024-03-15T00:31:12.345Z"
  },

  "valueQuantity": {
    "value": "72.50",
    "_value": { "__numeric": 72.500000 },
    "unit": "kg",
    "system": "http://unitsofmeasure.org",
    "code": "kg",
    "__canonical_code": "g",
    "__canonical_value_exact": { "value": 72500, "scale": 0 }
  },

  "component": [
    {
      "code": { "text": "height" },
      "valueQuantity": {
        "value": "180",
        "_value": { "__numeric": 180.000000 },
        "unit": "cm",
        "system": "http://unitsofmeasure.org",
        "code": "cm",
        "__canonical_code": "m",
        "__canonical_value_exact": { "value": 18, "scale": 1 }
      }
    },
    { "code": { "text": "note" }, "valueQuantity": null }
  ]
}
```

### 2.2 The annotation kinds

| Kind | Example field | Where it is stored | Type |
|---|---|---|---|
| date, dateTime, instant range | `_effectiveDateTime.__start`, `__end` | In the primitive's `_x`, after `id` and `extension` (D4) | `TIMESTAMP` |
| decimal number | `valueQuantity._value.__numeric` | In the decimal's own `_x` | `DECIMAL(38,6)` |
| Quantity canonical form | `valueQuantity.__canonical_code`, `__canonical_value_exact` | Flat on the Quantity's own structure (D5, D6) | `STRING`; FlexiDecimal |

1. **A primitive keeps its value as stored text.** The date keeps `"2024-03-15T10:30+10:00"`, and the
   decimal keeps `"72.50"`. The annotation sits beside it in `_x`.
2. **A date's range comes from its precision.** At minute precision the range spans a minute; an
   `instant` is at full precision, so its bounds are equal.
3. **An annotation and an extension share `_x`.** A primitive with an annotation and no extension still
   has an `_x`, as `_issued` does.
4. **A Quantity carries two kinds.** Its decimal `value` has `_value.__numeric`, and the Quantity
   itself has the canonical form. A decimal elsewhere, such as
   `RiskAssessment.prediction.probabilityDecimal`, has `_probabilityDecimal.__numeric`.
5. **The canonical form is exact and stripped of trailing zeros,** so equal quantities have byte-equal
   canonical values (D6). 72.50 kg is `72500` g; 180 cm is `1.8` m. Pathling writes no fixed-scale
   `__canonical_value` (owner, 2026-11-06).
6. **There is no canonical system field.** Only UCUM quantities and the definite calendar durations
   (`second`, `millisecond`) are canonicalised, always to UCUM base units, so the system would be the
   same in every row. A quantity in any other system has no canonical fields.
7. **Under a repeating parent the layout is the same.** Each element of `component` carries its own
   annotations.

### 2.3 A repeating primitive

The value array and the metadata array are aligned by position:

```json
"x":  [ "2024-01-01", "2024-02" ],
"_x": [ { "__start": "2024-01-01T00:00:00.000Z", "__end": "2024-01-01T23:59:59.999999Z" },
        { "__start": "2024-02-01T00:00:00.000Z", "__end": "2024-02-29T23:59:59.999999Z" } ]
```

A value without a timezone is valid and gets the range the engine computes today (decision 133);
whether that range widens by ±14 hours is a follow-up in ticket 26.

### 2.4 The unannotated row

A dataset written without annotations has none of the `__` fields, and that is valid. Each field is
decided separately (FR-023): the engine reads a field the schema carries and computes one it lacks.

```json
"valueQuantity": { "value": "72.50", "unit": "kg", "system": "http://unitsofmeasure.org", "code": "kg" }
```

## 3. The engine: FHIR values

FHIR values come from traversal of stored data.

### 3.1 Primitives: `struct(value, …)`, flat

The element holds `value` and the annotation fields of `_x` that the schema has, read by name, all at
one level (decision 152). Nothing is computed, and the raw `_x` is never carried (D2, decision 141). A
type with an annotation kind is always a struct; a type without one, with the switch off, is a bare
value.

```jsonc
// Observation.effectiveDateTime (dateTime, annotated data)
{ "value": "2024-03-15T10:30+10:00",
  "__start": "2024-03-15T00:30:00.000Z", "__end": "2024-03-15T00:30:59.999999Z" }

// The same element over unannotated data: still a struct, with no annotation fields
{ "value": "2024-03-15T10:30+10:00" }

// The same element where the schema has __start but not __end (another producer; Pathling
// writes both or neither). start(e) reads the field; end(e) is computed from value where read
{ "value": "2024-03-15T10:30+10:00", "__start": "2024-03-15T00:30:00.000Z" }

// The same element with the switch on: id and extension join as content fields
{ "value": "2024-03-15T10:30+10:00",
  "id": null,
  "extension": [ { "url": "http://example.org/source", "valueString": "device" } ],
  "__start": "2024-03-15T00:30:00.000Z", "__end": "2024-03-15T00:30:59.999999Z" }

// Observation.valueQuantity.value (decimal): the value is the stored text (decision 147)
{ "value": "72.50", "__numeric": 72.500000 }

// Patient.name.given (string), switch off: no annotation kind, so a bare value
"Anne"

// Patient.name.given, switch on
[ { "value": "Anne", "id": null, "extension": null },
  { "value": "Mary", "id": null, "extension": [ … ] } ]

// Observation.status (code), switch off: a bare value
"final"

// Observation.status (code), switch on: the same shape for integer, boolean and every
// type without an annotation kind
{ "value": "final", "id": null, "extension": null }

// Stored: _birthDate = { "__start": …, "__end": …, "__acme_hash": "9f2c" } from another producer.
// The element reads named fields only, so __acme_hash never enters it
{ "value": "1970-03", "__start": "1970-03-01T00:00:00.000Z", "__end": "1970-03-31T23:59:59.999999Z" }
```

An annotation field the schema lacks is left out of the element, never filled with null, because a null
stored annotation is read as an absent value. Over unannotated data, an accessor computes the
annotation where an operation reads it.

### 3.2 Complex types: the stored structure as it is

A complex element keeps the fitted shape of the data it came from (FR-059). It is not wrapped in
a `value` field, and its nested `_x` groups and annotations stay inside.

```jsonc
// Observation.valueQuantity (FHIR Quantity)
{ "id": null,
  "extension": [ { "url": "http://example.org/method", "valueString": "scale" } ],
  "value": "72.50",
  "_value": { "__numeric": 72.500000 },
  "unit": "kg", "system": "http://unitsofmeasure.org", "code": "kg",
  "__canonical_code": "g",
  "__canonical_value_exact": { "value": 72500, "scale": 0 } }

// Observation.code.coding[0] (FHIR Coding): only the fields the data has
{ "id": "c1",
  "extension": [ { "url": "http://example.org/origin", "valueString": "lab" } ],
  "system": "http://loinc.org", "code": "29463-7", "display": "Body weight" }

// Patient.name[0] (FHIR HumanName) stored with another producer's field: carried as stored,
// but stripped from the key that =, distinct() and | compare (decision 151)
{ "family": "Smith", "given": [ "Anne" ], "__acme_hash": "77ab" }
```

- Navigating into a complex value builds the element form only for the primitive child reached:
  `valueQuantity.value` gives `{ value: "72.50", __numeric: 72.500000 }`.
- Both forms put annotations at the same level as content, so one accessor serves both:
  `resolve_or_else(e, "__numeric", …)` on the decimal, `resolve_or_else(e, "__canonical_code", …)` on
  the quantity.
- A complex element's own `extension` is an ordinary field, stored inline (FR-003). The switch
  governs only the ids and extensions of primitives.

## 4. The engine: System values

System values come from conversion of FHIR values, from literals and from functions.

### 4.1 Dates: `struct(value, …)`, flat

A System value holds only the annotations available without computing (§4.2 of the design).

```jsonc
// Converted from the FHIR element above: stored annotations only; id and extension dropped (D8)
{ "value": "2024-03-15T10:30+10:00",
  "__start": "2024-03-15T00:30:00.000Z", "__end": "2024-03-15T00:30:59.999999Z" }

// Converted from the partial FHIR element: only __start is kept; end(e) is computed where read
{ "value": "2024-03-15T10:30+10:00", "__start": "2024-03-15T00:30:00.000Z" }

// The literal @2024-03: constants built at plan time
{ "value": "2024-03",
  "__start": "2024-03-01T00:00:00.000Z", "__end": "2024-03-31T23:59:59.999999Z" }

// A function result, such as toDate(): value only (rule A)
{ "value": "2024-03-15" }
```

### 4.2 Scalars

```jsonc
// System.Decimal: the number only, DECIMAL(38,6) (D13, decision 147)
72.500000

// System.String, Integer, Boolean, Time: bare scalars
"Anne"   42   true   "10:30:00"
```

### 4.3 System.Quantity: `{value, unit, code, system}` and flat annotations (decisions 150, 152)

`value` is a System.Decimal. `unit` is the unit as written and `code` its code. `system` follows from the
literal's form. There is no `id` and no `comparator`. The canonical fields are present where they
are available without computing.

```jsonc
// Converted from the annotated FHIR quantity: the stored canonical fields are kept
{ "value": 72.500000, "unit": "kg", "code": "kg", "system": "http://unitsofmeasure.org",
  "__canonical_code": "g",
  "__canonical_value_exact": { "value": 72500, "scale": 0 } }

// Converted from the unannotated FHIR quantity: no canonical fields. A comparison computes the
// canonical form where it reads it, from the FHIR operand's stored text
{ "value": 72.500000, "unit": "kg", "code": "kg", "system": "http://unitsofmeasure.org" }

// The literal 72.50 'kg': the canonical form from the literal's text, as constants at plan time
{ "value": 72.500000, "unit": "kg", "code": "kg", "system": "http://unitsofmeasure.org",
  "__canonical_code": "g",
  "__canonical_value_exact": { "value": 72500, "scale": 0 } }

// The literal 4 days: a calendar duration. Day is not definite, so there is no canonical form
{ "value": 4.000000, "unit": "days", "code": "day",
  "system": "https://hl7.org/fhirpath/N1/calendar-duration" }

// The literal 5 seconds: second is definite, so it canonicalises to UCUM s
{ "value": 5.000000, "unit": "seconds", "code": "second",
  "system": "https://hl7.org/fhirpath/N1/calendar-duration",
  "__canonical_code": "s",
  "__canonical_value_exact": { "value": 5, "scale": 0 } }
```

A FHIR quantity with a comparator, such as `<5 mg`, has no System form; converting it gives empty.

**The canonical form comes from the most exact source,** never from the converted decimal:

| Quantity from | Canonical form from |
|---|---|
| Stored data | The stored annotation, or else the stored text, on the FHIR operand where it is compared. Converted earlier (a merge, `toQuantity()`), the stored annotation if kept, or else the decimal |
| A literal | The literal's text, at plan time |
| Arithmetic | The decimal value |

### 4.4 System.Coding: `{system, code, version, display, userSelected}` (decision 150)

System.Coding has no annotations and no `id`.

```jsonc
// Converted from the FHIR Coding above: id and extension dropped; absent fields are null
{ "system": "http://loinc.org", "code": "29463-7", "version": null,
  "display": "Body weight", "userSelected": null }

// The literal http://loinc.org|29463-7||'Body weight'
{ "system": "http://loinc.org", "code": "29463-7", "version": null,
  "display": "Body weight", "userSelected": null }
```

## 5. How the forms connect

| Step | Effect on annotations | Computes an annotation |
|---|---|---|
| Traversal | Builds the element from `value` and the stored annotation fields | No |
| FHIR → System | The annotation fields are kept; `id`, `extension` and content the System type lacks are dropped. A decimal's value becomes its number: `__numeric`, or else the parsed text, where it is read | No |
| Crossing a lambda (`where`, `select`, `forEach`, `repeat`) | The structure crosses as it is | No |
| Merge (`a \| b`, `iif`) | A System value with `value` and the annotation fields every side has | No |
| A function or arithmetic result | `value` only (rule A) | No |
| An accessor, in a comparison | `resolve_or_else(e, "__start", date_start(e.value))` | Only where nothing is available |
| Output | A primitive outputs `value`; its annotation fields are never output. A complex FHIR value is output in its fitted shape, `_x` included | No |

### 5.1 Merges

A merge of different shapes gives a System value with `value` and the annotation fields every side has
(D9, decision 142; design §4.4). The rest are computed where an operation reads them.

```jsonc
// birthDate | @2000, annotated data: both sides have __start and __end, so both are kept
{ "value": "1970-03", "__start": "1970-03-01T00:00:00.000Z", "__end": "1970-03-31T23:59:59.999999Z" }
{ "value": "2000",    "__start": "2000-01-01T00:00:00.000Z", "__end": "2000-12-31T23:59:59.999999Z" }

// birthDate | @2000, unannotated data: the FHIR side has none, so the intersection is empty
// and the literal's constants are dropped too; accessors compute both sides where read
{ "value": "1970-03" }
{ "value": "2000" }

// valueQuantity | 5 'kg', unannotated data: the result is System.Quantity, with no canonical
// fields on either side; the FHIR side's text is gone, so its canonical form comes from the decimal
{ "value": 72.500000, "unit": "kg", "code": "kg", "system": "http://unitsofmeasure.org" }
{ "value": 5.000000,  "unit": "kg", "code": "kg", "system": "http://unitsofmeasure.org" }
```

## 6. Not shown, or not settled

- **The stored layout's placement and shape** (D4–D6) await the specification's owner (ticket 46).
- **System types with fields of their own** (P1, decision 148) are a suggestion, not a decision
  (design §4.2).
- **Which form is the truth for a computed System value** is deferred until quantity or date arithmetic
  is designed; rule A holds meanwhile (design §6, question 6).
- **Another producer's fields on output.** A complex value is output in its fitted shape, `_x` included
  (FR-059, design §4.5). Whether a field such as `__acme_hash` is output with it is not stated.
- **Calendar-duration comparisons above seconds,** such as `1 year = 12 months`, return empty today;
  they are GitHub #2494 and need no annotation.

---
name: vcl
description: Write FHIR ValueSet Compose Language (VCL), a compact URL-friendly syntax for ValueSet.compose. Use when writing or interpreting a VCL expression, building a VCL implicit ValueSet URL (http://fhir.org/VCL?v1=...), or expressing inclusion/exclusion rules in compact form. Trigger keywords include VCL, ValueSet Compose Language, VCL expression, implicit value set, VCL URL, fhir.org/VCL, compact value set, and operators <<, ~<<, <!, ^ in a ValueSet context.
---

You are an expert in the FHIR ValueSet Compose Language (VCL), a domain-specific language for expressing `ValueSet.compose` content as a compact, URL-safe string. VCL is inspired by SNOMED CT's Expression Constraint Language and is defined in the [FHIR IG Guidance](https://build.fhir.org/ig/FHIR/ig-guidance/vcl.html).

## When to use VCL

Use VCL when the goal is to convey a `ValueSet.compose` rule in a single string, typically inside a URL passed to `$expand` or `$validate-code`. VCL covers everything `compose` can express: code system selection, enumerated codes, filters, included sub-ValueSets, exclusions, and property navigation - across one or many code systems.

Choose VCL over alternatives when:

- The ValueSet is short-lived or query-time only and a full `ValueSet` resource would be heavyweight.
- The expression spans multiple code systems and needs to be carried in a URL.
- The use case is dynamic generation (e.g. an autocomplete that builds expressions on the fly).

Prefer SNOMED CT ECL (see the `snomed-ecl` skill) when working purely within SNOMED CT and the existing `http://snomed.info/sct?fhir_vs=ecl/...` syntax is sufficient. ECL is richer for SNOMED-specific concept-model navigation; VCL is broader but simpler.

## Lexical rules

- Whitespace (spaces, tabs) is ignored except inside quoted values. Newlines and carriage returns are not permitted.
- Codes and values use unquoted form (`SCODE`) when they consist only of alphanumerics, hyphens, and underscores starting with an alphanumeric. Anything else (periods, slashes, colons, spaces) requires double quotes.
- Inside quoted values, only `"` and `\` are escapable, written as `\"` and `\\`.
- URIs require an ASCII-letter scheme. Round brackets inside a URI must be percent-encoded (`%28`, `%29`) because brackets delimit the `systemUri`.
- A URI may carry a version pinned with `|`, e.g. `http://loinc.org|2.74`.

## Codes and the wildcard

| Expression | Meaning                 |
| ---------- | ----------------------- |
| `B`        | Single code `B`         |
| `"B.123"`  | Single code `B.123`     |
| `*`        | All codes in the system |

## Set operators

| Operator    | Symbol | Notes                                |
| ----------- | ------ | ------------------------------------ |
| Conjunction | `,`    | AND - intersection of subexpressions |
| Disjunction | `;`    | OR - union of subexpressions         |
| Exclusion   | `-`    | Removes the right-hand subset        |
| Grouping    | `( )`  | Disambiguates mixed `,` and `;`      |

There is no operator precedence between `,` and `;`. Always use brackets when mixing them.

```
(A;B),C       // (A or B) and C
A;(B,C)       // A or (B and C)
(A;B)-C       // (A or B) minus C
```

## Filter operators

Filters are written as `property OP value`. They map directly to `ValueSet.compose.include.filter` triples (`property`, `op`, `value`).

| Operator        | Symbol | Right-hand side                | Example                |
| --------------- | ------ | ------------------------------ | ---------------------- |
| equals          | `=`    | code                           | `parent = 73211009`    |
| is-a            | `<<`   | code                           | `concept << 73211009`  |
| is-not-a        | `~<<`  | code                           | `concept ~<< 46635009` |
| descendent-of   | `<`    | code                           | `concept < 73211009`   |
| child-of        | `<!`   | code                           | `concept <! 404684003` |
| descendent-leaf | `!!<`  | code                           | `concept !!< 64572001` |
| generalizes     | `>>`   | code                           | `concept >> 44054006`  |
| regex           | `/`    | quoted string                  | `code / "A[0-9]*\\.9"` |
| in (value set)  | `^`    | code list, URI, or filter list | `^http://acme/VS/x`    |
| not-in          | `~^`   | code list, URI, or filter list | `~^{A,B,C}`            |
| exists          | `?`    | code (property name)           | `ingredient ? true`    |

The `^` operator inside a filter checks property values against an enumerated set, an external ValueSet, or a nested filter. As a top-level expression, `^URI` includes all codes from another ValueSet.

## System URI prefix

A code system is selected by prefixing a `systemUri` in round brackets:

```
(http://loinc.org)4548-4
(http://snomed.info/sct)73211009
(http://snomed.info/sct|http://snomed.info/sct/32506021000036107/version/20230831)73211009
```

The system applies to the immediately following expression. Combine multiple systems with `;`, `,` or `-`:

```
(http://loinc.org)(41995-2;4548-4) - (http://loinc.org)29557-6
```

## Property navigation

The `.` operator (read as "of") flips a filter so the property is taken from a left-hand set of codes:

```
B.codeprop                    // values of `codeprop` on code B
{concept < B}.codeprop        // values of `codeprop` over all descendants of B
{B.codeprop1}.codeprop2       // chained navigation
```

This is the inverse of the ordinary `property = code` form and is useful for "give me everything pointed to by these codes".

## Including another ValueSet

```
^http://hl7.org/fhir/ValueSet/payeetype
```

Maps to a `compose.include.valueSet` entry. Combine with codes, filters or other ValueSets through the usual set operators.

## Implicit ValueSet URL form

VCL is most often used inside an implicit ValueSet URL passed to `$expand`:

```
http://fhir.org/VCL?v1=[percent-encoded-expression]
```

Rules:

- The base URL is `http://fhir.org/VCL`. The query parameter is `v1` (the version of the VCL syntax).
- The expression must be percent-encoded. Servers SHALL percent-decode `v1` before parsing.
- Round brackets must be encoded as `%28` and `%29`; curly braces as `%7B` and `%7D`; the caret `^` as `%5E`.

Example - all LOINC codes whose `parent` is one of two codes:

```
http://fhir.org/VCL?v1=(http://loinc.org)(parent%5E%7BLP46821-2,LP259418-4%7D)
```

Decoded expression:

```
(http://loinc.org)(parent^{LP46821-2,LP259418-4})
```

## Mapping to ValueSet.compose

A useful mental model is that a VCL expression compiles down to one or more `compose.include` (and possibly `compose.exclude`) entries.

| VCL element                | ValueSet.compose element                                  |
| -------------------------- | --------------------------------------------------------- |
| `(systemUri)` prefix       | `include.system` (with optional `version` after `\|`)     |
| Bare code `B`              | `include.concept[].code`                                  |
| Filter `prop OP value`     | `include.filter` triple `{property, op, value}`           |
| `^http://.../ValueSet/...` | `include.valueSet`                                        |
| Conjunction `,`            | Multiple filters within the same `include` (AND)          |
| Disjunction `;`            | Multiple `include` entries (OR)                           |
| Exclusion `-`              | Right-hand side becomes a `compose.exclude` entry         |
| Grouping `( )`             | Determines which include or exclude an element belongs to |

Example - this VCL:

```
(http://loinc.org)(41995-2;4548-4) - (http://loinc.org)29557-6
```

corresponds to this `compose`:

```json
{
    "compose": {
        "include": [
            {
                "system": "http://loinc.org",
                "concept": [{ "code": "41995-2" }, { "code": "4548-4" }]
            }
        ],
        "exclude": [
            {
                "system": "http://loinc.org",
                "concept": [{ "code": "29557-6" }]
            }
        ]
    }
}
```

## Worked examples

### Single concept and descendants

```
(http://snomed.info/sct)concept << 73211009
```

All diabetes mellitus concepts (self plus descendants).

### Multi-system union

```
(http://loinc.org)(41995-2;4548-4;4549-2);
(http://snomed.info/sct)(365845005;165679005);
(http://www.ama-assn.org/go/cpt)(83036;83037)
```

Three code systems unioned together, each with its own enumerated codes.

### Inclusion minus exclusion across systems

```
((http://snomed.info/sct)concept << 17311000168105) - ((http://loinc.org)76573-5)
```

All descendants of the SNOMED concept, minus a single LOINC code.

### Filter on a property

```
(http://loinc.org)COMPONENT = LP212516-1, PROPERTY = LP6817-3, TIME_ASPCT = LP6960-1
```

LOINC codes that match all three property filters (AND).

### Regex filter

```
(http://loinc.org)COMPONENT / ".*Dichloroethane.*"
```

LOINC codes whose `COMPONENT` term matches the regex.

### Property navigation

```
(http://loinc.org){concept < LP12345}.parent
```

The `parent` property values for every descendant of `LP12345`.

### Nested property filter

```
(http://acme.com/cs)consists_of ^ { has_ingredient ^ { has_tradename = 2201670 } }
```

Codes that consist of something which has an ingredient with a particular tradename.

### Mixing direct codes and an included ValueSet

```
(http://loinc.org)10007-3 ; ^http://loinc.org/vs/LP257682-7
```

Direct LOINC code plus everything in the named ValueSet.

### Boolean property

```
(http://snomed.info/sct)inactive = true
```

All inactive SNOMED codes.

### Existence check

```
(http://acme.com/cs)ingredient ? true
```

All codes that have an `ingredient` property defined.

## Common pitfalls

- **Forgetting to quote codes with periods or slashes.** `B.123` is parsed as property navigation, not as a code. Write `"B.123"`.
- **Missing percent-encoding in URLs.** `(`, `)`, `{`, `}`, `^`, `,`, `;` and `#` all need encoding when the expression is the value of `v1`.
- **Mixing `,` and `;` without brackets.** There is no precedence; the parser may accept it but the meaning is ambiguous to readers. Always group.
- **Using a URI scheme that is not all ASCII letters.** The lexer rejects digits or punctuation in the scheme.
- **Forgetting that whitespace excludes newlines.** A pretty-printed multi-line VCL expression is invalid. Keep it on a single line.
- **Unbracketed system change.** A `(systemUri)` only applies to the immediately following expression, not the whole document. To apply it to a group, wrap the group: `(http://loinc.org)(A;B;C)`.
- **Confusing `^` operators.** As a top-level operator, `^URI` means "include this ValueSet". As a filter operator (`property ^ {...}`), it means "in this set of values".

## When to load the formal grammar

For most authoring tasks the operator and example tables above are sufficient. Load `references/grammar.md` when:

- A precise lexical question arises (e.g. which characters are allowed in an unquoted code, or how a URI may end).
- Implementing or debugging a parser.
- Interpreting an unfamiliar construct that is not covered by the tables.

## Resources

- VCL specification: https://build.fhir.org/ig/FHIR/ig-guidance/vcl.html
- ValueSet resource: https://hl7.org/fhir/valueset.html
- SNOMED ECL (related, SNOMED-specific): see the `snomed-ecl` skill
- FHIR `$expand` operation: https://hl7.org/fhir/valueset-operation-expand.html

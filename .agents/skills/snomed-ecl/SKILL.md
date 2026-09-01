---
name: snomed-ecl
description: Write SNOMED CT Expression Constraint Language (ECL) queries to search and constrain concepts. Use when writing ECL queries, constraining SNOMED concepts, filtering clinical terminology, or creating FHIR value sets with ECL. Trigger keywords include ECL, Expression Constraint Language, SNOMED query, SNOMED expression, ECL syntax, descendant of, ECL v2, implicit value set, FHIR value set, terminology server.
---

You are an expert in SNOMED CT Expression Constraint Language (ECL), a formal syntax for representing computable rules that define bounded sets of clinical meanings.

## Overview

ECL is used to:

- Restrict valid EHR data values to specific SNOMED CT concepts.
- Define concept-based reference sets.
- Execute machine-processable queries.
- Constrain attribute ranges within SNOMED CT's concept model.

ECL queries return zero or more SNOMED CT concept codes with no duplicates. Evaluations are version-specific to the SNOMED CT edition being used.

## Basic syntax

### Concept references

Concepts can be referenced by concept ID with optional display term:

```
73211009
73211009 |Diabetes mellitus|
```

Display terms are for readability only and must be enclosed in pipe characters `|`.

### Wildcard

```
*
```

Returns all active and inactive concepts.

### Reference set members

```
^ 32570481000036109
```

Returns all concepts that are members of the specified reference set.

## Hierarchy navigation

ECL provides operators to navigate the SNOMED CT hierarchy:

| Operator | Meaning                                       | Example                             |
| -------- | --------------------------------------------- | ----------------------------------- |
| `<`      | Descendants only (excludes concept itself)    | `< 73211009`                        |
| `<<`     | Descendants or self (includes concept itself) | `<< 73211009 \|Diabetes mellitus\|` |
| `<!`     | Immediate children only                       | `<! 404684003`                      |
| `<<!`    | Immediate children or self                    | `<<! 404684003`                     |
| `>`      | Ancestors only (excludes concept itself)      | `> 73211009`                        |
| `>>`     | Ancestors or self (includes concept itself)   | `>> 73211009`                       |
| `>!`     | Immediate parents only                        | `>! 40541001`                       |
| `>>!`    | Immediate parents or self                     | `>>! 40541001`                      |

### Examples

Find all types of diabetes:

```
<< 73211009 |Diabetes mellitus|
```

Find immediate subtypes of a disease:

```
<! 64572001 |Disease|
```

Find all ancestors of appendicitis:

```
> 74400008 |Appendicitis|
```

## Set operations

Combine constraints using boolean operators:

| Operator | Function     | Example                         |
| -------- | ------------ | ------------------------------- |
| `AND`    | Intersection | `<< 19829001 AND << 301867009`  |
| `OR`     | Union        | `<< 73211009 OR << 38341003`    |
| `MINUS`  | Difference   | `<< 73211009 MINUS << 46635009` |

### Examples

Find procedures on the heart:

```
<< 71388002 |Procedure| AND << 80891009 |Heart structure|
```

Find diabetes or hypertension:

```
<< 73211009 |Diabetes mellitus| OR << 38341003 |Hypertensive disorder|
```

Find all diabetes except type 1:

```
<< 73211009 |Diabetes mellitus| MINUS << 46635009 |Type 1 diabetes mellitus|
```

## Attribute constraints

Constrain concepts based on their attributes (relationships):

### Basic attribute constraint

```
< 19829001 |Disorder of lung| : 116676008 |Associated morphology| = 79654002 |Edema|
```

This finds lung disorders with edema as the associated morphology.

### Hierarchy-aware attributes

Both the attribute type and value can use hierarchy operators:

```
<< 404684003 |Clinical finding| :
  << 47429007 |Associated with| = << 267038008 |Edema|
```

This finds clinical findings with any "associated with" attribute whose value is edema or a subtype of edema.

### Multiple attributes

Use commas to separate multiple attribute constraints:

```
<< 404684003 |Clinical finding| :
  << 363698007 |Finding site| = << 39057004 |Pulmonary valve structure|,
  << 116676008 |Associated morphology| = << 415582006 |Stenosis|
```

This finds clinical findings at the pulmonary valve site with stenosis morphology.

### Grouped attributes

Use curly braces to specify that attributes must be in the same role group:

```
<< 125605004 |Fracture of bone| :
  {
    << 363698007 |Finding site| = << 272673000 |Bone structure of tibia|,
    << 116676008 |Associated morphology| = << 72704001 |Fracture|
  }
```

### Nested attributes

Constrain an attribute value by its own attributes:

```
<< 404684003 |Clinical finding| :
  << 363698007 |Finding site| = (
    << 91723000 |Anatomical structure| :
      << 272741003 |Laterality| = << 7771000 |Left|
  )
```

This finds clinical findings on the left side of the body.

### Reversed attributes

Use dot notation to traverse relationships in reverse:

```
< 125605004 |Fracture of bone| . 363698007 |Finding site|
```

This returns all the finding sites (anatomical structures) that are referenced by fracture concepts.

### Cardinality constraints

Specify how many times an attribute can occur:

```
<< 373873005 |Pharmaceutical / biologic product| :
  [1..3] << 127489000 |Has active ingredient| = << 372687004 |Amoxicillin|
```

The cardinality syntax is `[min..max]` where:

- `[n..n]` - exactly n occurrences
- `[n..*]` - n or more occurrences
- `[0..n]` - zero to n occurrences
- `[1..*]` - one or more occurrences

## Filters

ECL supports filters to further constrain results based on concept properties:

### Term filters

```
<< 73211009 |Diabetes mellitus| {{ term = "type 1" }}
```

This finds diabetes concepts whose term contains "type 1".

### Definition status filter

```
<< 404684003 |Clinical finding| {{ definitionStatus = defined }}
```

This finds fully defined clinical findings.

### Module filter

```
* {{ module = 900000000000207008 |SNOMED CT core module| }}
```

This finds concepts in the core SNOMED CT module.

### Effective time filter

```
* {{ effectiveTime = "20230131" }}
```

This finds concepts with a specific effective time.

### Active status filter

```
<< 73211009 {{ active = true }}
```

This finds active diabetes concepts.

## History supplements

History supplements augment ECL query results with inactive SNOMED CT concepts that are semantically linked to the active results. As SNOMED CT evolves, previously recorded concepts become inactivated. Rather than querying older editions, history supplements use historical association reference sets to retrieve both active and relevant inactive concepts from the current edition.

### Syntax

History supplements use double braces with a plus sign prefix, appended after an ECL expression:

```
<< 195967001 |Asthma| {{ + HISTORY ( 900000000000527005 |SAME AS association reference set| ) }}
```

The general form expands to a union of the original query with a member filter on the specified reference set:

```
@ecl_query OR ^ @history_refset_query {{ M targetComponentId = @ecl_query }}
```

### History supplement profiles

Three standardised profiles provide different precision/recall trade-offs:

| Profile  | Keyword       | Use case                                   | Reference sets used                                                           |
| -------- | ------------- | ------------------------------------------ | ----------------------------------------------------------------------------- |
| Minimum  | `HISTORY-MIN` | Clinical decision support (high precision) | SAME AS                                                                       |
| Moderate | `HISTORY-MOD` | Clinical research and audit (balanced)     | SAME AS, REPLACED BY, WAS A, PARTIALLY EQUIVALENT TO                          |
| Maximum  | `HISTORY-MAX` | Patient identification (high recall)       | All subtypes of `900000000000522004 \|Historical association reference set\|` |

### Profile examples

**HISTORY-MIN** returns only one-to-one equivalent inactive concepts:

```
<< 195967001 |Asthma| {{ + HISTORY-MIN }}
```

**HISTORY-MOD** includes replacements and partial equivalences:

```
<< 195967001 |Asthma| {{ + HISTORY-MOD }}
```

**HISTORY-MAX** includes all possible historical associations:

```
<< 195967001 |Asthma| {{ + HISTORY-MAX }}
```

The following forms are all equivalent to HISTORY-MAX:

```
<< 195967001 |Asthma| {{ + HISTORY-MAX }}
<< 195967001 |Asthma| {{ + HISTORY (< 900000000000522004 |Historical association reference set|) }}
<< 195967001 |Asthma| {{ + HISTORY (*) }}
<< 195967001 |Asthma| {{ + HISTORY }}
```

### Custom history supplement

You can specify a particular association reference set directly:

```
<< 195967001 |Asthma| {{ + HISTORY ( 900000000000527005 |SAME AS association reference set| ) }}
```

### Practical examples

Find all referral-to-service procedures, including inactive concepts that have a SAME AS association with an active match:

```
<< 306206005 |Referral to service| {{ + HISTORY-MIN }}
```

Find all types of breast pain, including any historically associated inactive concepts:

```
<< 53430007 |Pain of breast| {{ + HISTORY-MAX }}
```

### Notes

- The `MOVED FROM` association reference set (`900000000000525002`) is not fully supported by the template pattern because its directional semantics are reversed.
- Choose the profile that matches your use case: HISTORY-MIN for precision-critical scenarios, HISTORY-MAX when recall matters more, and HISTORY-MOD for a balance between the two.

## Operator precedence

Operators are evaluated in this order (highest to lowest):

1. Attribute constraints and filters `:`
2. Nested expressions `()`
3. `MINUS`
4. `AND`
5. `OR`

Use parentheses to override default precedence:

```
(<< 73211009 |Diabetes mellitus| OR << 38341003 |Hypertensive disorder|)
  AND << 404684003 |Clinical finding|
```

## Using ECL with FHIR terminology servers

ECL can be used to define implicit value sets when working with FHIR terminology servers. This allows you to dynamically define value sets using ECL expressions rather than enumerating all concepts.

### Implicit value set URL syntax

FHIR terminology servers support ECL through implicit value set URLs with this pattern:

```
http://snomed.info/sct?fhir_vs=ecl/[uri-encoded-ecl]
```

The ECL expression must be URI-encoded in the URL. For example:

```
http://snomed.info/sct?fhir_vs=ecl/%3C%3C%2073211009
```

This represents the ECL expression `<< 73211009` (all descendants of diabetes mellitus).

### Version and edition specification

You can specify a particular SNOMED CT edition and version:

```
http://snomed.info/sct/[edition]/version/[version]?fhir_vs=ecl/[uri-encoded-ecl]
```

For example, to use the Australian edition:

```
http://snomed.info/sct/32506021000036107/version/20230831?fhir_vs=ecl/%3C%3C%2073211009
```

When no edition or version is specified, the terminology server uses its default edition (or the International Edition if no default is configured).

### ValueSet resource with ECL filter

You can define a ValueSet resource that uses ECL as a filter. This is the structured approach for defining ECL-based value sets:

**JSON format:**

```json
{
    "resourceType": "ValueSet",
    "id": "diabetes-disorders",
    "url": "http://example.org/fhir/ValueSet/diabetes-disorders",
    "name": "DiabetesDisorders",
    "title": "Diabetes disorders",
    "status": "active",
    "compose": {
        "include": [
            {
                "system": "http://snomed.info/sct",
                "filter": [
                    {
                        "property": "constraint",
                        "op": "=",
                        "value": "<< 73211009 |Diabetes mellitus|"
                    }
                ]
            }
        ]
    }
}
```

**XML format:**

```xml
<ValueSet xmlns="http://hl7.org/fhir">
  <id value="diabetes-disorders"/>
  <url value="http://example.org/fhir/ValueSet/diabetes-disorders"/>
  <name value="DiabetesDisorders"/>
  <title value="Diabetes disorders"/>
  <status value="active"/>
  <compose>
    <include>
      <system value="http://snomed.info/sct"/>
      <filter>
        <property value="constraint"/>
        <op value="="/>
        <value value="&lt;&lt; 73211009 |Diabetes mellitus|"/>
      </filter>
    </include>
  </compose>
</ValueSet>
```

### Key elements of ECL filters in FHIR

- **property**: Must be `constraint` to indicate an ECL expression.
- **op**: Must be `=` for ECL constraints.
- **value**: The ECL expression (not URI-encoded in the ValueSet resource).

### FHIR terminology operations with ECL

ECL-based value sets can be used with standard FHIR terminology operations:

**$expand operation:**

```
GET [base]/ValueSet/$expand?url=http://snomed.info/sct?fhir_vs=ecl/%3C%3C%2073211009
```

This expands the value set to return all concepts matching the ECL expression.

**$validate-code operation:**

```
GET [base]/ValueSet/$validate-code?url=http://snomed.info/sct?fhir_vs=ecl/%3C%3C%2073211009&code=44054006&system=http://snomed.info/sct
```

This checks if a specific code is valid within the ECL-defined value set.

### Examples with different ECL patterns

**All procedures on the heart:**

```json
{
    "system": "http://snomed.info/sct",
    "filter": [
        {
            "property": "constraint",
            "op": "=",
            "value": "<< 71388002 |Procedure| : << 363704007 |Procedure site| = << 80891009 |Heart structure|"
        }
    ]
}
```

**Pharmaceutical products containing paracetamol:**

```json
{
    "system": "http://snomed.info/sct",
    "filter": [
        {
            "property": "constraint",
            "op": "=",
            "value": "<< 373873005 |Pharmaceutical / biologic product| : << 127489000 |Has active ingredient| = << 387517004 |Paracetamol|"
        }
    ]
}
```

**All active diabetes concepts:**

```json
{
    "system": "http://snomed.info/sct",
    "filter": [
        {
            "property": "constraint",
            "op": "=",
            "value": "<< 73211009 |Diabetes mellitus| {{ active = true }}"
        }
    ]
}
```

### Benefits of ECL in FHIR value sets

1. **Dynamic expansion**: Value sets automatically include new concepts added to SNOMED CT that match the constraint.
2. **Maintainability**: No need to manually update enumerated concept lists.
3. **Precision**: Complex clinical meanings can be expressed precisely using attributes and filters.
4. **Interoperability**: Standard approach supported by FHIR-compliant terminology servers.
5. **Compactness**: A single ECL expression can represent thousands of concepts.

## Best practices

1. **Use display terms for readability**: Include `|display term|` after concept IDs to make queries self-documenting.

2. **Prefer `<<` over `<` when appropriate**: If you want to include the concept itself in results, use `<<` (descendants or self).

3. **Be specific with attributes**: Use the most specific attribute type that matches your intent.

4. **Use role grouping when necessary**: SNOMED CT uses role groups to disambiguate relationships. Use `{}` when you need attributes to be in the same group.

5. **Consider cardinality**: When the number of relationships matters, use cardinality constraints.

6. **Test incrementally**: Build complex queries incrementally, testing each part before adding more constraints.

7. **Be aware of version differences**: ECL results depend on the SNOMED CT version. Queries may return different results across versions.

8. **Use filters for non-structural constraints**: When constraining based on metadata (terms, status, modules), use filter syntax `{{ }}`.

9. **Document complex queries**: Add comments or documentation explaining the clinical intent of complex ECL expressions.

10. **Validate against the specification**: The current version is ECL v2.2. Ensure your implementation supports the features you're using.

11. **Use ECL for dynamic FHIR value sets**: When defining FHIR value sets, prefer ECL filters over enumerated concept lists for maintainability and automatic inclusion of new relevant concepts.

12. **URI-encode ECL in URLs**: When using implicit value set URLs, remember to properly URI-encode the ECL expression.

13. **Use history supplements for retrospective queries**: When querying patient data that may contain inactive concepts, use history supplements to capture historically associated concepts. Choose the profile (MIN, MOD, MAX) that matches the required precision/recall balance for your use case.

## Common patterns

### Find all disorders of a body structure

```
<< 64572001 |Disease| :
  << 363698007 |Finding site| = << 39057004 |Pulmonary valve structure|
```

### Find procedures by method and site

```
<< 71388002 |Procedure| :
  << 405813007 |Procedure site - Direct| = << 80891009 |Heart structure|,
  << 424226004 |Using device| = << 360062009 |Pacemaker|
```

### Find products with specific ingredients

```
<< 373873005 |Pharmaceutical / biologic product| :
  << 127489000 |Has active ingredient| = << 387517004 |Paracetamol|
```

### Find concepts modified in a specific time period

```
<< 404684003 |Clinical finding| {{
  effectiveTime = ("20230101".."20231231")
}}
```

### Find lateralised findings

```
<< 404684003 |Clinical finding| :
  << 363698007 |Finding site| = (
    << 91723000 |Anatomical structure| :
      << 272741003 |Laterality| = << 24028007 |Right|
  )
```

## Quick reference table

| Pattern               | ECL Syntax                                                       |
| --------------------- | ---------------------------------------------------------------- |
| Single concept        | `73211009`                                                       |
| With display term     | `73211009 \|Diabetes mellitus\|`                                 |
| Descendants           | `< 73211009`                                                     |
| Descendants or self   | `<< 73211009`                                                    |
| Children only         | `<! 73211009`                                                    |
| Ancestors             | `> 73211009`                                                     |
| Ancestors or self     | `>> 73211009`                                                    |
| Parents only          | `>! 73211009`                                                    |
| Intersection          | `<< 73211009 AND << 46635009`                                    |
| Union                 | `<< 73211009 OR << 38341003`                                     |
| Difference            | `<< 73211009 MINUS << 46635009`                                  |
| With attribute        | `<< 404684003 : 116676008 = 79654002`                            |
| Multiple attributes   | `<< 404684003 : attr1 = val1, attr2 = val2`                      |
| Grouped attributes    | `<< 404684003 : { attr1 = val1, attr2 = val2 }`                  |
| Nested constraint     | `<< 404684003 : 363698007 = (<< 91723000 : 272741003 = 7771000)` |
| With cardinality      | `<< 373873005 : [1..3] 127489000 = 372687004`                    |
| Term filter           | `<< 73211009 {{ term = "type 1" }}`                              |
| Active filter         | `<< 73211009 {{ active = true }}`                                |
| Reference set members | `^ 32570481000036109`                                            |
| History supplement    | `<< 195967001 {{ + HISTORY-MIN }}`                               |

## Resources

- Official ECL specification: https://docs.snomed.org/snomed-ct-specifications/snomed-ct-expression-constraint-language
- ECL cheat sheet: https://ontoserver.csiro.au/shrimp/ecl_help.html
- FHIR SNOMED CT implicit value sets: https://terminology.hl7.org/SNOMEDCT.html
- Current ECL version: v2.2

---
sidebar_position: 1
---

# Python

A Python package named [pathling](https://pypi.org/project/pathling/) is
available from PyPI.

[View the generated API documentation &rarr;](pathname:///docs/python/)

## Encoders

The Python library features a set of encoders for converting FHIR data into
Spark dataframes.

See the [Encoders](/docs/encoders) page for more details.

## Value set membership

The `member_of` function can be used to test the membership of a code within a
FHIR value set. In this example, we take a list of SNOMED CT diagnosis codes and
create a new column which shows which are viral infections.

```python
# << 64572001|Disease| : (
#   << 370135005|Pathological process| = << 441862004|Infectious process|,
#   << 246075003|Causative agent| = << 49872002|Virus|
# )
result = pc.member_of(csv, to_coding(csv.CODE, 'http://snomed.info/sct'),
                      'http://snomed.info/sct?fhir_vs=ecl/%3C%3C%2064572001%20%3A%20('
                      '%3C%3C%20370135005%20%3D%20%3C%3C%20441862004%20%2C%20%3C%3C%2'
                      '0246075003%20%3D%20%3C%3C%2049872002%20)',
                      'VIRAL_INFECTION')
result.select('CODE', 'DESCRIPTION', 'VIRAL_INFECTION').show()
```

Results in:

| CODE      | DESCRIPTION               | VIRAL_INFECTION |
|-----------|---------------------------|-----------------|
| 65363002  | Otitis media              | false           |
| 16114001  | Fracture of ankle         | false           |
| 444814009 | Viral sinusitis           | true            |
| 444814009 | Viral sinusitis           | true            |
| 43878008  | Streptococcal sore throat | false           |

## Concept translation

The `translate` function can be used to translate codes from one code system to
another using maps that are known to the terminology server. In this example, we
translate our SNOMED CT diagnosis codes into Read CTV3.

```python
result = pc.translate(csv, to_coding(csv.CODE, 'http://snomed.info/sct'),
                      'http://snomed.info/sct/900000000000207008?fhir_cm='
                      '900000000000497000',
                      output_column_name='READ_CODE')
result = result.withColumn('READ_CODE', result.READ_CODE.code)
result.select('CODE', 'DESCRIPTION', 'READ_CODE').show()
```

Results in:

| CODE      | DESCRIPTION               | READ_CODE |
|-----------|---------------------------|-----------|
| 65363002  | Otitis media              | X00ik     |
| 16114001  | Fracture of ankle         | S34..     |
| 444814009 | Viral sinusitis           | XUjp0     |
| 444814009 | Viral sinusitis           | XUjp0     |
| 43878008  | Streptococcal sore throat | A340.     |

## Subsumption testing

The `subsumes` function allows us to perform subsumption testing on codes within
our data. In hierarchical code systems, a subsumption test determines whether a
code is a subtype of another code, e.g. an "ankle fracture" is subsumed by "
fracture".

In this example, we first take our codes, cross-join them and then test whether
they subsume each other. Then we do another subsumption test against the "ear,
nose and throat disorder" concept.

```python
# 232208008 |Ear, nose and throat disorder|
left_coding = Coding('http://snomed.info/sct', '232208008')
right_coding_column = to_coding(csv.CODE, 'http://snomed.info/sct')

result = pc.subsumes(csv, 'SUBSUMES',
                     left_coding=left_coding,
                     right_coding_column=right_coding_column)

result.select('CODE', 'DESCRIPTION', 'IS_ENT').show()
```

Results in:

| CODE      | DESCRIPTION       | IS_ENT |
|-----------|-------------------|--------|
| 65363002  | Otitis media      | true   |
| 16114001  | Fracture of ankle | false  |
| 444814009 | Viral sinusitis   | true   |

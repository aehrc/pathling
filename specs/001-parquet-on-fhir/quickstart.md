# Quickstart: Parquet on FHIR

End-to-end scenarios that exercise the feature. Each is phrased so it can be run
as validation once the work is complete, and each maps to a user story in
[spec.md](spec.md).

## QS-001 Round trip a real corpus (US2)

Load the FHIR R4 specification examples and a Synthea dataset, write them, read
them back and export.

**Entry point**: through `io`'s own transform and serialiser in M1, M2 and M3,
and again through `PathlingContext` once M4 rewires it. The public API keeps the
previous layout until then, so running this scenario against it before M4 proves
nothing about the new one.

**Expect**: every conformant resource is semantically equal to its source —
object key order ignored, array order significant, numbers compared lexically.

**Must include**, because these are the cases that fail silently: a decimal with
a trailing zero, one in exponent notation, one with a leading sign, one of very
small magnitude, and one with forty significant digits. Also a resource with an
element absent, to confirm it is absent from the output rather than null, and a
resource where a structure's fields are all empty, to confirm it does not
serialise as an empty object.

**Also expect**: supplying the same resources as a dataset of strings rather than
as files loses the lexical form of decimals, as documented. This is asserted, so
the limitation cannot silently widen.

## QS-002 Inspect the stored layout (US1)

Load a small corpus through `io` and inspect the stored schema and values. As
with QS-001, this runs against the public API only from M4 onward.

**Expect**, from M5 when every annotation kind has landed: decimals stored as
text with a numeric annotation beside them; primitive ids and extensions in the
metadata group; extensions on complex elements inline; date range annotations;
two canonical annotations beside a quantity, the specification's followed by one
whose precision preserves magnitude. No field identifier, no root-level extension
map, no versioned identifier column, no decimal scale column.

**Expect at M1 through M4**: the same, except that the annotation fields and the
`_field` groups are present in the schema and unpopulated. The layout ships
annotation-free at the flip (decision 50) and the metadata group is written in M5
(decision 49), so this scenario is run in two parts rather than deferred whole.

## QS-003 Run the engine over the new layout (US3)

Run the FHIRPath test suite, both YAML conformance baselines and the SQL-on-FHIR
compliance suite over data in the new layout.

**Expect**: all pass. The exclusion baselines gain no new entries, and entries
that become obsolete are removed rather than left to self-report — the baseline
polices itself, so an obsolete exclusion reports as an excluded test that
unexpectedly passed.

Requires `git submodule update --init` for the compliance fixtures.

## QS-004 Run the engine with annotations disabled (US3)

Write the same corpus with every annotation disabled, and run the full suite
over it.

**Expect**: identical results. This is the check that annotations are a fast path
rather than a requirement, and therefore that Pathling can read conformant files
written by anything else.

## QS-005 Query a schema fitted to the data (US4)

Build a dataset in which several elements are never populated. Evaluate
expressions that traverse to them.

**Expect**: empty collections, not errors. An expression naming an element FHIR
does not define still errors. A choice variant never populated returns empty. An
expression combining an absent element with a populated one succeeds.

**Also**: a ViewDefinition column over an absent element produces a column of the
right type whether or not the column declares one — declared type if present,
otherwise the type the definitions give the element. An absent primitive
reaching a caller as a column carries the element's type and can be written to
Parquet.

**And the agnosticism check**: build one column expression without reference to
any dataset, then apply it to this fitted dataset and to a dense dataset holding
the same resources. The results must be equal. This is what makes the
expression-to-column API's unchanged signature honest.

**And combination across shapes**: combine two collections of the same FHIR type
reached by different paths — `name` and `contact.name` — whose fitted schemas
differ. The result must hold every element of both and be traversable. Assert the
field order of the result is definition order, since positional structure
equality makes a divergent order a silent wrong answer rather than a failure.

## QS-006 Load two divergent batches (US5)

Load a batch, then load a second batch containing elements the first did not
carry. Query across both, as raw files and as a transactional table.

**Expect**: every resource from both batches, with correct cardinality. In
particular, a view that unnests a repeating element and projects a leaf only the
second batch populated still returns the first batch's resources, with that leaf
null.

This is the scenario that silently loses rows if unnesting is ever reshaped into
a leaf-level read, so it is the regression test for that constraint.

## QS-007 Navigate primitive ids and extensions (US6)

Evaluate expressions reaching a primitive element's id and its extensions, over
fixtures carrying them.

**Expect**: the id and the extensions are returned; empty where the source
carried neither. This capability did not exist before.

## QS-008 Read data written by an earlier release (US7)

Point each file-based source at data written by a current release.

**Expect**: an error at read time naming the resource type, the detected layout,
the expected layout and the remedy. A conforming but narrow schema reads
successfully. A schema carrying no markers reads successfully. Disabling
detection for one source permits the read and leaves other sources unaffected.

Then append to the same dataset through a file-based sink.

**Expect**: the same error, raised before any schema merge is attempted. The
append must not succeed and leave one table carrying marker fields from both
layouts, which is what auto-merge would otherwise do for a resource type whose
columns happen not to conflict.

## QS-009 Confirm the module boundary (US8)

Add a dependency on internal Spark Catalyst API to the definitions and schema
module, and separately add an import of it to the new encoding code.

**Expect**: the build fails in both cases. Confirm also that the existing
encoding implementation is unmodified and still resolves at its coordinates.

## QS-010 Measure (US8, driver 1)

Run the benchmark suite, comparing against the baseline captured before any
change. Time encoding, decoding and query execution separately, and planning
time separately from execution time.

**Expect**: nothing in particular. The measurement is the deliverable. If it is
flat, driver 1 becomes a non-goal and the work stands on the losslessness,
attribute-level metadata and internal-API drivers.

Measure the two ingest mechanisms against each other at the same time: the
chosen approach, and direct parsing into a variant. That comparison decides
whether the lexical-decimal limitation is worth removing by changing mechanism.

# Absence coverage by position kind

Taken for T041 of [tasks.md](../tasks.md). SC-005 asks that absence be tested at every kind of
position, each giving the result of the present-but-empty element. This record maps each kind to
the tests that cover it, and names the gaps that T042 fills.

A test covers a kind where it evaluates expressions over an element that the schema does not carry
and asserts the answer the same data gives where the schema carries the element as an unpopulated
null. `AbsentElementTest` does this for every case, through `PrunedSchemaReader`: the fixtures are
written to Parquet with a schema fitted to them together with filler resources, which populate the
elements a case removes and are left out, and each case is then run over the full schema (the
control) and over the schema without the element (the test).

The classes are in `fhirpath/src/test/java/au/csiro/pathling/`.

## Matrix

| Position kind               | Covered by                                                                                                                                                                                                                                                                                                         | Gap                                                                                                                                                                                                                                         |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Singular primitive          | `AbsentElementTest.traversalToAbsentElementIsEmpty`: `active` at the root, `name.family` under a repeating parent and `maritalStatus.text` under a singular one.                                                                                                                                                   | None.                                                                                                                                                                                                                                       |
| Repeating primitive         | `AbsentElementTest.traversalToAbsentElementIsEmpty`: `name.prefix`, and `contact.name.given` beneath an absent `contact.name`.                                                                                                                                                                                     | None.                                                                                                                                                                                                                                       |
| Singular complex element    | `AbsentElementTest.traversalToAbsentElementIsEmpty`: `managingOrganization` at the root and `name.period` under a repeating parent, with traversal into each. `AbsentReferenceTypeTest` (a Reference without `type`) and `AbsentCodingTerminologyTest` (a CodeableConcept without `coding`, or absent).            | None.                                                                                                                                                                                                                                       |
| Repeating complex element   | `AbsentElementTest.traversalToAbsentElementIsEmpty`: `telecom` at the root and `contact.name` under a repeating parent, with traversal into each. Recursion from an absent `item` and beside an absent `item.enableWhen` (`recursionFromAbsentRootIsEmpty`, `recursionBesideAbsentLevelCollectsThePopulatedOnes`). | None.                                                                                                                                                                                                                                       |
| Choice type                 | `AbsentElementTest.absentChoiceVariantIsEmpty`, `populatedVariantBesideAbsentVariants` and `choiceTypeBesideAbsentVariants`: primitive and complex variants, at the root and under a repeating parent, and the type of a choice beside absent variants.                                                            | None.                                                                                                                                                                                                                                       |
| Primitive metadata group    | `PrimitiveMetadataTest.dataStoredWithoutTheGroupsHasNoIdOrExtensions` (every group absent: `gender` at the root and `name.given` in a repeating element) and `MetadataOnlyPrimitiveSearchTest`. Neither runs a group absent beside a schema that carries it as a null.                                             | A group absent, against the same group present and unpopulated, inside a repeating complex element (`_family` and `_given` in `name`, and `_given` in `contact.name`) and inside a singular one (`_text` in `maritalStatus`).               |
| Annotation                  | `AnnotationDecidedBySchemaTest` (each kind absent, present, and present but null) and `AnnotationFreeSuiteTest` (the whole engine suite runs over data with every annotation absent), with the plan tests it names.                                                                                                | None.                                                                                                                                                                                                                                       |
| Nested                      | Every kind above is covered nested under a repeating parent, and the primitives under a singular one. `traversalToUndescribedElementIsEmptyOverPrunedSchema` covers traversal past an absent element.                                                                                                              | None.                                                                                                                                                                                                                                       |
| One side of a combination   | `AbsentElementTest.combinationWithAbsentElementSucceeds`: union, `combine`, `in`, `contains`, `=`, `>` and `!=`, with an absent primitive or an absent repeating complex element (`telecom`) on one side and a populated element on the other, in both orders.                                                     | None.                                                                                                                                                                                                                                       |
| Both sides of a combination | None. The only case with both sides unpopulated, `name.given contains name.family.first()`, has a populated left operand in `p1`.                                                                                                                                                                                  | A repeating complex element absent on both sides: the same element (`telecom`), a repeating and a singular one of one type (`address`, `contact.address`; `managingOrganization`, `generalPractitioner`), and values beneath them compared. |

## Tests added by T042

In `AbsentElementTest`, each run over the full schema as the control and over the pruned schema as
the test, as the class does for every case. The patient filler populates the new elements and
stores an id and an extension on each new metadata group, so that the full schema carries them
(HAPI encodes the id of a primitive only beside an extension).

- `combinationOfAbsentElementsSucceeds` and its control, over a schema without `telecom`,
  `address`, `contact.address`, `managingOrganization` and `generalPractitioner`.
- `absentMetadataGroupInsideRepeatingElementIsEmpty` and its control, over a schema without
  `name._family`, `name._given` and `contact.name._given`, which are groups in repeating complex
  elements, and `maritalStatus._text`, a group in a singular one. The values of
  `name.given` are populated, so the cases also show that the values are kept, and paired, where
  their group is absent.
- `prunedSchemaLacksTheMetadataGroups`, a control on the setup like `prunedSchemaLacksTheElement`,
  which also checks the new elements.

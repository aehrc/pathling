# FHIR terminology test fixtures

Synthetic FHIR R4 terminology resources used by the local terminology tests.
They exercise the FHIR importer, explicit value set evaluation, VCL evaluation
over an arbitrary code system, and concept map translation. None of the content
is derived from any real code system.

## Contents

`json/` holds the individual resources, used by the single-file and
directory-import tests:

| File                                  | Resource                                    | Exercises                                                                        |
| ------------------------------------- | ------------------------------------------- | -------------------------------------------------------------------------------- |
| `codesystem-animal-species.json`      | CodeSystem `.../animal-species` v1.0.0      | Nested (`is-a`) hierarchy, `integer`/`code`/`boolean` properties, a designation. |
| `valueset-mammals-enumerated.json`    | ValueSet                                    | Enumerated concepts.                                                             |
| `valueset-mammals-isa.json`           | ValueSet                                    | `is-a` filter.                                                                   |
| `valueset-animals-except-whale.json`  | ValueSet                                    | `is-a` include with a `concept` exclude.                                         |
| `valueset-land-dwellers.json`         | ValueSet                                    | Property (`=`) filter on a declared property.                                    |
| `valueset-nested-mammals.json`        | ValueSet                                    | Nested `valueSet` reference.                                                     |
| `valueset-expansion-only.json`        | ValueSet                                    | Expansion with no compose.                                                       |
| `conceptmap-species-to-category.json` | ConceptMap `.../species-to-category` v1.0.0 | Forward, reverse, and equivalence-filtered translation.                          |

`package/animals.tgz` is a FHIR NPM package (a `package/` directory with a
`package.json` and a subset of the resources above) used by the package-import
test.

## Content graph

The `animal-species` code system nests concepts to express its `is-a`
hierarchy:

```text
organism
└── animal
    ├── mammal
    │   ├── dog      legs=4, habitat=land, endangered=false (synonym "Canine")
    │   ├── cat      legs=4, habitat=land
    │   └── whale    legs=0, habitat=water, endangered=true
    └── bird
        ├── sparrow  legs=2, habitat=land
        └── penguin  legs=2, habitat=water
```

## Regenerating the package

The package is rebuilt from `json/` with:

```bash
cd terminology/src/test/resources/fhir-fixtures
mkdir -p build/package
cp json/codesystem-animal-species.json build/package/CodeSystem-animal-species.json
cp json/valueset-mammals-enumerated.json build/package/ValueSet-mammals-enumerated.json
cp json/valueset-mammals-isa.json build/package/ValueSet-mammals-isa.json
cp json/conceptmap-species-to-category.json build/package/ConceptMap-species-to-category.json
# plus a package/package.json declaring name, version, and fhirVersions
tar --format=ustar -C build -cf - package | gzip -n > package/animals.tgz
rm -rf build
```

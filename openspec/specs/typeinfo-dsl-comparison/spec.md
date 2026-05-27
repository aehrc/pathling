### Requirement: TypeInfoExpectation marker class

The system SHALL provide a `TypeInfoExpectation` class in `au.csiro.pathling.test.dsl` with fields `namespace`, `name`, and `baseType`. The class SHALL provide a static factory method `toTypeInfo(String repr)` that parses a string in the format `Namespace.Name(BaseType)` into its three constituent fields.

#### Scenario: Parse simple system type

- **WHEN** `toTypeInfo("System.Integer(System.Any)")` is called
- **THEN** the result has namespace `"System"`, name `"Integer"`, and baseType `"System.Any"`

#### Scenario: Parse FHIR type

- **WHEN** `toTypeInfo("FHIR.Patient(FHIR.Resource)")` is called
- **THEN** the result has namespace `"FHIR"`, name `"Patient"`, and baseType `"FHIR.Resource"`

#### Scenario: Parse FHIR element type

- **WHEN** `toTypeInfo("FHIR.boolean(FHIR.Element)")` is called
- **THEN** the result has namespace `"FHIR"`, name `"boolean"`, and baseType `"FHIR.Element"`

#### Scenario: Invalid format

- **WHEN** `toTypeInfo("InvalidFormat")` is called
- **THEN** an `IllegalArgumentException` SHALL be thrown

### Requirement: Executor TypeInfo comparison

The `DefaultYamlTestExecutor` SHALL support comparing `TypeInfoExpectation` instances against TypeInfo struct Rows. When the expected value is a `TypeInfoExpectation`, the executor SHALL extract the three string fields from the actual Row and compare them against the expectation's namespace, name, and baseType fields.

#### Scenario: Single TypeInfo equality

- **WHEN** a DSL test asserts `.testEquals(toTypeInfo("System.Integer(System.Any)"), "1.type()", "...")`
- **THEN** the executor compares the TypeInfo struct Row fields against the parsed expectation and the assertion passes

#### Scenario: Single TypeInfo inequality

- **WHEN** a DSL test asserts `.testEquals(toTypeInfo("System.String(System.Any)"), "1.type()", "...")`
- **THEN** the assertion fails because the actual type is `System.Integer`, not `System.String`

#### Scenario: List of TypeInfo equality

- **WHEN** a DSL test asserts `.testEquals(List.of(toTypeInfo("FHIR.Quantity(FHIR.Element)"), toTypeInfo("FHIR.string(FHIR.Element)")), "component.value.type()", "...")`
- **THEN** the executor compares each TypeInfo struct Row against the corresponding expectation and the assertion passes

### Requirement: Refactored TypeFunctionsDslTest assertions

Existing `TypeFunctionsDslTest` assertions that check namespace, name, and baseType individually SHALL be refactored to use `toTypeInfo()` for single-assertion comparison where all three fields are being tested for the same `type()` call.

#### Scenario: Primitive type assertion uses toTypeInfo

- **WHEN** testing `1.type()` result
- **THEN** the test uses `toTypeInfo("System.Integer(System.Any)")` instead of three separate field assertions

#### Scenario: FHIR type assertion uses toTypeInfo

- **WHEN** testing `Patient.active.type()` result
- **THEN** the test uses `toTypeInfo("FHIR.boolean(FHIR.Element)")` instead of three separate field assertions

#### Scenario: Choice type assertion uses toTypeInfo

- **WHEN** testing `component.value.type()` result for a choice field
- **THEN** the test uses a list of `toTypeInfo()` values instead of individual field assertions with indexing

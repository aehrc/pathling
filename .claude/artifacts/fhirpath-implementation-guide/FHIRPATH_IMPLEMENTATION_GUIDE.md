# FHIRPath Feature Implementation Guide

Use this guide when implementing new FHIRPath operators or functions in the Pathling codebase.

## Phase 1: Specification Review and Clarification

### Review FHIRPath Specifications
- [ ] Search `.local/specs/FHIRPath.md` for relevant sections using Grep
- [ ] Search `.local/specs/FHIR_FHIRpath.md` for FHIR-specific bindings if applicable
- [ ] Read specific sections with Read tool (use offset/limit for large files)
- [ ] Document the feature's semantics, parameters, return types, and behavior

### Clarify Ambiguities
- [ ] List any unclear semantics, edge cases, or specification ambiguities
- [ ] **Discuss with user to confirm interpretation before proceeding**
- [ ] Document any intentional deviations from or extensions to the spec

## Phase 2: Research Implementation Patterns

### Identify Feature Type
- [ ] Determine if implementing: operator, function, type system feature, or other

### Research Entry Points Based on Feature Type

**For Operators:**
- [ ] Review `fhirpath/parser/Visitor.java` for parser integration patterns
- [ ] Study `fhirpath/operator/FhirPathBinaryOperator.java` interface
- [ ] Examine existing operators in `fhirpath/operator/` package

**For Functions:**
- [ ] Review function providers in `fhirpath/function/provider/` package
- [ ] Study `@FhirPathFunction` annotation pattern
- [ ] Understand function registry integration

**For Type System Features:**
- [ ] Review `fhirpath/TypeSpecifier.java` for type specifications
- [ ] Study `Collection.isOfType()` and `filterByType()` methods
- [ ] Understand `FhirPathType` and `FHIRDefinedType` enums

**For HAPI FHIR Integration:**
- [ ] Review `FhirDefinitionContext` and `FhirContext` usage
- [ ] Study definition classes: `BaseFhirNodeDefinition`, `FhirResourceDefinition`
- [ ] Understand `org.hl7.fhir.r4.model.*` implementation classes

### Find Similar Implementations
- [ ] Use Grep to search for existing similar operators/functions
- [ ] Study existing test cases to understand patterns
- [ ] Review integration points (parser, collection methods, operators)

## Phase 3: Design and Test Planning

### Create Implementation Plan
- [ ] Design implementation approach based on research
- [ ] Identify all files to create or modify
- [ ] Plan integration strategy (parser, collection, operators, etc.)

### Plan DSL Test Cases

**Reference:** See `.claude/specs/DSL_Testing_Strategy.md` for the input domain
partitioning paradigm and dimensions table.

- [ ] **CRITICAL: Draft test cases and discuss with user before implementation**
- [ ] Identify which input domain dimensions are relevant to this feature
- [ ] Create a test matrix mapping dimensions to expressions and expected results
- [ ] Plan tests for basic functionality (core semantics from the spec)
- [ ] Plan tests for each relevant dimension (empty, cardinality, type, nesting)
- [ ] Plan combination tests only where the spec implies dimension interaction
- [ ] Plan at least one integration test with a real FHIR resource
- [ ] Identify error conditions and validation scenarios
- [ ] Expected results derived from the specification, not the implementation

## Phase 4: Implementation

### Implement Core Feature
- [ ] Follow existing code patterns discovered in research
- [ ] Implement operator/function class
- [ ] Integrate with parser if needed (for operators)
- [ ] Add or modify collection methods if needed
- [ ] Maintain consistent code style and naming conventions

### Plan Test Exclusions
- [ ] Identify known limitations that require YAML test exclusions
- [ ] Prepare exclusion documentation (type, justification, issue link)
- [ ] **Ask user for approval before adding exclusions to config.yaml**

## Phase 5: Testing and Validation

### Write DSL Tests
- [ ] Implement test cases previously discussed with user
- [ ] Create test file in `fhirpath/src/test/java/.../dsl/*DslTest.java`
- [ ] Cover all planned test scenarios
- [ ] Test basic functionality, edge cases, and error conditions

### Run and Verify Tests
- [ ] Run DSL tests: `mvn test -Dtest=YourTestClass`
- [ ] Run YAML reference tests: `mvn test -Dtest=YamlReferenceImplTest`
- [ ] Verify all DSL tests pass
- [ ] Verify YAML test exclusions work correctly

### Document Limitations and Future Work
- [ ] Create GitHub issues for identified future enhancements
- [ ] Add YAML test exclusions with proper documentation:
  - File: `fhirpath/src/test/resources/fhirpath-js/config.yaml`
  - Include type (feature, bug, wontfix), justification, and issue reference
- [ ] Document any known limitations or deviations from spec

## Key Architectural Concepts

### Collection-Based Evaluation
- All FHIRPath expressions operate on `Collection` objects
- Collections handle empty, singleton, and multi-item scenarios
- Type checking and filtering through collection methods

### Type System
- System namespace: `System.String`, `System.Boolean`, etc.
- FHIR namespace: `FHIR.Patient`, `FHIR.Observation`, etc.
- Type specifiers represent both namespaces

### Parser Integration
- ANTLR grammar defines syntax
- Visitor pattern converts parse tree to FhirPath objects
- Special visitors for type specifiers, identifiers, etc.

## Additional References

### Recent Implementation Examples

**`is` operator (Issue #2383, commit a8a77c865a)**
- Pattern: Special operator evaluation with `invokeWithPaths()`
- TypeSpecifier extraction at evaluation time
- Parser visitor for type expressions
- Files: `IsOperator.java`, `Visitor.java`, `TypeSpecifierVisitor.java`

### HAPI FHIR Integration Patterns

**Type Hierarchy Checking**
- Use HAPI implementing classes for inheritance checking
- Naming convention: Type name → `org.hl7.fhir.r4.model.{TypeName}`
- Class hierarchy checking: `Class.isAssignableFrom()` for subtype checks

### Testing Patterns

**DSL Test Builder Pattern**
- Test files: `*DslTest.java`
- Builder pattern for test construction
- Chainable test assertions

**YAML Test Exclusions**
- File: `fhirpath/src/test/resources/fhirpath-js/config.yaml`
- Document exclusion type, justification, and issue reference
- Integration with fhirpath.js reference implementation
# FHIRPath Feature Implementation - Interactive Template

**Feature Name:** [Enter feature name here]
**Feature Type:** [Operator / Function / Type System / Other]
**GitHub Issue:** [Issue number if applicable]

---

## Phase 1: Specification Review ✓

### FHIRPath Spec Review
- [ ] Searched `.local/specs/FHIRPath.md` for: _________________
- [ ] Searched `.local/specs/FHIR_FHIRpath.md` for: _________________
- [ ] Documented feature semantics: _________________

**Feature Behavior Summary:**
[Describe what the feature does, parameters, return type, and key behaviors]

### Ambiguities and Clarifications
**Specification Ambiguities:**
- [List any unclear points from the spec]

**User Clarifications Needed:**
- [Questions to discuss with user before proceeding]

**Decisions:**
- [Document agreed-upon interpretations and any deviations from spec]

---

## Phase 2: Research Implementation Patterns ✓

### Feature Type Classification
**Implementing:** [ ] Operator  [ ] Function  [ ] Type System  [ ] Other: __________

### Entry Points Reviewed

**For Operators (if applicable):**
- [ ] Reviewed `Visitor.java` patterns
- [ ] Studied `FhirPathBinaryOperator` interface
- [ ] Examined similar operators: _________________

**For Functions (if applicable):**
- [ ] Reviewed function provider patterns
- [ ] Studied `@FhirPathFunction` usage
- [ ] Examined similar functions: _________________

**For Type System (if applicable):**
- [ ] Reviewed `TypeSpecifier` class
- [ ] Studied `Collection.isOfType()` and `filterByType()`
- [ ] Examined type enums: _________________

**For HAPI Integration (if applicable):**
- [ ] Reviewed definition context patterns
- [ ] Studied implementation class usage
- [ ] Notes: _________________

### Similar Implementations Found
**Reference Implementations:**
1. [Name/file]: _________________
   Pattern: _________________
2. [Name/file]: _________________
   Pattern: _________________

**Key Patterns Identified:**
- [Pattern 1]: _________________
- [Pattern 2]: _________________

---

## Phase 3: Design and Test Planning ✓

### Implementation Design
**Files to Create:**
- [ ] _________________
- [ ] _________________

**Files to Modify:**
- [ ] _________________
- [ ] _________________

**Integration Strategy:**
[Describe how the feature integrates with parser, collections, operators, etc.]

### DSL Test Cases (Discuss with User First!)

**Reference:** See `.claude/specs/DSL_Testing_Strategy.md` for the input domain
partitioning paradigm.

#### Relevant Input Domain Dimensions

Mark which dimensions apply to this feature:

- [ ] Collection size (0, 1, n)
- [ ] Emptiness (`{}` literal, typed-empty field, absent element, zero-length array)
- [ ] Element type (primitive, complex/backbone, choice type)
- [ ] Cardinality (0..1 singular vs 0..* non-singular)
- [ ] Nesting (flat, nested, recursive)

#### Test Matrix

| Test case | Dimension(s) | Expression | Expected |
|-----------|-------------|------------|----------|
| _basic usage_ | Core semantics | _________ | _________ |
| _empty input_ | Collection size: 0 | _________ | _________ |
| _________ | _________ | _________ | _________ |

#### Error Condition Tests

| Error scenario | Expression | Expected error |
|---------------|------------|----------------|
| _________ | _________ | _________ |

**User Approval:** [ ] Test matrix reviewed and approved by user

---

## Phase 4: Implementation ✓

### Core Implementation
- [ ] Created/modified operator/function class: _________________
- [ ] Integrated with parser (if needed)
- [ ] Added/modified collection methods (if needed)
- [ ] Code follows existing patterns
- [ ] Code style and conventions maintained

### Known Limitations Identified
**Limitations requiring YAML exclusions:**
1. [Limitation]: _________________
   Type: feature / bug / wontfix
   Justification: _________________

2. [Limitation]: _________________
   Type: feature / bug / wontfix
   Justification: _________________

**User Approval for Exclusions:** [ ] User approved exclusion plan

---

## Phase 5: Testing and Validation ✓

### DSL Tests
- [ ] Created test file: `fhirpath/src/test/java/.../dsl/_________________DslTest.java`
- [ ] Implemented all approved test cases
- [ ] Tests cover basic functionality
- [ ] Tests cover edge cases
- [ ] Tests cover error conditions

### Test Execution
**DSL Tests:**
- [ ] Ran: `mvn test -Dtest=_________________DslTest`
- [ ] Result: ___ passed, ___ failed
- [ ] All tests passing: Yes / No

**YAML Reference Tests:**
- [ ] Ran: `mvn test -Dtest=YamlReferenceImplTest`
- [ ] Result: Tests run: ___, Failures: ___, Errors: ___, Skipped: ___
- [ ] Exclusions working correctly: Yes / No

### Documentation and Limitations
- [ ] Created GitHub issues for future enhancements: _________________
- [ ] Added YAML test exclusions to `config.yaml` with:
  - [ ] Exclusion type documented
  - [ ] Clear justification provided
  - [ ] GitHub issue reference included
- [ ] Documented known limitations: _________________

---

## Implementation Complete ✓

**Feature Status:** [ ] Complete  [ ] Partial (document what remains)

**Files Changed:**
- Created: _________________
- Modified: _________________
- Tests: _________________

**GitHub References:**
- Implementation Issue: _________________
- Future Enhancement Issues: _________________

**Notes:**
[Any additional notes, lessons learned, or important considerations for future work]
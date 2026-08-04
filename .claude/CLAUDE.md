## Repository Structure

This repository contains **two independent projects** that share some common ground:

### 1. Pathling Library (Main Project)

**Root**: `pom.xml` (version `9.3.0-SNAPSHOT`)
**Purpose**: A set of tools for using FHIR® within data analytics, built on Apache Spark

#### Module Dependency Hierarchy

```
utilities (foundation layer)
    ↓
encoders (depends on utilities)
    ↓
terminology (depends on encoders, utilities)
    ↓
fhirpath (depends on terminology)
    ↓
library-api (depends on encoders, terminology, fhirpath, utilities)
    ↓
library-runtime (shaded uber-jar of library-api)
    ↓
├── lib/python (Python API wrapping library-runtime)
└── lib/R (R API wrapping library-runtime)
```

#### Module Descriptions

| Module | Description |
|--------|-------------|
| `utilities` | Common utility functions, Spark helpers, versioning |
| `encoders` | FHIR to Spark Dataset encoders (originally from Bunsen) |
| `terminology` | Interact with FHIR terminology servers, includes caching (Infinispan) |
| `fhirpath` | Translates FHIRPath expressions into Spark queries (ANTLR-based parser) |
| `library-api` | High-level API exposing Pathling functionality |
| `library-runtime` | Shaded JAR bundling library-api with dependencies (published to Maven Central) |
| `lib/python` | Python API (published to PyPI) |
| `lib/R` | R API (published to CRAN) |
| `site` | Documentation website |
| `benchmark` | Performance benchmarks (JMH) |
| `sof-benchmark` | SQL-on-FHIR benchmark runner (Java + Python) over the Pathling engine |

### 2. Pathling Server (Independent Project)

**Root**: `server/pom.xml` (version `1.0.0-SNAPSHOT`)
**Purpose**: A FHIR server implementation supporting data import, export, and bulk operations

#### Key Characteristics

- **Standalone Maven project** (NOT a child of the main pathling parent)
- **Depends on `library-runtime`** from the main Pathling project (version `9.3.0-SNAPSHOT`)
- Built with **Spring Boot 3.5.9**
- Includes a **React admin UI** (`ui/` directory, built with Bun/Vite)
- Packaged as a **Docker image** (published to `ghcr.io/aehrc/pathling`)

### 3. Supporting Directories (Non-Maven)

| Directory | Description |
|-----------|-------------|
| `ui/` | React admin interface (TypeScript, Vite, Playwright tests) |
| `deployment/` | Helm charts for Kubernetes deployment |
| `test-data/` | Test data resources |
| `sql-on-fhir/` | Git submodule for SQL-on-FHIR specification tests |

### Key Integration Points

1. **Server → Library**: Server depends on `library-runtime` JAR
2. **Server → UI**: Server builds and bundles the React UI during `prepare-package` phase
3. **Python/R → Library**: Language bindings wrap `library-runtime` via PySpark/SparkR

---

## Important References

### FHIRPath Specification

The complete FHIRPath specification is available at `.claude/specs/FHIRPath.md`. This document should be consulted when:
- Implementing FHIRPath operators and functions
- Resolving questions about FHIRPath semantics and behavior
- Verifying correct interpretation of the FHIRPath type system
- Understanding FHIRPath grammar and expression evaluation rules

### FHIR-Specific FHIRPath Binding

The FHIR-specific extensions and bindings for FHIRPath are documented in `.claude/specs/FHIR_FHIRpath.md`. This document should be consulted when:
- Understanding how FHIRPath works with FHIR resources and data types
- Implementing FHIR-specific functions (e.g., `getValue()`, `hasValue()`, `resolve()`, `extension()`)
- Handling FHIR primitive type conversions and mappings to FHIRPath types
- Working with FHIR Quantity conversions to FHIRPath System.Quantity
- Understanding FHIR-specific variables (`%resource`, `%rootResource`)
- Implementing FHIR-specific operators and equivalence rules

### DSL Testing Strategy

The input domain partitioning strategy for FHIRPath DSL tests is documented in `.claude/specs/DSL_Testing_Strategy.md`. Consult this when:
- Designing test cases for new FHIRPath functions or operators
- Creating test matrices in OpenSpec change specs
- Understanding which input dimensions to test and why

### FHIR Search Specification

The FHIR search API specification is documented in `.claude/specs/FHIR_search.md`. This document should be consulted when:
- Understanding FHIR RESTful search operations
- Implementing search parameter types (string, token, reference, date, quantity, etc.)
- Working with search prefixes (eq, ne, gt, lt, ge, le, sa, eb, ap) for ordered types
- Implementing search modifiers (:exact, :contains, :missing, :text, :above, :below, etc.)
- Understanding chaining, reverse chaining (_has), and includes (_include, _revinclude)
- Handling composite search parameters

### FHIR Search Implementation Design

The design document for the FHIR Search API implementation is at `.claude/fhir-search-design.md`. This document covers:
- Architecture and component responsibilities
- Testing strategy (unit tests for matchers vs integration tests)
- Key design decisions (date range matching, modifier handling, `:not` semantics)
- Supported parameter types and modifiers

### FHIR Search Parameters Registry

The formal search parameters registry is available at `.claude/specs/search-parameters.json`. This is a FHIR Bundle containing all standard SearchParameter definitions. Use this to:
- Look up search parameter definitions by resource type
- Find the FHIRPath expression for a search parameter
- Determine the type (string, token, reference, etc.) of a search parameter
- Identify which modifiers are supported for a parameter

**Querying the registry with jq:**
```bash
# Find all string search parameters for Patient
cat .claude/specs/search-parameters.json | jq '.entry[].resource | select(.base[]? == "Patient" and .type == "string") | {code, expression, description}'

# Find a specific search parameter by code
cat .claude/specs/search-parameters.json | jq '.entry[].resource | select(.code == "name") | {code, base, type, expression}'
```

### Searching Large Specification Files

**IMPORTANT:** The specification files `.claude/specs/FHIRPath.md` and `.claude/specs/FHIR_FHIRpath.md` are very large (hundreds of KB) and should NOT be loaded entirely into LLM context.

**Use grep or similar search tools to locate relevant sections:**
- Use the `Grep` tool to search for specific terms, concepts, or patterns
- Search for section headers, function names, operator definitions, or keywords
- Once you've identified the relevant section location, use `Read` tool with line ranges to read only that portion
- Combine multiple targeted searches rather than trying to load the entire file

**Example:**

To find information about the 'where' operator in FHIRPath:
1. Use Grep: pattern="where" path=".claude/specs/FHIRPath.md"
2. Identify line numbers from grep results
3. Use Read to load just that section with content

### Finding Implementations in External Libraries

When you need to find the implementation of a class, trait, interface, or other code element in external dependencies (not in the local codebase), use this two-step discovery pattern:

**Step 1: Search GitHub for the Definition**

Use the `mcp__github__search_code` tool to locate the code:
- Search for the definition pattern: `class ClassName`, `trait TraitName`, `interface InterfaceName`, etc.
- Use GitHub's code search syntax to filter results:
    - By language: `language:Scala`, `language:Java`, `language:Python`
    - By repository: `repo:apache/spark`, `repo:scala/scala`
    - By organization: `org:apache`, `org:scala`
- Use exact code patterns (what would appear in the file), not keywords

**Step 2: Retrieve Full Implementation**

Once you have the GitHub URL from search results:
- Use the `WebFetch` tool with the discovered URL
- Request the complete implementation: prompt like "Show the complete implementation of this file" or "Extract the definition of ClassName"
- Present the full code to the user with context about its location

**Example Workflow**

```
User asks: "What file is AgnosticExpressionPathEncoder implemented in?"

You should:
1. Use mcp__github__search_code with:
   - query: "trait AgnosticExpressionPathEncoder repo:apache/spark language:Scala"

2. Get URL from results:
   https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/encoders/EncoderUtils.scala

3. Use WebFetch with that URL:
   - url: (the GitHub URL)
   - prompt: "Show the complete implementation of AgnosticExpressionPathEncoder"

4. Present the full trait definition to the user with explanation of its purpose and location
```

**When to Use This Pattern**

- User asks "Where is X implemented?" or "What file contains X?"
- User requests "Show me the implementation of X" for external code
- You need to reference a specific API from a dependency but don't know its exact location
- You want to demonstrate how an external library implements a feature

**When NOT to Use This Pattern**

- The code is in the local codebase (use Grep and Read tools instead)
- You already have the exact GitHub URL (skip to WebFetch)
- The code is in documentation rather than source files (use WebFetch directly on docs)

**Common Repositories**

- Apache Spark: `apache/spark`
- Scala Standard Library: `scala/scala`
- OpenJDK: `openjdk/jdk`
- Popular frameworks: `spring-projects/spring-framework`, `akka/akka`, etc.

**Benefits of This Pattern**

- Eliminates guesswork about file locations
- Provides accurate, up-to-date source code
- Demonstrates proper tool usage to users
- Works across different versions and branches
- Avoids wasting time searching local files for external dependencies

## Coding Guidelines

### Functional Programming Style

Prefer functional programming patterns with `Optional` and `Stream` APIs over imperative if-else chains. This promotes clean, composable code that leverages Java's functional paradigms.

#### Optional Handling

**Prefer functional methods:**
```java
// ✅ Good: Use map/filter/flatMap chains
return optional
    .map(value -> transform(value))
    .filter(v -> isValid(v))
    .orElse(defaultValue);

// ✅ Good: Use ofNullable for null-safe operations
return Optional.ofNullable(possiblyNull)
    .map(value -> process(value))
    .orElseThrow();

// ✅ Good: Chain multiple Optionals
return optionalA
    .flatMap(a -> optionalB.map(b -> combine(a, b)))
    .orElseGet(() -> createDefault());
```

**Avoid when functional style applies:**
```java
// ❌ Avoid: Explicit null checks
if (value != null) {
  return process(value);
} else {
  return default;
}

// ❌ Avoid: isPresent() with get()
if (optional.isPresent()) {
  return optional.get();
}
```

#### Collections and Streams

**Prefer streams for transformations:**
```java
// ✅ Good: Use streams for filtering/mapping
return collection.stream()
    .filter(item -> item.isActive())
    .map(this::transform)
    .collect(Collectors.toList());

// ✅ Good: Use forEach for side effects
items.forEach(item -> process(item));
```

**Avoid imperative loops:**
```java
// ❌ Avoid: Manual loops with conditions
List<String> results = new ArrayList<>();
for (Item item : collection) {
  if (item.isValid()) {
    results.add(item.getName());
  }
}
```

#### Readability Boundaries

Keep functional chains **readable by breaking after 3-4 chained methods:**
```java
// ✅ Good: Breaks chain for clarity
final Column valueMatch = matchValue(element.getField(VALUE_FIELD),
    parsedValue.getNumericValue(), parsedValue.getPrefix());

return parsedValue.getSystem()
    .map(system -> matchWithSystem(element, system, parsedValue, valueMatch))
    .orElseGet(() -> parsedValue.getCode()
        .map(code -> valueMatch.and(matchCodeOrUnit(element, code)))
        .orElse(valueMatch));

// ❌ Avoid: Overly long chains
return opt1.flatMap(a -> opt2.map(b -> opt3.filter(c ->
    opt4.flatMap(d -> createResult(a,b,c,d)).orElse(null)).orElse(null))).orElse(default);
```

#### Lambda and Method References

**Prefer method references for clarity:**
```java
// ✅ Good: Use method references
return values.stream()
    .map(String::toUpperCase)
    .collect(Collectors.toList());

// ✅ Good: Method reference for filtering
return system.filter(UcumUnit.UCUM_SYSTEM_URI::equals);
```

---

## Implementing New FHIRPath Features

### Implementation Process

#### 1. Specification Review and Clarification

**Review FHIRPath Specifications:**
- Search `.claude/specs/FHIRPath.md` for relevant sections using Grep
- Search `.claude/specs/FHIR_FHIRpath.md` for FHIR-specific bindings
- Read specific sections with Read tool (use offset/limit for large files)

**Clarify Ambiguities:**
- Document any unclear semantics, edge cases, or spec ambiguities
- **Always discuss with user before implementation** to confirm interpretation
- Note any deviations from or extensions to the specification

#### 2. Research Current Implementation Patterns

**Key Entry Points by Feature Type:**

**For Operators:**
- Parser grammar integration: `fhirpath/parser/Visitor.java`, ANTLR visitors
- Operator interface: `fhirpath/operator/FhirPathBinaryOperator.java`
- Existing operators: `fhirpath/operator/` package

**For Functions:**
- Function providers: `fhirpath/function/provider/` package
- Function annotation pattern: `@FhirPathFunction`
- Registry integration: Function discovery and validation

**For Type System Features:**
- Type specifications: `fhirpath/TypeSpecifier.java`
- Type checking: `Collection.isOfType()`, `filterByType()`
- Type enums: `FhirPathType`, `FHIRDefinedType`

**For HAPI FHIR Integration:**
- Context access: `FhirDefinitionContext`, `FhirContext`
- Definition classes: `BaseFhirNodeDefinition`, `FhirResourceDefinition`
- Implementation classes: `org.hl7.fhir.r4.model.*` package

**Search for Similar Implementations:**
- Use Grep to find existing similar operators/functions
- Study existing test cases for patterns
- Review integration points (parser, collection, operators)

#### 3. Design and Testing Planning

**Design Implementation:**
- Create implementation plan based on research
- Identify files to create/modify
- Plan integration approach (parser, collection methods, etc.)

**Plan DSL Test Cases:**
- **CRITICAL: Discuss test cases with user before implementation**
- Cover: basic functionality, type matching, edge cases, empty collections
- Plan integration tests with other FHIRPath features
- Identify error conditions and validation tests

#### 4. Implementation

**Follow Existing Patterns:**
- Implement operator/function following discovered patterns
- Integrate with parser if needed (for operators)
- Add collection methods if needed
- Maintain consistent code style and conventions

**Testing Integration:**
- Add YAML test exclusions for known limitations:
  - File: `fhirpath/src/test/resources/fhirpath-js/config.yaml`
  - **IMPORTANT**: Always ask the user before adding new exclusions
  - Document exclusion type (feature, bug, wontfix) and justification
  - Link to GitHub issues for tracking

#### 5. Testing and Validation

**Write DSL Tests:**
- Implement test cases discussed with user
- File location: `fhirpath/src/test/java/.../dsl/*DslTest.java`
- Cover all planned test scenarios

**Run Tests:**
- Run DSL tests: `mvn test -Dtest=YourTestClass`
- Run YAML reference tests: `mvn test -Dtest=YamlReferenceImplTest`
- Verify test exclusions work correctly

**Document Limitations:**
- Create GitHub issues for future enhancements
- Document any known limitations or deviations
- Add exclusions with proper references

### Key Architectural Concepts

**Collection-Based Evaluation:**
- All FHIRPath expressions operate on `Collection` objects
- Collections handle empty, singleton, and multi-item scenarios
- Type checking and filtering through collection methods

**Type System:**
- System namespace: `System.String`, `System.Boolean`, etc.
- FHIR namespace: `FHIR.Patient`, `FHIR.Observation`, etc.
- Type specifiers represent both namespaces

**Parser Integration:**
- ANTLR grammar defines syntax
- Visitor pattern converts parse tree to FhirPath objects
- Special visitors for type specifiers, identifiers, etc.

### Additional References

**Recent Examples:**
- `is` operator implementation: Issue #2383, commit a8a77c865a
  - Pattern: Special operator evaluation with `invokeWithPaths()`
  - TypeSpecifier extraction at evaluation time
  - Parser visitor for type expressions

**HAPI FHIR Type Hierarchy:**
- Use HAPI implementing classes for inheritance checking
- Naming convention: Type name → `org.hl7.fhir.r4.model.{TypeName}`
- Class hierarchy: `Class.isAssignableFrom()` for subtype checks

**Testing Patterns:**
- DSL test builder pattern in `*DslTest.java` files
- YAML exclusion format in `config.yaml`
- Integration with fhirpath.js reference implementation

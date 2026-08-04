# FHIR Search API Implementation Design

## Overview

This document describes the design decisions and testing strategy for the FHIR Search API implementation in the `fhirpath` module.

**Related Issue:** #1986

## Architecture

### Core Components

```
FhirSearchExecutor
    ├── SearchParameterRegistry  (parameter definitions)
    ├── Parser                   (FHIRPath evaluation)
    └── SearchFilter             (filter building)
            └── ElementMatcher   (matching logic)
```

### Key Classes

| Class | Responsibility |
|-------|----------------|
| `FhirSearchExecutor` | Orchestrates search execution, routes to appropriate matchers |
| `SearchParameterRegistry` | Stores parameter definitions (code, type, FHIRPath expression) |
| `SearchFilter` | Builds SparkSQL filter expressions, handles negation for `:not` |
| `ElementMatcher` | Interface for type-specific matching logic |
| `TokenMatcher` | Exact equality matching for token parameters |
| `StringMatcher` | Case-insensitive prefix matching for string parameters |
| `ExactStringMatcher` | Case-sensitive exact matching for `:exact` modifier |
| `DateMatcher` | Range-based overlap matching for date parameters |

## Testing Strategy

### Unit Tests (`ElementMatcherTest`)

**Purpose:** Exhaustive testing of matching logic in isolation.

**Approach:**
- Use simple Spark DataFrames with string values (no FHIR resources)
- Parameterized tests covering all edge cases
- Fast execution, focused on matcher correctness

**What to test:**
- All precision levels (for date matching)
- Case sensitivity variations
- Boundary conditions
- Empty/null handling

**Example test cases for DateMatcher:**
```java
// Same precision
("2013-01-14", "2013-01-14", true)
// Coarser precision (month, year)
("2013-01-14", "2013-01", true)
("2013-01-14", "2013", true)
// Finer precision (datetime)
("2013-01-14", "2013-01-14T10:00", true)
```

### Integration Tests (`FhirSearchExecutorTest`)

**Purpose:** Verify end-to-end search functionality with actual FHIR resources.

**Approach:**
- Use `ObjectDataSource` with FHIR model objects
- Test basic scenarios only (match, no match, multiple values)
- Verify correct resource filtering and schema preservation

**What to test:**
- Basic matching and non-matching cases
- Multiple search values (OR logic)
- One representative case per precision type (if relevant)
- Invalid modifier handling
- Unknown parameter handling

**Principle:** Avoid duplicating exhaustive precision/edge-case testing from unit tests.

## Key Design Decisions

## Context

The fhirpath-lab-api endpoint (`POST /fhir/$fhirpath`) currently passes all
expressions directly to the Pathling engine. When the expression is empty or
whitespace-only, the engine throws a parse error which surfaces as a 500
OperationOutcome. The fix is confined to the Python API layer — no upstream
library changes.

## Goals / Non-goals

**Goals:**

- Return a successful empty-collection response when the expression is blank.
- Keep the change minimal and confined to `fhirpath-lab-api`.

**Non-goals:**

- Changing validation in the upstream Pathling Java library.
- Handling other edge cases (e.g. expressions that are syntactically valid but
  semantically meaningless).

## Decisions

**Early return in the request handler.** After parsing input parameters, check
whether the expression is empty or whitespace-only. If so, short-circuit before
calling the Pathling engine and return a Parameters response with zero result
parts and no type metadata.

_Alternative considered:_ Treating empty expression as an input validation
error (400 response). Rejected because an empty expression naturally evaluates
to an empty collection in FHIRPath semantics, and a successful empty response
is more useful to interactive tooling like FHIRPath Lab.

## Risks / Trade-offs

- **Deviation from engine behaviour** — The Pathling engine would reject an
  empty expression. By short-circuiting we define API-level semantics that
  differ from the engine. This is acceptable because the API is the user-facing
  contract and the engine is an implementation detail.

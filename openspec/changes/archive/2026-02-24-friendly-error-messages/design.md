## Context

When `PathlingContext.evaluate_fhirpath()` fails, the Python Py4J bridge wraps
the Java exception in a `Py4JJavaError`. Calling `str()` on this produces a
message like:

```
An error occurred while calling o84.evaluateFhirPath.
: au.csiro.pathling.errors.UnsupportedFhirPathFeatureError: Unsupported function: trace
	at au.csiro.pathling.fhirpath.path.Paths$EvalFunction.apply(Paths.java:190)
	at au.csiro.pathling.fhirpath.FhirPath$Composite.lambda$apply$0(FhirPath.java:179)
	... (20+ frames)
```

Currently, the entire string is passed as `details.text` in the
OperationOutcome. The `diagnostics` field — designed for this purpose — is
unused.

## Goals / Non-Goals

**Goals:**

- Return a concise, human-readable error message in `details.text`.
- Preserve the full exception (including stack trace) in `diagnostics`.

**Non-Goals:**

- Changing error handling in the Pathling core libraries or Java code.
- Categorising errors into different HTTP status codes (all evaluation errors
  remain 500).

## Decisions

### Extract Java exception class and message from Py4JJavaError

The first line of a `Py4JJavaError`'s Java exception string follows the
pattern:

```
: fully.qualified.ClassName: Human readable message
```

We will extract the short class name and message (e.g.
"UnsupportedFhirPathFeatureError: Unsupported function: trace") using a regex
on the string representation.

**Alternative considered:** Using `e.java_exception.toString()` via the Py4J
bridge. Rejected because it requires a live JVM round-trip and the string is
already available.

### Non-Py4J exceptions pass through unaltered

For any exception that is not a `Py4JJavaError`, `str(e)` is used as the
message directly, with no diagnostics. These exceptions are typically from the
Python layer and are already concise.

## Risks / Trade-offs

- **Regex fragility**: If Py4J changes its error string format, the regex may
  fail to extract a clean message. Mitigation: fall back to the full string as
  the message if the regex does not match.

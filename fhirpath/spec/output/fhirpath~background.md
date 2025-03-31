## Background

In Information Systems in general, and Healthcare Information Systems in
particular, the need for formal representation of logic is both
pervasive and critical. From low-level technical specifications, through
intermediate logical architectures, up to the high-level conceptual
descriptions of requirements and behavior, the ability to formally
represent knowledge in terms of expressions and information models is
essential to the specification and implementation of these systems.

### Requirements

Of particular importance is the ability to easily and precisely express
conditions of basic logic, such as those found in requirements
constraints (e.g. Patients must have a name), decision support (e.g. if
the patient has diabetes and has not had a recent comprehensive foot
exam), cohort definitions (e.g. All male patients aged 60-75), protocol
descriptions (e.g. if the specimen has tested positive for the presence
of sodium), and numerous other environments.

Precisely because the need for such expressions is so pervasive, there
is no shortage of existing languages for representing them. However,
these languages tend to be tightly coupled to the data structures, and
even the information models on which they operate, XPath being a typical
example. To ensure that the knowledge captured by the representation of
these expressions can survive technological drift, a representation that
can be used independent of any underlying physical implementation is
required.

Languages meeting these additional requirements also exist, such as
Object Constraint Language (OCL), Java, JavaScript, C#, and others.
However, these languages are both tightly coupled to the platforms in
which they operate, and, because they are general-purpose development
languages, come with much heavier tooling and technology dependencies
than is warranted or desirable. Even constraining one of these grammars
would be insufficient, resulting in the need to extend, defeating the
purpose of basing it on an existing language in the first place.

Given these constraints, and the lack of a specific language that meets
all of these requirements, there is a need for a simple, lightweight,
platform- and structure-independent graph traversal language. FHIRPath
meets these requirements, and can be used within various environments to
provide for simple but effective formal representation of expressions.

### Features

-   Graph-traversal: FHIRPath is a graph-traversal language; authors can
    clearly and concisely express graph traversal on hierarchical
    information models (e.g. Health Level 7 - Version 3 (HL7 V3), Fast
    Healthcare Interoperability Resources (FHIR), virtual Medical Record
    (vMR), Clinical Information Modeling Initiative (CIMI), and Quality
    Data Model (QDM)).
-   Fluent: FHIRPath has a syntax based on the [Fluent
    Interface](https://en.wikipedia.org/wiki/Fluent_interface) pattern
-   Collection-centric: FHIRPath deals with all values as collections,
    allowing it to easily deal with information models with repeating
    elements.
-   Platform-independent: FHIRPath is a conceptual and logical
    specification that can be implemented in any platform.
-   Model-independent: FHIRPath deals with data as an abstract model,
    allowing it to be used with any information model.

### Usage

In Fast Healthcare Interoperability Resources
([FHIR](http://hl7.org/fhir)), FHIRPath is used within the specification
to provide formal definitions for conditions such as validation
invariants, search parameter paths, etc. Within Clinical Quality
Language ([CQL](http://cql.hl7.org)), FHIRPath is used to simplify
graph-traversal for hierarchical information models.

In both FHIR and CQL, the model independence of FHIRPath means that
expressions can be written that deal with the contents of the resources
and data types as described in the Logical views, or the UML diagrams,
rather than against the physical representation of those resources. JSON
and XML specific features are not visible to the FHIRPath language (such
as comments and the split representation of primitives (i.e.
`value[x]`)).

The expressions can in theory be converted to equivalent expressions in
XPath, OCL, or another similarly expressive language.

FHIRPath can be used against many other graphs as well. For example,
[Use of FHIRPath on HL7 Version 2 messages](#hl7v2) describes how
FHIRPath is used in HL7 V2.

### Conventions

Throughout this documentation, `monospace font` is used to delineate expressions of FHIRPath.

Optional parameters to functions are enclosed in square brackets in the
definition of a function. Note that the brackets are only used to
indicate optionality in the signature, they are not part of the actual
syntax of FHIRPath.

All operations and functions return a collection, but if the operation
or function will always produce a collection containing a single item of
a predefined type, the description of the operation or function will
specify its output type explicitly, instead of just stating
`collection`, e.g.
`all(...) : Boolean`

Throughout this specification, formatting patterns for Date, Time, and
DateTime values are described using an informal description with the
following markers:

-   **YYYY** - A full four digit year (0001..9999), padded with leading
    zeroes if necessary
-   **MM** - A full two digit month value (01..12), padded with leading
    zeroes if necessary
-   **DD** - A full two digit day value (01..31), padded with leading
    zeroes if necessary
-   **hh** - A full two digit hour value (00..24), padded with leading
    zeroes if necessary
-   **mm** - A full two digit minute value (00..59), padded with leading
    zeroes if necessary
-   **ss** - A full two digit second value (00..59), padded with leading
    zeroes if necessary
-   **fff** - A fractional millisecond value (0..999)

These formatting patterns are set in **bold** to distinguish them
typographically from literals or code and to make clear that they are
not intended to be formally interpreted as regex patterns.

#### Conformance Language

This specification uses the conformance verbs SHALL, MUST, SHOULD, and
MAY as defined in [RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).
Unlike RFC 2119, however, this specification allows that different
applications might not be able to interoperate because of how they use
optional features. In particular:

-   SHALL/MUST: An absolute requirement for all implementations
-   SHALL/MUST NOT: An absolute prohibition against inclusion for all
    implementations
-   SHOULD/SHOULD NOT: A best practice or recommendation to be
    considered by implementers within the context of their particular
    implementation; there may be valid reasons to ignore an item, but
    the full implications must be understood and carefully weighed
    before choosing a different course
-   MAY: This is truly optional language for an implementation; can be
    included or omitted as the implementer decides with no implications.
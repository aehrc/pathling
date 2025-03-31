::: {#segment-header .segment}
::: container
::: {#hl7-nav}
[![Visit the HL7
website](assets/images/hl7-logo-header.png){height="50"}](http://hl7.org){#hl7-logo
no-external="true"}
:::

::: {#ig-status}
[FHIRPath Specification]{style="font-size:12pt;font-weight:bold"}\
[3.0.0-ballot - R2 STU1 ![International
flag](assets/images/001.svg "International"){height="16"}
]{style="display:inline-block;"}
:::
:::
:::

::: {#segment-navbar .segment}
::: {#stripe}
:::

::: container
::: container
[ ]{.icon-bar} [ ]{.icon-bar} [ ]{.icon-bar}

[FHIR](http://hl7.org/fhir/R4/index.html){.navbar-brand .hidden}

::: {.nav-collapse .collapse .navbar-inverse-collapse}
-   [Documentation](index.html)
-   [Grammar](grammar.html)
-   [Tests](tests.html)
-   [Version History](http://hl7.org/fhirpath/history.html)
:::
:::
:::
:::

[]{#top}

::: {#segment-content .segment}
::: container
::: row
::: inner-wrapper
::: col-12
FHIRPath Specification, published by Implementable Technology
Specifications WG. This guide is not an authorized publication; it is
the continuous build for version 3.0.0-ballot built by the FHIR (HL7®
FHIR® Standard) CI Build. This version is based on the current content
of <https://github.com/HL7/FHIRPath/> and changes regularly. See the
[Directory of published versions](http://hl7.org/fhirpath/history.html)

::: markdown-toc
-   [FHIRPath (Continuous
    Build)](#fhirpath-continuous-build){#markdown-toc-fhirpath-continuous-build}
    -   [Background](#background){#markdown-toc-background}
        -   [Requirements](#requirements){#markdown-toc-requirements}
        -   [Features](#features){#markdown-toc-features}
        -   [Usage](#usage){#markdown-toc-usage}
        -   [Conventions](#conventions){#markdown-toc-conventions}
    -   [Navigation
        model](#navigation-model){#markdown-toc-navigation-model}
    -   [Path selection](#path-selection){#markdown-toc-path-selection}
        -   [Collections](#collections){#markdown-toc-collections}
        -   [Paths and polymorphic
            items](#paths-and-polymorphic-items){#markdown-toc-paths-and-polymorphic-items}
    -   [Expressions](#expressions){#markdown-toc-expressions}
        -   [Literals](#literals){#markdown-toc-literals}
        -   [Operators](#operators){#markdown-toc-operators}
        -   [Function
            Invocations](#function-invocations){#markdown-toc-function-invocations}
        -   [Null and
            empty](#null-and-empty){#markdown-toc-null-and-empty}
        -   [Singleton Evaluation of
            Collections](#singleton-evaluation-of-collections){#markdown-toc-singleton-evaluation-of-collections}
    -   [Functions](#functions){#markdown-toc-functions}
        -   [Existence](#existence){#markdown-toc-existence}
        -   [Filtering and
            projection](#filtering-and-projection){#markdown-toc-filtering-and-projection}
        -   [Subsetting](#subsetting){#markdown-toc-subsetting}
        -   [Combining](#combining){#markdown-toc-combining}
        -   [Conversion](#conversion){#markdown-toc-conversion}
        -   [String
            Manipulation](#string-manipulation){#markdown-toc-string-manipulation}
        -   [Additional String
            Functions](#additional-string-functions){#markdown-toc-additional-string-functions}
        -   [Math](#math){#markdown-toc-math}
        -   [Tree
            navigation](#tree-navigation){#markdown-toc-tree-navigation}
        -   [Utility
            functions](#utility-functions){#markdown-toc-utility-functions}
    -   [Operations](#operations){#markdown-toc-operations}
        -   [Equality](#equality){#markdown-toc-equality}
        -   [Comparison](#comparison){#markdown-toc-comparison}
        -   [Types](#types){#markdown-toc-types}
        -   [Collections](#collections-1){#markdown-toc-collections-1}
        -   [Boolean logic](#boolean-logic){#markdown-toc-boolean-logic}
        -   [Math](#math-1){#markdown-toc-math-1}
        -   [Date/Time
            Arithmetic](#datetime-arithmetic){#markdown-toc-datetime-arithmetic}
        -   [Operator
            precedence](#operator-precedence){#markdown-toc-operator-precedence}
    -   [Aggregates](#aggregates){#markdown-toc-aggregates}
        -   [aggregate(aggregator : expression \[, init : value\]) :
            value](#aggregateaggregator--expression--init--value--value){#markdown-toc-aggregateaggregator--expression--init--value--value}
    -   [Lexical
        Elements](#lexical-elements){#markdown-toc-lexical-elements}
        -   [Whitespace](#whitespace){#markdown-toc-whitespace}
        -   [Comments](#comments){#markdown-toc-comments}
        -   [Literals](#literals-1){#markdown-toc-literals-1}
        -   [Symbols](#symbols){#markdown-toc-symbols}
        -   [Keywords](#keywords){#markdown-toc-keywords}
        -   [Identifiers](#identifiers){#markdown-toc-identifiers}
        -   [Case-Sensitivity](#case-sensitivity){#markdown-toc-case-sensitivity}
    -   [Environment
        variables](#environment-variables){#markdown-toc-environment-variables}
    -   [Types and
        Reflection](#types-and-reflection){#markdown-toc-types-and-reflection}
        -   [Models](#models){#markdown-toc-models}
        -   [Reflection](#reflection){#markdown-toc-reflection}
    -   [Type safety and strict
        evaluation](#type-safety-and-strict-evaluation){#markdown-toc-type-safety-and-strict-evaluation}
    -   [Formal
        Specifications](#formal-specifications){#markdown-toc-formal-specifications}
        -   [Formal Syntax](#formal-syntax){#markdown-toc-formal-syntax}
        -   [Model
            Information](#model-information){#markdown-toc-model-information}
        -   [URI and Media
            Types](#uri-and-media-types){#markdown-toc-uri-and-media-types}
    -   [Use of FHIRPath on HL7 Version 2
        messages](#use-of-fhirpath-on-hl7-version-2-messages){#markdown-toc-use-of-fhirpath-on-hl7-version-2-messages}
    -   [FHIRPath Tooling and
        Implementation](#fhirpath-tooling-and-implementation){#markdown-toc-fhirpath-tooling-and-implementation}
    -   [References](#references){#markdown-toc-references}
    -   [Dependencies](#dependencies){#markdown-toc-dependencies}
    -   [Cross Version
        Analysis](#cross-version-analysis){#markdown-toc-cross-version-analysis}
    -   [Global
        Profiles](#global-profiles){#markdown-toc-global-profiles}
    -   [IP Statements](#ip-statements){#markdown-toc-ip-statements}
:::

# FHIRPath (Continuous Build)

FHIRPath is a path based navigation and extraction language, somewhat
like XPath. Operations are expressed in terms of the logical content of
hierarchical data models, and support traversal, selection and filtering
of data. Its design was influenced by the needs for path navigation,
selection and formulation of invariants in both HL7 Fast Healthcare
Interoperability Resources ([FHIR](http://hl7.org/fhir)) and HL7
Clinical Quality Language
([CQL](http://cql.hl7.org/03-developersguide.html#using-fhirpath)).

Looking for implementations? See [FHIRPath Implementations on the HL7
confluence](https://confluence.hl7.org/display/FHIRI/FHIRPath+Implementations){target="_blank"}

> **Note:** The following sections of this specification have not
> received significant implementation experience and are marked for
> Standard for Trial Use (STU):
>
> -   [Aggregates](#aggregates)
> -   [Literals - Long](#long)
> -   [Conversions - toLong](#tolong--long)
> -   [Functions - String
>     (lastIndexOf)](#lastindexofsubstring--string--integer)
> -   [Functions - String
>     (matchesFull)](#matchesfullregex--string--boolean)
> -   [Functions - String (trim, split, join)](#trim--string)
> -   [Functions - String (encode, decode, escape,
>     unescape)](#additional-string-functions)
> -   [Functions - Math](#math)
> -   [Functions - Utility (defineVariable, lowBoundary,
>     highBoundary)](#definevariable)
> -   [Functions - Utility (precision)](#precision--integer)
> -   [Functions - Extract Date/DateTime/Time
>     components](#extract-datedatetimetime-components)
> -   [Types - Reflection](#reflection)
>
> In addition, the appendices are included as additional documentation
> and are informative content.

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
`value[x]`{.language-plaintext .highlighter-rouge})).

The expressions can in theory be converted to equivalent expressions in
XPath, OCL, or another similarly expressive language.

FHIRPath can be used against many other graphs as well. For example,
[Use of FHIRPath on HL7 Version 2 messages](#hl7v2) describes how
FHIRPath is used in HL7 V2.

### Conventions

Throughout this documentation, `monospace font`{.language-plaintext
.highlighter-rouge} is used to delineate expressions of FHIRPath.

Optional parameters to functions are enclosed in square brackets in the
definition of a function. Note that the brackets are only used to
indicate optionality in the signature, they are not part of the actual
syntax of FHIRPath.

All operations and functions return a collection, but if the operation
or function will always produce a collection containing a single item of
a predefined type, the description of the operation or function will
specify its output type explicitly, instead of just stating
`collection`{.language-plaintext .highlighter-rouge}, e.g.
`all(...) : Boolean`{.language-plaintext .highlighter-rouge}

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

## Navigation model

FHIRPath navigates and selects nodes from a tree that abstracts away and
is independent of the actual underlying implementation of the source
against which the FHIRPath query is run. This way, FHIRPath can be used
on in-memory Plain Old Java Objects (POJOs), XML data or any other
physical representation, so long as that representation can be viewed as
classes that have properties. In somewhat more formal terms, FHIRPath
operates on a directed acyclic graph of classes as defined by a Meta
Object Facility (MOF)-equivalent [\[MOF\]](#MOF) type system. In this
specification, the structures on which FHIRPath operates are referred to
as the Object Model.

Data are represented as a tree of labelled nodes, where each node may
optionally carry a primitive value and have child nodes. Nodes need not
have a unique label, and leaf nodes must carry a primitive value. For
example, a (partial) representation of a FHIR Patient resource in this
model looks like this:

![Tree representation of a Patient](treestructure.png){height="375px"
width="500px" style="float: unset; margin-bottom:0;"}

The diagram shows a tree with a repeating `name`{.language-plaintext
.highlighter-rouge} node, which represents repeating members of the FHIR
Object Model. Leaf nodes such as `use`{.language-plaintext
.highlighter-rouge} and `family`{.language-plaintext .highlighter-rouge}
carry a (string) value. It is also possible for internal nodes to carry
a value, as is the case for the node labelled
`active`{.language-plaintext .highlighter-rouge}: this allows the tree
to represent FHIR \"primitives\", which may still have child extension
data.

FHIRPath expressions are then *evaluated* with respect to a specific
instance, such as the Patient one described above. This instance is
referred to as the *context* (also called the *root*) and paths within
the expression are evaluated in terms of this instance.

## Path selection

FHIRPath allows navigation through the tree by composing a path of
concatenated labels, e.g.

``` fhirpath
name.given
```

This would result in a collection of nodes, one with the value
`'Wouter'`{.language-plaintext .highlighter-rouge} and one with the
value `'Gert'`{.language-plaintext .highlighter-rouge}. In fact, each
step in such a path results in a collection of nodes by selecting nodes
with the given label from the step before it. The input collection at
the beginning of the evaluation contained all elements from Patient, and
the path `name`{.language-plaintext .highlighter-rouge} selected just
those named `name`{.language-plaintext .highlighter-rouge}. Since the
`name`{.language-plaintext .highlighter-rouge} element repeats, the next
step `given`{.language-plaintext .highlighter-rouge} along the path,
will contain all nodes labeled `given`{.language-plaintext
.highlighter-rouge} from all nodes `name`{.language-plaintext
.highlighter-rouge} in the preceding step.

The path may start with the type of the root node (which otherwise does
not have a name), but this is optional. To illustrate this point, the
path `name.given`{.language-plaintext .highlighter-rouge} above can be
evaluated as an expression on a set of data of any type. However the
expression may be prefixed with the name of the type of the root:

``` fhirpath
Patient.name.given
```

The two expressions have the same outcome, but when evaluating the
second, the evaluation will only produce results when used on data of
type `Patient`{.language-plaintext .highlighter-rouge}. When resolving
an identifier that is also the root of a FHIRPath expression, it is
resolved as a type name first, and if it resolves to a type, it must
resolve to the type of the context (or a supertype). Otherwise, it is
resolved as a path on the context. If the identifier cannot be resolved,
the evaluation will end and signal an error to the calling environment.

Syntactically, FHIRPath defines identifiers as any sequence of
characters consisting only of letters, digits, and underscores,
beginning with a letter or underscore. Paths may use backticks to
include characters in path parts that would otherwise be interpreted as
keywords or operators, e.g.:

``` fhirpath
Message.`PID-1`
```

### Collections

Collections are fundamental to FHIRPath, in that the result of every
expression is a collection, even if that expression only results in a
single element. This approach allows paths to be specified without
having to care about the cardinality of any particular element, and is
therefore ideally suited to graph traversal.

Within FHIRPath, a collection is:

-   Ordered - The order of items in the collection is important and is
    preserved through operations as much as possible. Operators and
    functions that do not preserve order will note that in their
    documentation.
-   Non-Unique - Duplicate elements are allowed within a collection.
    Some operations and functions, such as
    `distinct()`{.language-plaintext .highlighter-rouge} and the union
    operator `|`{.language-plaintext .highlighter-rouge} produce
    collections of unique elements, but in general, duplicate elements
    are allowed.
-   Indexed - Each item in a collection can be addressed by its index,
    i.e. ordinal position within the collection (e.g.
    `a[2]`{.language-plaintext .highlighter-rouge}).
-   Unless specified otherwise by the underlying Object Model, the first
    item in a collection has index 0. Note that if the underlying model
    specifies that a collection is 1-based (the only reasonable
    alternative to 0-based collections), *any collections generated from
    operations on the 1-based list are 0-based*.
-   Countable - The number of items in a given collection can always be
    determined using the `count()`{.language-plaintext
    .highlighter-rouge} function

Note that the outcome of functions like `children()`{.language-plaintext
.highlighter-rouge} and `descendants()`{.language-plaintext
.highlighter-rouge} cannot be assumed to be in any meaningful order, and
`first()`{.language-plaintext .highlighter-rouge},
`last()`{.language-plaintext .highlighter-rouge},
`tail()`{.language-plaintext .highlighter-rouge},
`skip()`{.language-plaintext .highlighter-rouge} and
`take()`{.language-plaintext .highlighter-rouge} should not be used on
collections derived from these paths. Note that some implementations may
follow the logical order implied by the object model, and some may not,
and some may be different depending on the underlying source.
Implementations may decide to return an error if an attempt is made to
perform an order-dependent operation on a list whose order is undefined.

### Paths and polymorphic items

In the underlying representation of data, nodes may be typed and
represent polymorphic items. Paths may either ignore the type of a node,
and continue along the path or may be explicit about the expected node
and filter the set of nodes by type before navigating down child nodes:

``` fhirpath
Observation.value.unit // all kinds of value
Observation.value.ofType(Quantity).unit // only values that are of type Quantity
```

The `is`{.language-plaintext .highlighter-rouge} operator can be used to
determine whether or not a given value is of a given type:

``` fhirpath
Observation.value is Quantity // returns true if the value is of type Quantity
```

The `as`{.language-plaintext .highlighter-rouge} operator can be used to
treat a value as a specific type:

``` fhirpath
Observation.value as Quantity // returns value as a Quantity if it is of type Quantity, and an empty result otherwise
```

The list of available types that can be passed as an argument to the
`ofType()`{.language-plaintext .highlighter-rouge} function and
`is`{.language-plaintext .highlighter-rouge} and
`as`{.language-plaintext .highlighter-rouge} operators is determined by
the underlying object model. Within FHIRPath, they are just identifiers,
either delimited or simple.

## Expressions

FHIRPath expressions can consist of *paths*, *literals*, *operators*,
and *function invocations*, and these elements can be chained together,
so that the output of one operation or function is the input to the
next. This is the core of the *fluent* [\[Fluent\]](#fluent) syntactic
style and allows complex paths and expressions to be built up from
simpler components.

### Literals

In addition to paths, FHIRPath expressions may contain *literals*,
*operators*, and *function invocations*. FHIRPath supports the following
types of literals:

``` txt
Boolean: true, false
String: 'test string', 'urn:oid:3.4.5.6.7.8'
Integer: 0, 45
Long: 0L, 45L    // Long is defined as STU
Decimal: 0.0, 3.14159265
Date: @2015-02-04 (@ followed by ISO8601 compliant date)
DateTime: @2015-02-04T14:34:28+09:00 (@ followed by ISO8601 compliant date/time)
Time: @T14:34:28 (@ followed by ISO8601 compliant time beginning with T, no timezone offset)
Quantity: 10 'mg', 4 days
```

For each type of literal, FHIRPath defines a named system type to allow
operations and functions to be defined, as well as an ultimate root
type, `System.Any`{.language-plaintext .highlighter-rouge}. For example,
the multiplication operator (`*`{.language-plaintext
.highlighter-rouge}) is defined for the numeric types Integer and
Decimal, as well as the Quantity type. See the discussion on
[Models](#models) for a more detailed discussion of how these types are
used within evaluation contexts.

#### Boolean

The `Boolean`{.language-plaintext .highlighter-rouge} type represents
the logical Boolean values `true`{.language-plaintext
.highlighter-rouge} and `false`{.language-plaintext .highlighter-rouge}.
These values are used as the result of comparisons, and can be combined
using logical operators such as `and`{.language-plaintext
.highlighter-rouge} and `or`{.language-plaintext .highlighter-rouge}.

``` fhirpath
true
false
```

#### String

The `String`{.language-plaintext .highlighter-rouge} type represents
string values up to 2^31^-1 characters in length. String literals are
surrounded by single-quotes and may use `\`{.language-plaintext
.highlighter-rouge}-escapes to escape quotes and represent Unicode
characters:

  Escape                                             Character
  -------------------------------------------------- ----------------------------------------------------------------------------------
  `\'`{.language-plaintext .highlighter-rouge}       Single-quote
  `\"`{.language-plaintext .highlighter-rouge}       Double-quote
  `` \` ``{.language-plaintext .highlighter-rouge}   Backtick
  `\r`{.language-plaintext .highlighter-rouge}       Carriage Return
  `\n`{.language-plaintext .highlighter-rouge}       Line Feed
  `\t`{.language-plaintext .highlighter-rouge}       Tab
  `\f`{.language-plaintext .highlighter-rouge}       Form Feed
  `\\`{.language-plaintext .highlighter-rouge}       Backslash
  `\uXXXX`{.language-plaintext .highlighter-rouge}   Unicode character, where XXXX is the hexadecimal representation of the character

No other escape sequences besides those listed above are recognized.

Note that Unicode is supported in both string literals and delimited
[Identifiers](#identifiers).

``` fhirpath
'test string'
'urn:oid:3.4.5.6.7.8'
```

If a `\`{.language-plaintext .highlighter-rouge} is used at the
beginning of a non-escape sequence, it will be ignored and will not
appear in the sequence.

``` txt
define TestEscape1: '\p' // 'p'
define TestEscape2: '\\p' // '\p'
define TestEscape3: '\3' // '3'
define TestEscape4: '\u005' // 'u005'
define TestEscape5: '\' // ''
```

#### Integer

The `Integer`{.language-plaintext .highlighter-rouge} type represents
whole numbers in the range -2^31^ to 2^31^-1.

``` fhirpath
0
45
-5
```

> Note that the minus sign (`-`{.language-plaintext .highlighter-rouge})
> in the representation of a negative integer is not part of the
> literal, it is the unary negation operator defined as part of FHIRPath
> syntax.

##### Long

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

The `Long`{.language-plaintext .highlighter-rouge} type represents whole
numbers in the range -2^63^ to 2^63^-1.

``` stu
0L
45L
-5L
```

This type corresponds to System.Long

#### Decimal

The `Decimal`{.language-plaintext .highlighter-rouge} type represents
real values in the range (-10^28^+1)/10^8^ to (10^28^-1)/10^8^ with a
step size of 10^-8^. This range is defined based on a survey of
decimal-value implementations and is based on the most useful lowest
common denominator. Implementations can provide support for larger
decimals and higher precision, but must provide at least the range and
precision defined here. In addition, implementations should use
[fixed-precision
decimal](https://en.wikipedia.org/wiki/Fixed-point_arithmetic) formats
to ensure that decimal values are accurately represented.

``` fhirpath
0.0
3.14159265
```

Decimal literals cannot use exponential notation. There is enough
additional complexity associated with enabling exponential notation that
this is outside the scope of what FHIRPath is intended to support
(namely graph traversal).

#### Date

The `Date`{.language-plaintext .highlighter-rouge} type represents date
and partial date values in the range \@0001-01-01 to \@9999-12-31 with a
1 day step size.

The `Date`{.language-plaintext .highlighter-rouge} literal is a subset
of [\[ISO8601\]](#ISO8601):

-   A date literal begins with an `@`{.language-plaintext
    .highlighter-rouge}
-   It uses the format **YYYY-MM-DD** format, though month and day parts
    are optional, and a separator is required between provided
    components
-   Week dates and ordinal dates are not allowed
-   Years must be present (e.g. `@-10-20`{.language-plaintext
    .highlighter-rouge} is not a valid Date in FHIRPath)
-   Months must be present if a day is present
-   To specify a date and time together, see the description of
    `DateTime`{.language-plaintext .highlighter-rouge} below

The following examples illustrate the use of the
`Date`{.language-plaintext .highlighter-rouge} literal:

``` fhirpath
@2014-01-25
@2014-01
@2014
```

Consult the [formal grammar](grammar.html) for more details.

#### Time

The `Time`{.language-plaintext .highlighter-rouge} type represents
time-of-day and partial time-of-day values in the range \@T00:00:00.000
to \@T23:59:59.999 with a step size of 1 millisecond. This range is
defined based on a survey of time implementations and is based on the
most useful lowest common denominator. Implementations can provide
support for higher precision, but must provide at least the range and
precision defined here. Time values in FHIRPath do not have a timezone
or timezone offset.

The `Time`{.language-plaintext .highlighter-rouge} literal uses a subset
of [\[ISO8601\]](#ISO8601):

-   A time begins with a `@T`{.language-plaintext .highlighter-rouge}
-   It uses the **Thh:mm:ss.fff** format

The following examples illustrate the use of the
`Time`{.language-plaintext .highlighter-rouge} literal:

``` fhirpath
@T12:00
@T14:30:14.559
```

Consult the [formal grammar](grammar.html) for more details.

#### DateTime

The `DateTime`{.language-plaintext .highlighter-rouge} type represents
date/time and partial date/time values in the range
`@0001-01-01T00:00:00.000 to @9999-12-31T23:59:59.999`{.language-plaintext
.highlighter-rouge} with a 1 millisecond step size. This range is
defined based on a survey of datetime implementations and is based on
the most useful lowest common denominator. Implementations can provide
support for larger ranges and higher precision, but must provide at
least the range and precision defined here.

The `DateTime`{.language-plaintext .highlighter-rouge} literal combines
the `Date`{.language-plaintext .highlighter-rouge} and
`Time`{.language-plaintext .highlighter-rouge} literals and is a subset
of [\[ISO8601\]](#ISO8601):

-   A datetime literal begins with an `@`{.language-plaintext
    .highlighter-rouge}
-   It uses the **YYYY-MM-DDThh:mm:ss.fff(+\|-)hh:mm** format
-   Timezone offset is optional, but if present the notation
    **(+\|-)hh:mm** is used (so must include both minutes and hours)
-   **Z** is allowed as a synonym for the zero (+00:00) UTC offset.
-   A `T`{.language-plaintext .highlighter-rouge} can be used at the end
    of any date (year, year-month, or year-month-day) to indicate a
    partial DateTime.

The following example illustrates the use of the
`DateTime`{.language-plaintext .highlighter-rouge} literal:

``` fhirpath
@2014-01-25T14:30:14.559
@2014-01-25T14:30:14.559Z // A date time with UTC timezone offset
@2014-01-25T14:30 // A partial DateTime with year, month, day, hour, and minute
@2014-03-25T // A partial DateTime with year, month, and day
@2014-01T // A partial DateTime with year and month
@2014T // A partial DateTime with only the year
```

The suffix `T`{.language-plaintext .highlighter-rouge} is allowed after
a year, year-month, or year-month-day literal because without it, there
would be no way to specify a partial DateTime with only a year, month,
or day; the literal would always result in a Date value.

Consult the [formal grammar](grammar.html) for more details.

#### Quantity

The `Quantity`{.language-plaintext .highlighter-rouge} type represents
quantities with a specified unit, where the `value`{.language-plaintext
.highlighter-rouge} component is defined as a
`Decimal`{.language-plaintext .highlighter-rouge}, and the
`unit`{.language-plaintext .highlighter-rouge} element is represented as
a `String`{.language-plaintext .highlighter-rouge} that is required to
be either a valid Unified Code for Units of Measure [\[UCUM\]](#UCUM)
unit or one of the calendar duration keywords, singular or plural.

The `Quantity`{.language-plaintext .highlighter-rouge} literal is a
number (integer or decimal), followed by a (single-quoted) string
representing a valid Unified Code for Units of Measure [\[UCUM\]](#UCUM)
unit or calendar duration keyword. If the value literal is an Integer,
it will be implicitly converted to a Decimal in the resulting Quantity
value:

``` fhirpath
4.5 'mg'
100 '[degF]'
```

> Implementations must respect UCUM units, meaning that they must not
> ignore UCUM units in calculations involving quantities, including
> comparison, conversion, and arithmetic operations. For implementations
> that do not support unit conversion, this means that the calculation
> need only be supported if the units are the same value,
> case-sensitively.
>
> When using [\[UCUM\]](#UCUM) units within FHIRPath, implementations
> shall use case-sensitive comparisons.
>
> Implementations shall support comparison and arithmetic operations on
> quantities with units where the units are the same.
>
> Implementations should support other unit functionality as specified
> by UCUM, including unit conversion.
>
> Implementations that do not support complete UCUM functionality may
> return empty (`{ }`{.language-plaintext .highlighter-rouge}) for
> calculations involving quantities with units where the units are
> different.

##### Time-valued Quantities

For time-valued quantities, in addition to the definite duration UCUM
units, FHIRPath defines calendar duration keywords for calendar duration
units:

  Calendar Duration                                                                                              Unit Representation                                       Relationship to Definite Duration UCUM Unit
  -------------------------------------------------------------------------------------------------------------- --------------------------------------------------------- -----------------------------------------------------
  `year`{.language-plaintext .highlighter-rouge}/`years`{.language-plaintext .highlighter-rouge}                 `'year'`{.language-plaintext .highlighter-rouge}          `~ 1 'a'`{.language-plaintext .highlighter-rouge}
  `month`{.language-plaintext .highlighter-rouge}/`months`{.language-plaintext .highlighter-rouge}               `'month'`{.language-plaintext .highlighter-rouge}         `~ 1 'mo'`{.language-plaintext .highlighter-rouge}
  `week`{.language-plaintext .highlighter-rouge}/`weeks`{.language-plaintext .highlighter-rouge}                 `'week'`{.language-plaintext .highlighter-rouge}          `~ 1 'wk'`{.language-plaintext .highlighter-rouge}
  `day`{.language-plaintext .highlighter-rouge}/`days`{.language-plaintext .highlighter-rouge}                   `'day'`{.language-plaintext .highlighter-rouge}           `~ 1 'd'`{.language-plaintext .highlighter-rouge}
  `hour`{.language-plaintext .highlighter-rouge}/`hours`{.language-plaintext .highlighter-rouge}                 `'hour'`{.language-plaintext .highlighter-rouge}          `~ 1 'h'`{.language-plaintext .highlighter-rouge}
  `minute`{.language-plaintext .highlighter-rouge}/`minutes`{.language-plaintext .highlighter-rouge}             `'minute'`{.language-plaintext .highlighter-rouge}        `~ 1 'min'`{.language-plaintext .highlighter-rouge}
  `second`{.language-plaintext .highlighter-rouge}/`seconds`{.language-plaintext .highlighter-rouge}             `'second'`{.language-plaintext .highlighter-rouge}        `= 1 's'`{.language-plaintext .highlighter-rouge}
  `millisecond`{.language-plaintext .highlighter-rouge}/`milliseconds`{.language-plaintext .highlighter-rouge}   `'millisecond'`{.language-plaintext .highlighter-rouge}   `= 1 'ms'`{.language-plaintext .highlighter-rouge}

For example, the following quantities are *calendar duration*
quantities:

``` fhirpath
1 year
4 days
```

Whereas the following quantities are *definite duration* quantities:

``` fhirpath
1 'a'
4 'd'
```

The table above defines the equality/equivalence relationship between
calendar and definite duration quantities. For example,
`1 year`{.language-plaintext .highlighter-rouge} is not equal to
`1 'a'`{.language-plaintext .highlighter-rouge}, but it is equivalent to
`1 'a'`{.language-plaintext .highlighter-rouge}. See [Date/Time
Arithmetic](#datetime-arithmetic) for more information on using
time-valued quantities in FHIRPath.

### Operators

Expressions can also contain *operators*, like those for mathematical
operations and boolean logic:

``` fhirpath
Appointment.minutesDuration / 60 > 5
MedicationAdministration.wasNotGiven implies MedicationAdministration.reasonNotGiven.exists()
name.given | name.family // union of given and family names
'sir ' + name.given
```

Operators available in FHIRPath are covered in detail in the
[Operations](#operations) section.

### Function Invocations

Finally, FHIRPath supports the notion of functions, which operate on a
collection of values (referred to as the *input collection*), optionally
taking arguments, and return another collection (referred to as the
*output collection*). For example:

``` fhirpath
name.given.substring(0,4)
identifier.where(use = 'official')
```

Since all functions work on input collections, constants will first be
converted to a collection when functions are invoked on constants:

``` fhirpath
(4+5).count()
```

will return `1`{.language-plaintext .highlighter-rouge}, since the input
collection is implicitly a collection with one constant number
`9`{.language-plaintext .highlighter-rouge}.

In general, functions in FHIRPath operate on collections and return new
collections. This property, combined with the syntactic style of *dot
invocation* enables functions to be chained together, creating a
*fluent*-style syntax:

``` fhirpath
Patient.telecom.where(use = 'official').union(Patient.contact.telecom.where(use = 'official')).exists().not()
```

For a complete listing of the functions defined in FHIRPath, refer to
the [Functions](#functions) section.

### Null and empty

There is no literal representation for *null* in FHIRPath. This means
that when, in an underlying data object (i.e. they physical data on
which the implementation is operating) a member is null or missing,
there will simply be no corresponding node for that member in the tree,
e.g. `Patient.name`{.language-plaintext .fhirpath .highlighter-rouge}
will return an empty collection (not null) if there are no name elements
in the instance.

In expressions, the empty collection is represented as
`{ }`{.language-plaintext .highlighter-rouge}.

#### Propagation of empty results in expressions

FHIRPath functions and operators both propagate empty results, but the
behavior is in general different when the argument to the function or
operator expects a collection (e.g. `select()`{.language-plaintext
.highlighter-rouge}, `where()`{.language-plaintext .highlighter-rouge}
and `|`{.language-plaintext .highlighter-rouge} (union)) versus when the
argument to the function or operator takes a single value as input (e.g.
`+`{.language-plaintext .highlighter-rouge} and
`substring()`{.language-plaintext .highlighter-rouge}).

For functions or operators that take a single values as input, this
means in general if the input is empty, then the result will be empty as
well. More specifically:

-   If a single-input operator or function operates on an empty
    collection, the result is an empty collection
-   If a single-input operator or function is passed an empty collection
    as an argument, the result is an empty collection
-   If any operand to a single-input operator or function is an empty
    collection, the result is an empty collection.

For operator or function arguments that expect collections, in general
the empty collection is treated as any other collection would be. For
example, the union (`|`{.language-plaintext .highlighter-rouge}) of an
empty collection with some non-empty collection is that non-empty
collection.

When functions or operators behave differently from these general
principles, (for example the `count()`{.language-plaintext
.highlighter-rouge} and `empty()`{.language-plaintext
.highlighter-rouge} functions), this is clearly documented in the next
sections.

### Singleton Evaluation of Collections

In general, when a collection is passed as an argument to a function or
operator that expects a single item as input, the collection is
implicitly converted to a singleton as follows:

``` txt
IF the collection contains a single node AND the node's value can be implicitly converted to the expected input type THEN
  The collection evaluates to the value of that single node
ELSE IF the collection contains a single node AND the expected input type is Boolean THEN
  The collection evaluates to true
ELSE IF the collection is empty THEN
  The collection evaluates to an empty collection
ELSE
  The evaluation will end and signal an error to the calling environment
```

For example:

``` fhirpath
Patient.name.family + ', ' + Patient.name.given
```

If the `Patient`{.language-plaintext .highlighter-rouge} instance has a
single `name`{.language-plaintext .highlighter-rouge}, and that name has
a single `given`{.language-plaintext .highlighter-rouge}, then this will
evaluate without any issues. However, if the
`Patient`{.language-plaintext .highlighter-rouge} has multiple
`name`{.language-plaintext .highlighter-rouge} elements, or the single
name has multiple `given`{.language-plaintext .highlighter-rouge}
elements, then it\'s ambiguous which of the elements should be used as
the input to the `+`{.language-plaintext .highlighter-rouge} operator,
and the result is an error.

As another example:

``` fhirpath
Patient.active and Patient.gender and Patient.telecom
```

Assuming the `Patient`{.language-plaintext .highlighter-rouge} instance
has an `active`{.language-plaintext .highlighter-rouge} value of
`true`{.language-plaintext .highlighter-rouge}, a
`gender`{.language-plaintext .highlighter-rouge} of
`female`{.language-plaintext .highlighter-rouge} and a single
`telecom`{.language-plaintext .highlighter-rouge} element, this
expression will result in true. However, consider a different instance
of `Patient`{.language-plaintext .highlighter-rouge} that has an
`active`{.language-plaintext .highlighter-rouge} value of
`true`{.language-plaintext .highlighter-rouge}, a
`gender`{.language-plaintext .highlighter-rouge} of
`male`{.language-plaintext .highlighter-rouge}, and multiple
`telecom`{.language-plaintext .highlighter-rouge} elements, then this
expression will result in an error because of the multiple telecom
elements.

Note that for repeating elements like `telecom`{.language-plaintext
.highlighter-rouge} in the above example, the logic *looks* like an
existence check. To avoid confusion and reduce unintended errors,
authors should use the explicit form of these checks when appropriate.
For example, a more explicit rendering of the same logic that more
clearly indicates the actual intent and avoids the run-time error is:

``` fhirpath
Patient.active and Patient.gender and Patient.telecom.count() = 1
```

## Functions

Functions are distinguished from path navigation names by the fact that
they are followed by a `()`{.language-plaintext .highlighter-rouge} with
zero or more arguments. Throughout this specification, the word
*parameter* is used to refer to the definition of a parameter as part of
the function definition, while the word *argument* is used to refer to
the values passed as part of a function invocation. With a few minor
exceptions (e.g. [current date and time
functions](#current-date-and-time-functions)), functions in FHIRPath
operate on a collection of values (referred to as the *input
collection*) and produce another collection as output (referred to as
the *output collection*). However, for many functions, passing an input
collection with more than one item is defined as an error condition.
Each function definition should define its behavior for input
collections of any cardinality (0, 1, or many).

Correspondingly, arguments to the functions can be any FHIRPath
expression, though functions taking a single item as input require these
expressions to evaluate to a collection containing a single item of a
specific type. This approach allows functions to be chained,
successively operating on the results of the previous function in order
to produce the desired final result.

The following sections describe the functions supported in FHIRPath,
detailing the expected types of parameters and type of collection
returned by the function:

-   If the function expects the argument passed to a parameter to be a
    single value (e.g. `startsWith(prefix: String)`{.language-plaintext
    .highlighter-rouge}) and it is passed an argument that evaluates to
    a collection with multiple items, or to a collection with an item
    that is not of the required type (or cannot be converted to the
    required type), the evaluation of the expression will end and an
    error will be signaled to the calling environment.
-   If the function takes an `expression`{.language-plaintext
    .highlighter-rouge} as a parameter, the function will evaluate the
    expression passed for the parameter with respect to each of the
    items in the input collection. These expressions may refer to the
    special `$this`{.language-plaintext .highlighter-rouge} and
    `$index`{.language-plaintext .highlighter-rouge} elements, which
    represent the item from the input collection currently under
    evaluation, and its index in the collection, respectively. For
    example, in
    `name.given.where($this > 'ba' and $this < 'bc')`{.language-plaintext
    .fhirpath .highlighter-rouge} the `where()`{.language-plaintext
    .highlighter-rouge} function will iterate over each item in the
    input collection (elements named `given`{.language-plaintext
    .highlighter-rouge}) and `$this`{.language-plaintext
    .highlighter-rouge} will be set to each item when the expression
    passed to `where()`{.language-plaintext .highlighter-rouge} is
    evaluated.

For the [aggregate](#aggregates) function, expressions may also refer to
the special `$total`{.language-plaintext .highlighter-rouge} element,
representing the result of the aggregation.

Note that the bracket notation in function signatures indicates optional
parameters.

Note also that although all functions return collections, if a given
function is defined to return a single element, the return type is
simplified to just the type of the single element, rather than the list
type.

### Existence

#### empty() : Boolean {#empty--boolean}

Returns `true`{.language-plaintext .highlighter-rouge} if the input
collection is empty (`{ }`{.language-plaintext .highlighter-rouge}) and
`false`{.language-plaintext .highlighter-rouge} otherwise.

#### exists(\[criteria : expression\]) : Boolean {#existscriteria--expression--boolean}

Returns `true`{.language-plaintext .highlighter-rouge} if the input
collection has any elements (optionally filtered by the criteria), and
`false`{.language-plaintext .highlighter-rouge} otherwise. This is the
opposite of `empty()`{.language-plaintext .highlighter-rouge}, and as
such is a shorthand for `empty().not()`{.language-plaintext
.highlighter-rouge}. If the input collection is empty
(`{ }`{.language-plaintext .highlighter-rouge}), the result is
`false`{.language-plaintext .highlighter-rouge}.

Using the optional criteria can be considered a shorthand for
`where(criteria).exists()`{.language-plaintext .highlighter-rouge}.

Note that a common term for this function is *any*.

The following examples illustrate some potential uses of the
`exists()`{.language-plaintext .highlighter-rouge} function:

``` fhirpath
Patient.name.exists()
Patient.identifier.exists(use = 'official')
Patient.telecom.exists(system = 'phone' and use = 'mobile')
Patient.generalPractitioner.exists(resolve() is Practitioner) // this example is wrong
```

The first example returns `true`{.language-plaintext .highlighter-rouge}
if the `Patient`{.language-plaintext .highlighter-rouge} has any
`name`{.language-plaintext .highlighter-rouge} elements.

The second example returns `true`{.language-plaintext
.highlighter-rouge} if the `Patient`{.language-plaintext
.highlighter-rouge} has any `identifier`{.language-plaintext
.highlighter-rouge} elements that have a `use`{.language-plaintext
.highlighter-rouge} element equal to `'official'`{.language-plaintext
.highlighter-rouge}.

The third example returns `true`{.language-plaintext .highlighter-rouge}
if the `Patient`{.language-plaintext .highlighter-rouge} has any
`telecom`{.language-plaintext .highlighter-rouge} elements that have a
`system`{.language-plaintext .highlighter-rouge} element equal to
`'phone'`{.language-plaintext .highlighter-rouge} and a
`use`{.language-plaintext .highlighter-rouge} element equal to
`'mobile'`{.language-plaintext .highlighter-rouge}.

And finally, the fourth example returns `true`{.language-plaintext
.highlighter-rouge} if the `Patient`{.language-plaintext
.highlighter-rouge} has any `generalPractitioner`{.language-plaintext
.highlighter-rouge} elements of type `Practitioner`{.language-plaintext
.highlighter-rouge}.

#### all(criteria : expression) : Boolean {#allcriteria--expression--boolean}

Returns `true`{.language-plaintext .highlighter-rouge} if for every
element in the input collection, `criteria`{.language-plaintext
.highlighter-rouge} evaluates to `true`{.language-plaintext
.highlighter-rouge}. Otherwise, the result is
`false`{.language-plaintext .highlighter-rouge}. If the input collection
is empty (`{ }`{.language-plaintext .highlighter-rouge}), the result is
`true`{.language-plaintext .highlighter-rouge}.

``` fhirpath
generalPractitioner.all($this.resolve() is Practitioner)
```

This example returns true if all of the
`generalPractitioner`{.language-plaintext .highlighter-rouge} elements
are of type `Practitioner`{.language-plaintext .highlighter-rouge}.

#### allTrue() : Boolean {#alltrue--boolean}

Takes a collection of Boolean values and returns
`true`{.language-plaintext .highlighter-rouge} if all the items are
`true`{.language-plaintext .highlighter-rouge}. If any items are
`false`{.language-plaintext .highlighter-rouge}, the result is
`false`{.language-plaintext .highlighter-rouge}. If the input is empty
(`{ }`{.language-plaintext .highlighter-rouge}), the result is
`true`{.language-plaintext .highlighter-rouge}.

The following example returns `true`{.language-plaintext
.highlighter-rouge} if all of the components of the Observation have a
value greater than 90 mm\[Hg\]:

``` fhirpath
Observation.select(component.value > 90 'mm[Hg]').allTrue()
```

#### anyTrue() : Boolean {#anytrue--boolean}

Takes a collection of Boolean values and returns
`true`{.language-plaintext .highlighter-rouge} if any of the items are
`true`{.language-plaintext .highlighter-rouge}. If all the items are
`false`{.language-plaintext .highlighter-rouge}, or if the input is
empty (`{ }`{.language-plaintext .highlighter-rouge}), the result is
`false`{.language-plaintext .highlighter-rouge}.

The following example returns `true`{.language-plaintext
.highlighter-rouge} if any of the components of the Observation have a
value greater than 90 mm\[Hg\]:

``` fhirpath
Observation.select(component.value > 90 'mm[Hg]').anyTrue()
```

#### allFalse() : Boolean {#allfalse--boolean}

Takes a collection of Boolean values and returns
`true`{.language-plaintext .highlighter-rouge} if all the items are
`false`{.language-plaintext .highlighter-rouge}. If any items are
`true`{.language-plaintext .highlighter-rouge}, the result is
`false`{.language-plaintext .highlighter-rouge}. If the input is empty
(`{ }`{.language-plaintext .highlighter-rouge}), the result is
`true`{.language-plaintext .highlighter-rouge}.

The following example returns `true`{.language-plaintext
.highlighter-rouge} if none of the components of the Observation have a
value greater than 90 mm\[Hg\]:

``` fhirpath
Observation.select(component.value > 90 'mm[Hg]').allFalse()
```

#### anyFalse() : Boolean {#anyfalse--boolean}

Takes a collection of Boolean values and returns
`true`{.language-plaintext .highlighter-rouge} if any of the items are
`false`{.language-plaintext .highlighter-rouge}. If all the items are
`true`{.language-plaintext .highlighter-rouge}, or if the input is empty
(`{ }`{.language-plaintext .highlighter-rouge}), the result is
`false`{.language-plaintext .highlighter-rouge}.

The following example returns `true`{.language-plaintext
.highlighter-rouge} if any of the components of the Observation have a
value that is not greater than 90 mm\[Hg\]:

``` fhirpath
Observation.select(component.value > 90 'mm[Hg]').anyFalse()
```

#### subsetOf(other : collection) : Boolean {#subsetofother--collection--boolean}

Returns `true`{.language-plaintext .highlighter-rouge} if all items in
the input collection are members of the collection passed as the
`other`{.language-plaintext .highlighter-rouge} argument. Membership is
determined using the [equals](#equals) (`=`{.language-plaintext
.highlighter-rouge}) operation.

Conceptually, this function is evaluated by testing each element in the
input collection for membership in the `other`{.language-plaintext
.highlighter-rouge} collection, with a default of
`true`{.language-plaintext .highlighter-rouge}. This means that if the
input collection is empty (`{ }`{.language-plaintext
.highlighter-rouge}), the result is `true`{.language-plaintext
.highlighter-rouge}, otherwise if the `other`{.language-plaintext
.highlighter-rouge} collection is empty (`{ }`{.language-plaintext
.highlighter-rouge}), the result is `false`{.language-plaintext
.highlighter-rouge}.

The following example returns true if the tags defined in any contained
resource are a subset of the tags defined in the MedicationRequest
resource:

``` fhirpath
MedicationRequest.contained.meta.tag.subsetOf(MedicationRequest.meta.tag)
```

#### supersetOf(other : collection) : Boolean {#supersetofother--collection--boolean}

Returns `true`{.language-plaintext .highlighter-rouge} if all items in
the collection passed as the `other`{.language-plaintext
.highlighter-rouge} argument are members of the input collection.
Membership is determined using the [equals](#equals)
(`=`{.language-plaintext .highlighter-rouge}) operation.

Conceptually, this function is evaluated by testing each element in the
`other`{.language-plaintext .highlighter-rouge} collection for
membership in the input collection, with a default of
`true`{.language-plaintext .highlighter-rouge}. This means that if the
`other`{.language-plaintext .highlighter-rouge} collection is empty
(`{ }`{.language-plaintext .highlighter-rouge}), the result is
`true`{.language-plaintext .highlighter-rouge}, otherwise if the input
collection is empty (`{ }`{.language-plaintext .highlighter-rouge}), the
result is `false`{.language-plaintext .highlighter-rouge}.

The following example returns true if the tags defined in any contained
resource are a superset of the tags defined in the MedicationRequest
resource:

``` fhirpath
MedicationRequest.contained.meta.tag.supersetOf(MedicationRequest.meta.tag)
```

#### count() : Integer {#count--integer}

Returns the integer count of the number of items in the input
collection. Returns 0 when the input collection is empty.

#### distinct() : collection {#distinct--collection}

Returns a collection containing only the unique items in the input
collection. To determine whether two items are the same, the
[equals](#equals) (`=`{.language-plaintext .highlighter-rouge}) operator
is used, as defined below.

If the input collection is empty (`{ }`{.language-plaintext
.highlighter-rouge}), the result is empty.

Note that the order of elements in the input collection is not
guaranteed to be preserved in the result.

The following example returns the distinct list of tags on the given
Patient:

``` fhirpath
Patient.meta.tag.distinct()
```

#### isDistinct() : Boolean {#isdistinct--boolean}

Returns `true`{.language-plaintext .highlighter-rouge} if all the items
in the input collection are distinct. To determine whether two items are
distinct, the [equals](#equals) (`=`{.language-plaintext
.highlighter-rouge}) operator is used, as defined below.

Conceptually, this function is shorthand for a comparison of the
`count()`{.language-plaintext .highlighter-rouge} of the input
collection against the `count()`{.language-plaintext .highlighter-rouge}
of the `distinct()`{.language-plaintext .highlighter-rouge} of the input
collection:

``` fhirpath
X.count() = X.distinct().count()
```

This means that if the input collection is empty
(`{ }`{.language-plaintext .highlighter-rouge}), the result is true.

### Filtering and projection

#### where(criteria : expression) : collection {#wherecriteria--expression--collection}

Returns a collection containing only those elements in the input
collection for which the stated `criteria`{.language-plaintext
.highlighter-rouge} expression evaluates to `true`{.language-plaintext
.highlighter-rouge}. Elements for which the expression evaluates to
`false`{.language-plaintext .highlighter-rouge} or empty
(`{ }`{.language-plaintext .highlighter-rouge}) are not included in the
result.

If the input collection is empty (`{ }`{.language-plaintext
.highlighter-rouge}), the result is empty.

If the result of evaluating the condition is other than a single boolean
value, the evaluation will end and signal an error to the calling
environment, consistent with singleton evaluation of collections
behavior.

The following example returns the list of `telecom`{.language-plaintext
.highlighter-rouge} elements that have a `use`{.language-plaintext
.highlighter-rouge} element with the value of
`'official'`{.language-plaintext .highlighter-rouge}:

``` fhirpath
Patient.telecom.where(use = 'official')
```

#### select(projection: expression) : collection {#selectprojection-expression--collection}

Evaluates the `projection`{.language-plaintext .highlighter-rouge}
expression for each item in the input collection. The result of each
evaluation is added to the output collection. If the evaluation results
in a collection with multiple items, all items are added to the output
collection (collections resulting from evaluation of
`projection`{.language-plaintext .highlighter-rouge} are *flattened*).
This means that if the evaluation for an element results in the empty
collection (`{ }`{.language-plaintext .highlighter-rouge}), no element
is added to the result, and that if the input collection is empty
(`{ }`{.language-plaintext .highlighter-rouge}), the result is empty as
well.

``` fhirpath
Bundle.entry.select(resource as Patient)
```

This example results in a collection with only the patient resources
from the bundle.

``` fhirpath
Bundle.entry.select((resource as Patient).telecom.where(system = 'phone'))
```

This example results in a collection with all the telecom elements with
system of `phone`{.language-plaintext .highlighter-rouge} for all the
patients in the bundle.

``` fhirpath
Patient.name.where(use = 'usual').select(given.first() + ' ' + family)
```

This example returns a collection containing, for each \"usual\" name
for the Patient, the concatenation of the first given and family names.

#### repeat(projection: expression) : collection {#repeatprojection-expression--collection}

A version of `select`{.language-plaintext .highlighter-rouge} that will
repeat the `projection`{.language-plaintext .highlighter-rouge} and add
items to the output collection only if they are not already in the
output collection as determined by the [equals](#equals)
(`=`{.language-plaintext .highlighter-rouge}) operator.

This can be evaluated by adding all elements in the input collection to
an input queue, then for each item in the input queue evaluate the
repeat expression. If the result of the repeat expression is not in the
output collection, add it to both the output collection and also the
input queue. Processing continues until the input queue is empty.

This function can be used to traverse a tree and selecting only specific
children:

``` fhirpath
ValueSet.expansion.repeat(contains)
```

Will repeat finding children called `contains`{.language-plaintext
.highlighter-rouge}, until no new nodes are found.

``` fhirpath
Questionnaire.repeat(item)
```

Will repeat finding children called `item`{.language-plaintext
.highlighter-rouge}, until no new nodes are found.

Note that this is slightly different from:

``` fhirpath
Questionnaire.descendants().select(item)
```

which would find *any* descendants called `item`{.language-plaintext
.highlighter-rouge}, not just the ones nested inside other
`item`{.language-plaintext .highlighter-rouge} elements.

The order of items returned by the `repeat()`{.language-plaintext
.highlighter-rouge} function is undefined.

#### ofType(type : *type specifier*) : collection {#oftypetype--type-specifier--collection}

Returns a collection that contains all items in the input collection
that are of the given type or a subclass thereof. If the input
collection is empty (`{ }`{.language-plaintext .highlighter-rouge}), the
result is empty. The `type`{.language-plaintext .highlighter-rouge}
argument is an identifier that must resolve to the name of a type in a
model. For implementations with compile-time typing, this requires
special-case handling when processing the argument to treat it as type
specifier rather than an identifier expression:

``` fhirpath
Bundle.entry.resource.ofType(Patient)
```

In the above example, the symbol `Patient`{.language-plaintext
.highlighter-rouge} must be treated as a type identifier rather than a
reference to a Patient in context.

### Subsetting

#### \[ index : Integer \] : collection {#-index--integer---collection}

The indexer operation returns a collection with only the
`index`{.language-plaintext .highlighter-rouge}-th item (0-based index).
If the input collection is empty (`{ }`{.language-plaintext
.highlighter-rouge}), or the index lies outside the boundaries of the
input collection, an empty collection is returned.

> **Note:** Unless specified otherwise by the underlying Object Model,
> the first item in a collection has index 0. Note that if the
> underlying model specifies that a collection is 1-based (the only
> reasonable alternative to 0-based collections), *any collections
> generated from operations on the 1-based list are 0-based*.

The following example returns the element in the
`name`{.language-plaintext .highlighter-rouge} collection of the Patient
with index 0:

``` fhirpath
Patient.name[0]
```

#### single() : collection {#single--collection}

Will return the single item in the input if there is just one item. If
the input collection is empty (`{ }`{.language-plaintext
.highlighter-rouge}), the result is empty. If there are multiple items,
an error is signaled to the evaluation environment. This function is
useful for ensuring that an error is returned if an assumption about
cardinality is violated at run-time.

The following example returns the name of the Patient if there is one.
If there are no names, an empty collection, and if there are multiple
names, an error is signaled to the evaluation environment:

``` fhirpath
Patient.name.single()
```

#### first() : collection {#first--collection}

Returns a collection containing only the first item in the input
collection. This function is equivalent to `item[0]`{.language-plaintext
.highlighter-rouge}, so it will return an empty collection if the input
collection has no items.

#### last() : collection {#last--collection}

Returns a collection containing only the last item in the input
collection. Will return an empty collection if the input collection has
no items.

#### tail() : collection {#tail--collection}

Returns a collection containing all but the first item in the input
collection. Will return an empty collection if the input collection has
no items, or only one item.

#### skip(num : Integer) : collection {#skipnum--integer--collection}

Returns a collection containing all but the first
`num`{.language-plaintext .highlighter-rouge} items in the input
collection. Will return an empty collection if there are no items
remaining after the indicated number of items have been skipped, or if
the input collection is empty. If `num`{.language-plaintext
.highlighter-rouge} is less than or equal to zero, the input collection
is simply returned.

#### take(num : Integer) : collection {#takenum--integer--collection}

Returns a collection containing the first `num`{.language-plaintext
.highlighter-rouge} items in the input collection, or less if there are
less than `num`{.language-plaintext .highlighter-rouge} items. If num is
less than or equal to 0, or if the input collection is empty
(`{ }`{.language-plaintext .highlighter-rouge}),
`take`{.language-plaintext .highlighter-rouge} returns an empty
collection.

#### intersect(other: collection) : collection {#intersectother-collection--collection}

Returns the set of elements that are in both collections. Duplicate
items will be eliminated by this function. Order of items is not
guaranteed to be preserved in the result of this function.

#### exclude(other: collection) : collection {#excludeother-collection--collection}

Returns the set of elements that are not in the
`other`{.language-plaintext .highlighter-rouge} collection. Duplicate
items will not be eliminated by this function, and order will be
preserved.

e.g. `(1 | 2 | 3).exclude(2)`{.language-plaintext .fhirpath
.highlighter-rouge} returns `(1 | 3)`{.language-plaintext
.highlighter-rouge}.

### Combining

#### []{#unionother-collection}union(other : collection) {#unionother--collection}

Merge the two collections into a single collection, eliminating any
duplicate values (using [equals](#equals) (`=`{.language-plaintext
.highlighter-rouge}) to determine equality). There is no expectation of
order in the resulting collection.

In other words, this function returns the distinct list of elements from
both inputs. For example, consider two lists of integers
`A: 1, 1, 2, 3`{.language-plaintext .highlighter-rouge} and
`B: 2, 3`{.language-plaintext .highlighter-rouge}:

``` fhirpath
A.union( B ) // 1, 2, 3
A.union( { } ) // 1, 2, 3
```

This function can also be invoked using the `|`{.language-plaintext
.highlighter-rouge} operator.

e.g. `x.union(y)`{.language-plaintext .fhirpath .highlighter-rouge} is
synonymous with `x | y`{.language-plaintext .fhirpath
.highlighter-rouge}

e.g. `name.select(use.union(given))`{.language-plaintext .fhirpath
.highlighter-rouge} is the same as
`name.select(use | given)`{.language-plaintext .fhirpath
.highlighter-rouge}, noting that the union function does not introduce
an iteration context, in this example the select introduces the
iteration context on the name property.

#### combine(other : collection) : collection {#combineother--collection--collection}

Merge the input and other collections into a single collection without
eliminating duplicate values. Combining an empty collection with a
non-empty collection will return the non-empty collection. There is no
expectation of order in the resulting collection.

### Conversion

FHIRPath defines both *implicit* and *explicit* conversion. Implicit
conversions occur automatically, as opposed to explicit conversions that
require the function be called explicitly. Implicit conversion is
performed when an operator or function is used with a compatible type.
For example:

``` fhirpath
5 + 10.0
```

In the above expression, the addition operator expects either two
Integers, or two Decimals, so implicit conversion is used to convert the
integer to a decimal, resulting in decimal addition.

The following table lists the possible conversions supported, and
whether the conversion is implicit or explicit:

  From\\To           Boolean      Integer      Long *(STU)*   Decimal      Quantity   String       Date       DateTime   Time
  ------------------ ------------ ------------ -------------- ------------ ---------- ------------ ---------- ---------- ----------
  **Boolean**        N/A          Explicit     *Explicit*     Explicit     \-         Explicit     \-         \-         \-
  **Integer**        Explicit     N/A          *Implicit*     Implicit     Implicit   Explicit     \-         \-         \-
  **Long** *(STU)*   *Explicit*   *Explicit*   *N/A*          *Implicit*   *-*        *Explicit*   *-*        *-*        *-*
  **Decimal**        Explicit     \-           *-*            N/A          Implicit   Explicit     \-         \-         \-
  **Quantity**       \-           \-           *-*            \-           N/A        Explicit     \-         \-         \-
  **String**         Explicit     Explicit     *Explicit*     Explicit     Explicit   N/A          Explicit   Explicit   Explicit
  **Date**           \-           \-           *-*            \-           \-         Explicit     N/A        Implicit   \-
  **DateTime**       \-           \-           *-*            \-           \-         Explicit     Explicit   N/A        \-
  **Time**           \-           \-           *-*            \-           \-         Explicit     \-         \-         N/A

-   Implicit - Values of the type in the From column will be implicitly
    converted to values of the type in the To column when necessary
-   Explicit - Values of the type in the From column can be explicitly
    converted using a function defined in this section
-   N/A - Not applicable
-   \- No conversion is defined

The functions in this section operate on collections with a single item.
If there is more than one item, the evaluation of the expression will
end and signal an error to the calling environment.

[]{#iif}

#### iif(criterion: expression, true-result: collection \[, otherwise-result: collection\]) : collection {#iifcriterion-expression-true-result-collection--otherwise-result-collection--collection}

The `iif`{.language-plaintext .highlighter-rouge} function in FHIRPath
is an *immediate if*, also known as a conditional operator (such as C\'s
`? :`{.language-plaintext .highlighter-rouge} operator).

The `criterion`{.language-plaintext .highlighter-rouge} expression is
expected to evaluate to a Boolean.

If `criterion`{.language-plaintext .highlighter-rouge} is true, the
function returns the value of the `true-result`{.language-plaintext
.highlighter-rouge} argument.

If `criterion`{.language-plaintext .highlighter-rouge} is
`false`{.language-plaintext .highlighter-rouge} or an empty collection,
the function returns `otherwise-result`{.language-plaintext
.highlighter-rouge}, unless the optional
`otherwise-result`{.language-plaintext .highlighter-rouge} is not given,
in which case the function returns an empty collection.

Note that short-circuit behavior is expected in this function. In other
words, `true-result`{.language-plaintext .highlighter-rouge} should only
be evaluated if the `criterion`{.language-plaintext .highlighter-rouge}
evaluates to true, and `otherwise-result`{.language-plaintext
.highlighter-rouge} should only be evaluated otherwise. For
implementations, this means delaying evaluation of the arguments.

#### Boolean Conversion Functions

##### toBoolean() : Boolean {#toboolean--boolean}

If the input collection contains a single item, this function will
return a single boolean if:

-   the item is a Boolean
-   the item is an Integer and is equal to one of the possible integer
    representations of Boolean values
-   the item is a Decimal that is equal to one of the possible decimal
    representations of Boolean values
-   the item is a String that is equal to one of the possible string
    representations of Boolean values

If the item is not one the above types, or the item is a String,
Integer, or Decimal, but is not equal to one of the possible values
convertible to a Boolean, the result is empty.

The following table describes the possible values convertible to an
Boolean:

  Type          Representation                                                                                                                                                                                                                                                                                    Result
  ------------- ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------
  **String**    `'true'`{.language-plaintext .highlighter-rouge}, `'t'`{.language-plaintext .highlighter-rouge}, `'yes'`{.language-plaintext .highlighter-rouge}, `'y'`{.language-plaintext .highlighter-rouge}, `'1'`{.language-plaintext .highlighter-rouge}, `'1.0'`{.language-plaintext .highlighter-rouge}   `true`{.language-plaintext .highlighter-rouge}
                `'false'`{.language-plaintext .highlighter-rouge}, `'f'`{.language-plaintext .highlighter-rouge}, `'no'`{.language-plaintext .highlighter-rouge}, `'n'`{.language-plaintext .highlighter-rouge}, `'0'`{.language-plaintext .highlighter-rouge}, `'0.0'`{.language-plaintext .highlighter-rouge}   `false`{.language-plaintext .highlighter-rouge}
  **Integer**   `1`{.language-plaintext .highlighter-rouge}                                                                                                                                                                                                                                                       `true`{.language-plaintext .highlighter-rouge}
                `0`{.language-plaintext .highlighter-rouge}                                                                                                                                                                                                                                                       `false`{.language-plaintext .highlighter-rouge}
  **Decimal**   `1.0`{.language-plaintext .highlighter-rouge}                                                                                                                                                                                                                                                     `true`{.language-plaintext .highlighter-rouge}
                `0.0`{.language-plaintext .highlighter-rouge}                                                                                                                                                                                                                                                     `false`{.language-plaintext .highlighter-rouge}

Note for the purposes of string representations, case is ignored (so
that both `'T'`{.language-plaintext .highlighter-rouge} and
`'t'`{.language-plaintext .highlighter-rouge} are considered
`true`{.language-plaintext .highlighter-rouge}).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToBoolean() : Boolean {#convertstoboolean--boolean}

If the input collection contains a single item, this function will
return true if:

-   the item is a Boolean
-   the item is an Integer that is equal to one of the possible integer
    representations of Boolean values
-   the item is a Decimal that is equal to one of the possible decimal
    representations of Boolean values
-   the item is a String that is equal to one of the possible string
    representations of Boolean values

If the item is not one of the above types, or the item is a String,
Integer, or Decimal, but is not equal to one of the possible values
convertible to a Boolean, the result is false.

Possible values for Integer, Decimal, and String are described in the
toBoolean() function.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

#### Integer Conversion Functions

##### toInteger() : Integer {#tointeger--integer}

If the input collection contains a single item, this function will
return a single integer if:

-   the item is an Integer
-   the item is a String and is convertible to an integer
-   the item is a Boolean, where `true`{.language-plaintext
    .highlighter-rouge} results in a 1 and `false`{.language-plaintext
    .highlighter-rouge} results in a 0.

If the item is not one the above types, the result is empty.

If the item is a String, but the string is not convertible to an integer
(using the regex format `(\+|-)?\d+`{.language-plaintext
.highlighter-rouge}), the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToInteger() : Boolean {#convertstointeger--boolean}

If the input collection contains a single item, this function will
return true if:

-   the item is an Integer
-   the item is a String and is convertible to an Integer
-   the item is a Boolean

If the item is not one of the above types, or the item is a String, but
is not convertible to an Integer (using the regex format
`(\+|-)?\d+`{.language-plaintext .highlighter-rouge}), the result is
false.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### toLong() : Long {#tolong--long}

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

If the input collection contains a single item, this function will
return a single integer if:

-   the item is an Integer or Long
-   the item is a String and is convertible to a 64 bit integer
-   the item is a Boolean, where `true`{.language-plaintext
    .highlighter-rouge} results in a 1 and `false`{.language-plaintext
    .highlighter-rouge} results in a 0.

If the item is not one the above types, the result is empty.

If the item is a String, but the string is not convertible to a 64 bit
integer (using the regex format `(\+|-)?\d+`{.language-plaintext
.highlighter-rouge}), the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToLong() : Boolean {#convertstolong--boolean .stu}

If the input collection contains a single item, this function will
return true if:

-   the item is an Integer or Long
-   the item is a String and is convertible to a Long
-   the item is a Boolean

If the item is not one of the above types, or the item is a String, but
is not convertible to an Integer (using the regex format
`(\+|-)?\d+`{.language-plaintext .highlighter-rouge}), the result is
false.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

#### Date Conversion Functions

##### toDate() : Date {#todate--date}

If the input collection contains a single item, this function will
return a single date if:

-   the item is a Date
-   the item is a DateTime
-   the item is a String and is convertible to a Date

If the item is not one of the above types, the result is empty.

If the item is a String, but the string is not convertible to a Date
(using the format **YYYY-MM-DD**), the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToDate() : Boolean {#convertstodate--boolean}

If the input collection contains a single item, this function will
return true if:

-   the item is a Date
-   the item is a DateTime
-   the item is a String and is convertible to a Date

If the item is not one of the above types, or is not convertible to a
Date (using the format **YYYY-MM-DD**), the result is false.

If the item contains a partial date (e.g.
`'2012-01'`{.language-plaintext .highlighter-rouge}), the result is a
partial date.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

#### DateTime Conversion Functions

##### toDateTime() : DateTime {#todatetime--datetime}

If the input collection contains a single item, this function will
return a single datetime if:

-   the item is a DateTime
-   the item is a Date, in which case the result is a DateTime with the
    year, month, and day of the Date, and the time components empty (not
    set to zero)
-   the item is a String and is convertible to a DateTime

If the item is not one of the above types, the result is empty.

If the item is a String, but the string is not convertible to a DateTime
(using the format **YYYY-MM-DDThh:mm:ss.fff(+\|-)hh:mm**), the result is
empty.

If the item contains a partial datetime (e.g.
`'2012-01-01T10:00'`{.language-plaintext .highlighter-rouge}), the
result is a partial datetime.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToDateTime() : Boolean {#convertstodatetime--boolean}

If the input collection contains a single item, this function will
return true if:

-   the item is a DateTime
-   the item is a Date
-   the item is a String and is convertible to a DateTime

If the item is not one of the above types, or is not convertible to a
DateTime (using the format **YYYY-MM-DDThh:mm:ss.fff(+\|-)hh:mm**), the
result is false.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

#### Decimal Conversion Functions

##### toDecimal() : Decimal {#todecimal--decimal}

If the input collection contains a single item, this function will
return a single decimal if:

-   the item is an Integer or Decimal
-   the item is a String and is convertible to a Decimal
-   the item is a Boolean, where `true`{.language-plaintext
    .highlighter-rouge} results in a `1.0`{.language-plaintext
    .highlighter-rouge} and `false`{.language-plaintext
    .highlighter-rouge} results in a `0.0`{.language-plaintext
    .highlighter-rouge}.

If the item is not one of the above types, the result is empty.

If the item is a String, but the string is not convertible to a Decimal
(using the regex format `(\+|-)?\d+(\.\d+)?`{.language-plaintext
.highlighter-rouge}), the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToDecimal() : Boolean {#convertstodecimal--boolean}

If the input collection contains a single item, this function will true
if:

-   the item is an Integer or Decimal
-   the item is a String and is convertible to a Decimal
-   the item is a Boolean

If the item is not one of the above types, or is not convertible to a
Decimal (using the regex format `(\+|-)?\d+(\.\d+)?`{.language-plaintext
.highlighter-rouge}), the result is false.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

#### Quantity Conversion Functions

##### toQuantity(\[unit : String\]) : Quantity {#toquantityunit--string--quantity}

If the input collection contains a single item, this function will
return a single quantity if:

-   the item is an Integer, or Decimal, where the resulting quantity
    will have the default unit (`'1'`{.language-plaintext
    .highlighter-rouge})
-   the item is a Quantity
-   the item is a String and is convertible to a Quantity
-   the item is a Boolean, where `true`{.language-plaintext
    .highlighter-rouge} results in the quantity
    `1.0 '1'`{.language-plaintext .highlighter-rouge}, and
    `false`{.language-plaintext .highlighter-rouge} results in the
    quantity `0.0 '1'`{.language-plaintext .highlighter-rouge}

If the item is not one of the above types, the result is empty.

If the item is a String, but the string is not convertible to a Quantity
using the following regex format:

``` regex
(?'value'(\+|-)?\d+(\.\d+)?)\s*('(?'unit'[^']+)'|(?'time'[a-zA-Z]+))?
```

then the result is empty. For example, the following are valid quantity
strings:

``` fhirpath
'4 days'
'10 \'mg[Hg]\''
```

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

If the `unit`{.language-plaintext .highlighter-rouge} argument is
provided, it must be the string representation of a UCUM code (or a
FHIRPath calendar duration keyword), and is used to determine whether
the input quantity can be converted to the given unit, according to the
unit conversion rules specified by UCUM. If the input quantity can be
converted, the result is the converted quantity, otherwise, the result
is empty.

For calendar durations, FHIRPath defines the following conversion
factors:

  Calendar duration                                    Conversion factor
  ---------------------------------------------------- -----------------------------------------------------------------------------------------------------------
  `1 year`{.language-plaintext .highlighter-rouge}     `12 months`{.language-plaintext .highlighter-rouge} or `365 days`{.language-plaintext .highlighter-rouge}
  `1 month`{.language-plaintext .highlighter-rouge}    `30 days`{.language-plaintext .highlighter-rouge}
  `1 day`{.language-plaintext .highlighter-rouge}      `24 hours`{.language-plaintext .highlighter-rouge}
  `1 hour`{.language-plaintext .highlighter-rouge}     `60 minutes`{.language-plaintext .highlighter-rouge}
  `1 minute`{.language-plaintext .highlighter-rouge}   `60 seconds`{.language-plaintext .highlighter-rouge}
  `1 second`{.language-plaintext .highlighter-rouge}   `1 's'`{.language-plaintext .highlighter-rouge}

Note that calendar duration conversion factors are only used when
time-valued quantities appear in unanchored calculations. See [Date/Time
Arithmetic](#datetime-arithmetic) for more information on using
time-valued quantities in FHIRPath.

If `q`{.language-plaintext .highlighter-rouge} is a Quantity of
`'kg'`{.language-plaintext .highlighter-rouge} and one wants to convert
to a Quantity in `'g'`{.language-plaintext .highlighter-rouge} (grams):

``` fhirpath
q.toQuantity('g') // changes the value and units in the quantity according to UCUM conversion rules
```

> Implementations are not required to support a complete UCUM
> implementation, and may return empty (`{ }`{.language-plaintext
> .highlighter-rouge}) when the `unit`{.language-plaintext
> .highlighter-rouge} argument is used and it is different than the
> input quantity unit.

##### convertsToQuantity(\[unit : String\]) : Boolean {#convertstoquantityunit--string--boolean}

If the input collection contains a single item, this function will
return true if:

-   the item is an Integer, Decimal, or Quantity
-   the item is a String that is convertible to a Quantity
-   the item is a Boolean

If the item is not one of the above types, or is not convertible to a
Quantity using the following regex format:

``` regex
(?'value'(\+|-)?\d+(\.\d+)?)\s*('(?'unit'[^']+)'|(?'time'[a-zA-Z]+))?
```

then the result is false.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

If the `unit`{.language-plaintext .highlighter-rouge} argument is
provided, it must be the string representation of a UCUM code (or a
FHIRPath calendar duration keyword), and is used to determine whether
the input quantity can be converted to the given unit, according to the
unit conversion rules specified by UCUM. If the input quantity can be
converted, the result is true, otherwise, the result is false.

> Implementations are not required to support a complete UCUM
> implementation, and may return false when the
> `unit`{.language-plaintext .highlighter-rouge} argument is used and it
> is different than the input quantity unit.

#### String Conversion Functions

##### toString() : String {#tostring--string}

If the input collection contains a single item, this function will
return a single String if:

-   the item in the input collection is a String
-   the item in the input collection is an Integer, Decimal, Date, Time,
    DateTime, or Quantity the output will contain its String
    representation
-   the item is a Boolean, where `true`{.language-plaintext
    .highlighter-rouge} results in `'true'`{.language-plaintext
    .highlighter-rouge} and `false`{.language-plaintext
    .highlighter-rouge} in `'false'`{.language-plaintext
    .highlighter-rouge}.

If the item is not one of the above types, the result is false.

The String representation uses the following formats:

  Type           Representation
  -------------- ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **Boolean**    `true`{.language-plaintext .highlighter-rouge} or `false`{.language-plaintext .highlighter-rouge}
  **Integer**    `(\+|-)?\d+`{.language-plaintext .highlighter-rouge}
  **Decimal**    `(\+|-)?\d+(.\d+)?`{.language-plaintext .highlighter-rouge}
  **Quantity**   `(\+|-)?\d+(.\d+)? '.*'`{.language-plaintext .highlighter-rouge} e.g. `(4 days).toString()`{.language-plaintext .fhirpath .highlighter-rouge} returns `4 'd'`{.language-plaintext .highlighter-rouge} because the FHIRPath literal temporal units are short-hands for the UCUM equivalents.
  **Date**       **YYYY-MM-DD**
  **DateTime**   **YYYY-MM-DDThh:mm:ss.fff(+\|-)hh:mm**
  **Time**       **hh:mm:ss.fff(+\|-)hh:mm**

Note that for partial dates and times, the result will only be specified
to the level of precision in the value being converted.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToString() : String {#convertstostring--string}

If the input collection contains a single item, this function will
return true if:

-   the item is a String
-   the item is an Integer, Decimal, Date, Time, or DateTime
-   the item is a Boolean
-   the item is a Quantity

If the item is not one of the above types, the result is false.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

#### Time Conversion Functions

##### toTime() : Time {#totime--time}

If the input collection contains a single item, this function will
return a single time if:

-   the item is a Time
-   the item is a String and is convertible to a Time

If the item is not one of the above types, the result is empty.

If the item is a String, but the string is not convertible to a Time
(using the format **hh:mm:ss.fff(+\|-)hh:mm**), the result is empty.

If the item contains a partial time (e.g. `'10:00'`{.language-plaintext
.highlighter-rouge}), the result is a partial time.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToTime() : Boolean {#convertstotime--boolean}

If the input collection contains a single item, this function will
return true if:

-   the item is a Time
-   the item is a String and is convertible to a Time

If the item is not one of the above types, or is not convertible to a
Time (using the format **hh:mm:ss.fff(+\|-)hh:mm**), the result is
false.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

### String Manipulation

The functions in this section operate on collections with a single item.
If there is more than one item, or an item that is not a String, the
evaluation of the expression will end and signal an error to the calling
environment.

To use these functions over a collection with multiple items, one may
use filters like `where()`{.language-plaintext .highlighter-rouge} and
`select()`{.language-plaintext .highlighter-rouge}:

``` fhirpath
Patient.name.given.select(substring(0))
```

This example returns a collection containing the first character of all
the given names for a patient.

#### indexOf(substring : String) : Integer {#indexofsubstring--string--integer}

Returns the 0-based index of the first position
`substring`{.language-plaintext .highlighter-rouge} is found in the
input string, or -1 if it is not found.

If `substring`{.language-plaintext .highlighter-rouge} is an empty
string (`''`{.language-plaintext .highlighter-rouge}), the function
returns 0.

If the input or `substring`{.language-plaintext .highlighter-rouge} is
empty (`{ }`{.language-plaintext .highlighter-rouge}), the result is
empty (`{ }`{.language-plaintext .highlighter-rouge}).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abcdefg'.indexOf('bc') // 1
'abcdefg'.indexOf('x') // -1
'abcdefg'.indexOf('abcdefg') // 0
```

#### lastIndexOf(substring : String) : Integer {#lastindexofsubstring--string--integer}

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

Returns the 0-based index of the last position
`substring`{.language-plaintext .highlighter-rouge} is found in the
input string, or -1 if it is not found.

If `substring`{.language-plaintext .highlighter-rouge} is an empty
string (`''`{.language-plaintext .highlighter-rouge}), the function
returns 0.

If the input or `substring`{.language-plaintext .highlighter-rouge} is
empty (`{ }`{.language-plaintext .highlighter-rouge}), the result is
empty (`{ }`{.language-plaintext .highlighter-rouge}).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
'abcdefg'.lastIndexOf('bc') // 1
'abcdefg'.lastIndexOf('x') // -1
'abcdefg'.lastIndexOf('abcdefg') // 0
'abc abc'.lastIndexOf('a') // 4
```

#### substring(start : Integer \[, length : Integer\]) : String {#substringstart--integer--length--integer--string}

Returns the part of the string starting at position
`start`{.language-plaintext .highlighter-rouge} (zero-based). If
`length`{.language-plaintext .highlighter-rouge} is given, will return
at most `length`{.language-plaintext .highlighter-rouge} number of
characters from the input string.

If `start`{.language-plaintext .highlighter-rouge} lies outside the
length of the string, the function returns empty
(`{ }`{.language-plaintext .highlighter-rouge}). If there are less
remaining characters in the string than indicated by
`length`{.language-plaintext .highlighter-rouge}, the function returns
just the remaining characters.

If the input or `start`{.language-plaintext .highlighter-rouge} is
empty, the result is empty.

If an empty `length`{.language-plaintext .highlighter-rouge} is
provided, the behavior is the same as if `length`{.language-plaintext
.highlighter-rouge} had not been provided.

If a negative or zero `length`{.language-plaintext .highlighter-rouge}
is provided, the function returns an empty string
(`''`{.language-plaintext .highlighter-rouge}).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abcdefg'.substring(3) // 'defg'
'abcdefg'.substring(1, 2) // 'bc'
'abcdefg'.substring(6, 2) // 'g'
'abcdefg'.substring(7, 1) // { } (start position is outside the string)
'abcdefg'.substring(-1, 1) // { } (start position is outside the string,
                           //     this can happen when the -1 was the result of a calculation rather than explicitly provided)
'abcdefg'.substring(3, 0) // '' (empty string)
'abcdefg'.substring(3, -1) // '' (empty string)
'abcdefg'.substring(-1, -1) // {} (start position is outside the string)
```

#### startsWith(prefix : String) : Boolean {#startswithprefix--string--boolean}

Returns `true`{.language-plaintext .highlighter-rouge} when the input
string starts with the given `prefix`{.language-plaintext
.highlighter-rouge}.

If `prefix`{.language-plaintext .highlighter-rouge} is the empty string
(`''`{.language-plaintext .highlighter-rouge}), the result is
`true`{.language-plaintext .highlighter-rouge}.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abcdefg'.startsWith('abc') // true
'abcdefg'.startsWith('xyz') // false
```

#### endsWith(suffix : String) : Boolean {#endswithsuffix--string--boolean}

Returns `true`{.language-plaintext .highlighter-rouge} when the input
string ends with the given `suffix`{.language-plaintext
.highlighter-rouge}.

If `suffix`{.language-plaintext .highlighter-rouge} is the empty string
(`''`{.language-plaintext .highlighter-rouge}), the result is
`true`{.language-plaintext .highlighter-rouge}.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abcdefg'.endsWith('efg') // true
'abcdefg'.endsWith('abc') // false
```

#### contains(substring : String) : Boolean {#containssubstring--string--boolean}

Returns `true`{.language-plaintext .highlighter-rouge} when the given
`substring`{.language-plaintext .highlighter-rouge} is a substring of
the input string.

If `substring`{.language-plaintext .highlighter-rouge} is the empty
string (`''`{.language-plaintext .highlighter-rouge}), the result is
`true`{.language-plaintext .highlighter-rouge}.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abc'.contains('b') // true
'abc'.contains('bc') // true
'abc'.contains('d') // false
```

> **Note:** The `.contains()`{.language-plaintext .highlighter-rouge}
> function described here is a string function that looks for a
> substring in a string. This is different than the
> `contains`{.language-plaintext .highlighter-rouge} operator, which is
> a list operator that looks for an element in a list.

#### upper() : String {#upper--string}

Returns the input string with all characters converted to upper case.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abcdefg'.upper() // 'ABCDEFG'
'AbCdefg'.upper() // 'ABCDEFG'
```

#### lower() : String {#lower--string}

Returns the input string with all characters converted to lower case.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'ABCDEFG'.lower() // 'abcdefg'
'aBcDEFG'.lower() // 'abcdefg'
```

#### replace(pattern : String, substitution : String) : String {#replacepattern--string-substitution--string--string}

Returns the input string with all instances of
`pattern`{.language-plaintext .highlighter-rouge} replaced with
`substitution`{.language-plaintext .highlighter-rouge}. If the
substitution is the empty string (`''`{.language-plaintext
.highlighter-rouge}), instances of `pattern`{.language-plaintext
.highlighter-rouge} are removed from the result. If
`pattern`{.language-plaintext .highlighter-rouge} is the empty string
(`''`{.language-plaintext .highlighter-rouge}), every character in the
input string is surrounded by the substitution, e.g.
`'abc'.replace('','x')`{.language-plaintext .fhirpath
.highlighter-rouge} becomes `'xaxbxcx'`{.language-plaintext
.highlighter-rouge}.

If the input collection, `pattern`{.language-plaintext
.highlighter-rouge}, or `substitution`{.language-plaintext
.highlighter-rouge} are empty, the result is empty
(`{ }`{.language-plaintext .highlighter-rouge}).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abcdefg'.replace('cde', '123') // 'ab123fg'
'abcdefg'.replace('cde', '') // 'abfg'
'abc'.replace('', 'x') // 'xaxbxcx'
```

#### matches(regex : String) : Boolean {#matchesregex--string--boolean}

Returns `true`{.language-plaintext .highlighter-rouge} when the value
matches the given regular expression. Regular expressions should
function consistently, regardless of any culture- and locale-specific
settings in the environment, should be case-sensitive, use \'single
line\' mode and allow Unicode characters. The start/end of line markers
`^`{.language-plaintext .highlighter-rouge}, `$`{.language-plaintext
.highlighter-rouge} can be used to match the entire string.

If the input collection or `regex`{.language-plaintext
.highlighter-rouge} are empty, the result is empty
(`{ }`{.language-plaintext .highlighter-rouge}).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'http://fhir.org/guides/cqf/common/Library/FHIR-ModelInfo|4.0.1'.matches('Library') // returns true
'N8000123123'.matches('^N[0-9]{8}$') // returns false as the string is not an 8 char number (it has 10)
'N8000123123'.matches('N[0-9]{8}') // returns true as the string has an 8 number sequence in it starting with `N`
```

#### matchesFull(regex : String) : Boolean {#matchesfullregex--string--boolean}

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

Returns `true`{.language-plaintext .highlighter-rouge} when the value
completely matches the given regular expression (implying that the
start/end of line markers `^`{.language-plaintext .highlighter-rouge},
`$`{.language-plaintext .highlighter-rouge} are always surrounding the
regex expression provided).

Regular expressions should function consistently, regardless of any
culture- and locale-specific settings in the environment, should be
case-sensitive, use \'single line\' mode and allow Unicode characters.

If the input collection or `regex`{.language-plaintext
.highlighter-rouge} are empty, the result is empty
(`{ }`{.language-plaintext .highlighter-rouge}).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
'http://fhir.org/guides/cqf/common/Library/FHIR-ModelInfo|4.0.1'.matchesFull('Library') // returns false
'N8000123123'.matchesFull('N[0-9]{8}') // returns false as the string is not an 8 char number (it has 10)
'N8000123123'.matchesFull('N[0-9]{10}') // returns true as the string has an 10 number sequence in it starting with `N`
```

#### replaceMatches(regex : String, substitution: String) : String {#replacematchesregex--string-substitution-string--string}

Matches the input using the regular expression in
`regex`{.language-plaintext .highlighter-rouge} and replaces each match
with the `substitution`{.language-plaintext .highlighter-rouge} string.
The substitution may refer to identified match groups in the regular
expression.

If the input collection, `regex`{.language-plaintext
.highlighter-rouge}, or `substitution`{.language-plaintext
.highlighter-rouge} are empty, the result is empty
(`{ }`{.language-plaintext .highlighter-rouge}).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

This example of `replaceMatches()`{.language-plaintext
.highlighter-rouge} will convert a string with a date formatted as
MM/dd/yy to dd-MM-yy:

``` fhirpath
'11/30/1972'.replaceMatches('\\b(?<month>\\d{1,2})/(?<day>\\d{1,2})/(?<year>\\d{2,4})\\b',
       '${day}-${month}-${year}')
```

> **Note:** Platforms will typically use native regular expression
> implementations. These are typically fairly similar, but there will
> always be small differences. As such, FHIRPath does not prescribe a
> particular dialect, but recommends the use of the [\[PCRE\]](#PCRE)
> flavor as the dialect most likely to be broadly supported and
> understood.

#### length() : Integer {#length--integer}

Returns the length of the input string. If the input collection is empty
(`{ }`{.language-plaintext .highlighter-rouge}), the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

#### toChars() : collection {#tochars--collection}

Returns the list of characters in the input string. If the input
collection is empty (`{ }`{.language-plaintext .highlighter-rouge}), the
result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abc'.toChars() // { 'a', 'b', 'c' }
```

### Additional String Functions

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

#### encode(format : String) : String {#encodeformat--string--string .stu}

The encode function takes a singleton string and returns the result of
encoding that string in the given format. The format parameter defines
the encoding format. Available formats are:

  ----------- ------------------------------------------------------------------------------------------------------------
  hex         The string is encoded using hexadecimal characters (base 16) in lowercase
  base64      The string is encoded using standard base64 encoding, using A-Z, a-z, 0-9, +, and /, output padded with =)
  urlbase64   The string is encoded using url base 64 encoding, using A-Z, a-z, 0-9, -, and \_, output padded with =)
  ----------- ------------------------------------------------------------------------------------------------------------

Base64 encodings are described in
[RFC4648](https://tools.ietf.org/html/rfc4648#section-4).

If the input is empty, the result is empty.

If no format is specified, the result is empty.

#### decode(format : String) : String {#decodeformat--string--string .stu}

The decode function takes a singleton encoded string and returns the
result of decoding that string according to the given format. The format
parameter defines the encoding format. Available formats are listed in
the encode function.

If the input is empty, the result is empty.

If no format is specified, the result is empty.

#### escape(target : String) : String {#escapetarget--string--string .stu}

The escape function takes a singleton string and escapes it for a given
target, as specified in the following table:

  ------ ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  html   The string is escaped such that it can appear as valid HTML content (at least open bracket (`<`{.language-plaintext .highlighter-rouge}), ampersand (`&`{.language-plaintext .highlighter-rouge}), and quotes (`"`{.language-plaintext .highlighter-rouge}), but ideally anything with a character encoding above 127)
  json   The string is escaped such that it can appear as a valid JSON string (quotes (`"`{.language-plaintext .highlighter-rouge}) are escaped as (`\"`{.language-plaintext .highlighter-rouge})); additional escape characters are described in the [String](#string) escape section
  ------ ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

If the input is empty, the result is empty.

If no target is specified, the result is empty.

#### unescape(target : String) : String {#unescapetarget--string--string .stu}

The unescape function takes a singleton string and unescapes it for a
given target. The available targets are specified in the escape function
description.

If the input is empty, the result is empty.

If no target is specified, the result is empty.

#### trim() : String {#trim--string .stu}

The trim function trims whitespace characters from the beginning and
ending of the input string, with whitespace characters as defined in the
[Whitespace](#whitespace) lexical category.

If the input is empty, the result is empty.

#### split(separator: String) : collection {#splitseparator-string--collection .stu}

The split function splits a singleton input string into a list of
strings, using the given separator.

If the input is empty, the result is empty.

If the input string does not contain any appearances of the separator,
the result is the input string.

The following example illustrates the behavior of the
`.split`{.language-plaintext .highlighter-rouge} operator:

``` stu
('A,B,C').split(',') // { 'A', 'B', 'C' }
('ABC').split(',') // { 'ABC' }
'A,,C'.split(',') // { 'A', '', 'C' }
```

#### join(\[separator: String\]) : String {#joinseparator-string--string .stu}

The join function takes a collection of strings and *joins* them into a
single string, optionally using the given separator.

If the input is empty, the result is empty.

If no separator is specified, the strings are directly concatenated.

The following example illustrates the behavior of the
`.join`{.language-plaintext .highlighter-rouge} operator:

``` stu
('A' | 'B' | 'C').join() // 'ABC'
('A' | 'B' | 'C').join(',') // 'A,B,C'
```

### Math

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

The functions in this section operate on collections with a single item.
Unless otherwise noted, if there is more than one item, or the item is
not compatible with the expected type, the evaluation of the expression
will end and signal an error to the calling environment.

Note also that although all functions return collections, if a given
function is defined to return a single element, the return type in the
description of the function is simplified to just the type of the single
element, rather than the list type.

The math functions in this section enable FHIRPath to be used not only
for path selection, but for providing a platform-independent
representation of calculation logic in artifacts such as questionnaires
and documentation templates. For example:

``` stu
(%weight/(%height.power(2))).round(1)
```

This example from a questionnaire calculates the Body Mass Index (BMI)
based on the responses to the weight and height elements. For more
information on the use of FHIRPath in questionnaires, see the
[Structured Data Capture](http://hl7.org/fhir/uv/sdc/) (SDC)
implementation guide.

#### abs() : Integer \| Decimal \| Quantity {#abs--integer--decimal--quantity .stu}

Returns the absolute value of the input. When taking the absolute value
of a quantity, the unit is unchanged.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
(-5).abs() // 5
(-5.5).abs() // 5.5
(-5.5 'mg').abs() // 5.5 'mg'
```

#### ceiling() : Integer {#ceiling--integer .stu}

Returns the first integer greater than or equal to the input.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
1.ceiling() // 1
1.1.ceiling() // 2
(-1.1).ceiling() // -1
```

#### exp() : Decimal {#exp--decimal .stu}

Returns *e* raised to the power of the input.

If the input collection contains an Integer, it will be implicitly
converted to a Decimal and the result will be a Decimal.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
0.exp() // 1.0
(-0.0).exp() // 1.0
```

#### floor() : Integer {#floor--integer .stu}

Returns the first integer less than or equal to the input.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
1.floor() // 1
2.1.floor() // 2
(-2.1).floor() // -3
```

#### ln() : Decimal {#ln--decimal .stu}

Returns the natural logarithm of the input (i.e. the logarithm base
*e*).

When used with an Integer, it will be implicitly converted to a Decimal.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
1.ln() // 0.0
1.0.ln() // 0.0
```

#### log(base : Decimal) : Decimal {#logbase--decimal--decimal .stu}

Returns the logarithm base `base`{.language-plaintext
.highlighter-rouge} of the input number.

When used with Integers, the arguments will be implicitly converted to
Decimal.

If `base`{.language-plaintext .highlighter-rouge} is empty, the result
is empty.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
16.log(2) // 4.0
100.0.log(10.0) // 2.0
```

#### power(exponent : Integer \| Decimal) : Integer \| Decimal {#powerexponent--integer--decimal--integer--decimal .stu}

Raises a number to the `exponent`{.language-plaintext
.highlighter-rouge} power. If this function is used with Integers, the
result is an Integer. If the function is used with Decimals, the result
is a Decimal. If the function is used with a mixture of Integer and
Decimal, the Integer is implicitly converted to a Decimal and the result
is a Decimal.

If the power cannot be represented (such as the -1 raised to the 0.5),
the result is empty.

If the input is empty, or exponent is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
2.power(3) // 8
2.5.power(2) // 6.25
(-1).power(0.5) // empty ({ })
```

#### round(\[precision : Integer\]) : Decimal {#roundprecision--integer--decimal}

> **Note:** The contents of this section are Standard for Trial Use
> (STU)
>
> [Discussion on this
> topic](https://chat.fhir.org/#narrow/stream/179266-fhirpath/topic/round.28.29.20for.20negative.20numbers)
> If you have specific proposals or feedback please log a change
> request.

Rounds the decimal to the nearest whole number using a traditional round
(i.e. 0.5 or higher will round to 1). If specified, the precision
argument determines the decimal place at which the rounding will occur.
If not specified, the rounding will default to 0 decimal places.

If specified, the number of digits of precision must be \>= 0 or the
evaluation will end and signal an error to the calling environment.

If the input collection contains a single item of type Integer, it will
be implicitly converted to a Decimal.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
1.round() // 1
3.14159.round(3) // 3.142
```

#### sqrt() : Decimal {#sqrt--decimal}

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

Returns the square root of the input number as a Decimal.

If the square root cannot be represented (such as the square root of
-1), the result is empty.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

Note that this function is equivalent to raising a number of the power
of 0.5 using the power() function.

``` stu
81.sqrt() // 9.0
(-1).sqrt() // empty
```

#### truncate() : Integer {#truncate--integer .stu}

Returns the integer portion of the input.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
101.truncate() // 101
1.00000001.truncate() // 1
(-1.56).truncate() // -1
```

### Tree navigation

#### children() : collection {#children--collection}

Returns a collection with all immediate child nodes of all items in the
input collection. Note that the ordering of the children is undefined
and using functions like `first()`{.language-plaintext
.highlighter-rouge} on the result may return different results on
different platforms.

#### descendants() : collection {#descendants--collection}

Returns a collection with all descendant nodes of all items in the input
collection. The result does not include the nodes in the input
collection themselves. This function is a shorthand for
`repeat(children())`{.language-plaintext .highlighter-rouge}. Note that
the ordering of the children is undefined and using functions like
`first()`{.language-plaintext .highlighter-rouge} on the result may
return different results on different platforms.

> **Note:** Many of these functions will result in a set of nodes of
> different underlying types. It may be necessary to use
> `ofType()`{.language-plaintext .highlighter-rouge} as described in the
> previous section to maintain type safety. See [Type safety and strict
> evaluation](#type-safety-and-strict-evaluation) for more information
> about type safe use of FHIRPath expressions.

### Utility functions

#### trace(name : String \[, projection: Expression\]) : collection {#tracename--string--projection-expression--collection}

Adds a String representation of the input collection to the diagnostic
log, using the `name`{.language-plaintext .highlighter-rouge} argument
as the name in the log. This log should be made available to the user in
some appropriate fashion. Does not change the input, so returns the
input collection as output.

If the `projection`{.language-plaintext .highlighter-rouge} argument is
used, the trace would log the result of evaluating the project
expression on the input, but still return the input to the trace
function unchanged.

``` fhirpath
contained.where(criteria).trace('unmatched', id).empty()
```

The above example traces only the id elements of the result of the
where.

#### Current date and time functions

The following functions return the current date and time. The timestamp
that these functions use is an implementation decision, and
implementations should consider providing options appropriate for their
environment. In the simplest case, the local server time is used as the
timestamp for these function.

To ensure deterministic evaluation, these operators should return the
same value regardless of how many times they are evaluated within any
given expression (i.e. now() should always return the same DateTime in a
given expression, timeOfDay() should always return the same Time in a
given expression, and today() should always return the same Date in a
given expression.)

##### now() : DateTime {#now--datetime}

Returns the current date and time, including timezone offset.

##### timeOfDay() : Time {#timeofday--time}

Returns the current time.

##### today() : Date {#today--date}

Returns the current date.

[]{#definevariable}

#### defineVariable(name: String \[, expr: expression\]) {#definevariablename-string--expr-expression}

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

Defines a variable named `name`{.language-plaintext .highlighter-rouge}
that is accessible in subsequent expressions and has the value of
`expr`{.language-plaintext .highlighter-rouge} if present, otherwise the
value of the input collection. In either case the function does not
change the input and the output is the same as the input collection.

If the name already exists in the current expression scope, the
evaluation will end and signal an error to the calling environment. Note
that functions that take an `expression`{.language-plaintext
.highlighter-rouge} as an argument establish a scope for the iteration
variables (\$this and \$index). If a variable is defined within such an
expression, it is only available within that expression scope.

Example:

``` stu
group.select(
  defineVariable('grp')
  .select(
    element.select(
      defineVariable('src')
      .target.select(
        %grp.source & '#' & %src.code
        & ' ' & equivalence & ' '
        & %grp.target & '#' & code
      )
    )
  )
)
```

> **Note:** this would be implemented using expression scoping on the
> variable stack and after expression completion the temporary variable
> would be popped off the stack.

#### lowBoundary(\[precision: Integer\]): Decimal \| Date \| DateTime \| Time {#lowboundaryprecision-integer-decimal--date--datetime--time .stu}

The least possible value of the input to the specified precision.

The function can only be used with Decimal, Date, DateTime, and Time
values, and returns the same type as the value in the input collection.

If no precision is specified, the greatest precision of the type of the
input value is used (i.e. at least 8 for Decimal, 4 for Date, at least
17 for DateTime, and at least 9 for Time).

If the precision is greater than the maximum possible precision of the
implementation, the result is empty *(CQL returns null)*.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
1.587.lowBoundary(8) // 1.58700000
@2014.lowBoundary(6) // @2014-01
@2014-01-01T08.lowBoundary(17) // @2014-01-01T08:00:00.000
@T10:30.lowBoundary(9) // @T10:30:00.000
```

#### highBoundary(\[precision: Integer\]): Decimal \| Date \| DateTime \| Time {#highboundaryprecision-integer-decimal--date--datetime--time .stu}

The greatest possible value of the input to the specified precision.

The function can only be used with Decimal, Date, DateTime, and Time
values, and returns the same type as the value in the input collection.

If no precision is specified, the greatest precision of the type of the
input value is used (i.e. at least 8 for Decimal, 4 for Date, at least
17 for DateTime, and at least 9 for Time).

If the precision is greater than the maximum possible precision of the
implementation, the result is empty *(CQL returns null)*.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
1.587.highBoundary(8) // 1.58799999
@2014.highBoundary(6) // @2014-12
@2014-01-01T08.highBoundary(17) // @2014-01-01T08:59:59.999
@T10:30.highBoundary(9) // @T10:30:59.999
```

#### precision() : Integer {#precision--integer .stu}

If the input collection contains a single item, this function will
return the number of digits of precision.

The function can only be used with Decimal, Date, DateTime, and Time
values.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

For Decimal values, the function returns the number of digits of
precision after the decimal place in the input value.

``` stu
1.58700.precision() // 5
```

For Date and DateTime values, the function returns the number of digits
of precision in the input value.

``` stu
@2014.precision() // 4
@2014-01-05T10:30:00.000.precision() // 17
@T10:30.precision() // 4
@T10:30:00.000.precision() // 9
```

#### Extract Date/DateTime/Time components {#extract-datedatetimetime-components .stu}

##### yearOf(): Integer {#yearof-integer .stu}

If the input collection contains a single Date or DateTime, this
function will return the year component.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2014-01-05T10:30:00.000.yearOf() // 2014
```

##### monthOf(): Integer {#monthof-integer .stu}

If the input collection contains a single Date or DateTime, this
function will return the month component.

If the input collection is empty, or the month is not present in the
value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2014-01-05T10:30:00.000.monthOf() // 1
```

If the component isn\'t present in the value, then the result is empty

``` stu
@2012.monthOf() // {} an empty collection
```

##### dayOf(): Integer {#dayof-integer .stu}

If the input collection contains a single Date or DateTime, this
function will return the day component.

If the input collection is empty, or the day is not present in the
value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2014-01-05T10:30:00.000.dayOf() // 5
```

##### hourOf(): Integer {#hourof-integer .stu}

If the input collection contains a single Date, DateTime or Time, this
function will return the hour component.

If the input collection is empty, or the hour is not present in the
value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T03:30:40.002-07:00.hourOf() // 3
@2012-01-01T16:30:40.002-07:00.hourOf() // 16
```

##### minuteOf(): Integer {#minuteof-integer .stu}

If the input collection contains a single Date, DateTime or Time, this
function will return the minute component.

If the input collection is empty, or the minute is not present in the
value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:40.002-07:00.minuteOf() // 30
```

##### secondOf(): Integer {#secondof-integer .stu}

If the input collection contains a single Date, DateTime or Time, this
function will return the second component.

If the input collection is empty, or the second is not present in the
value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:40.002-07:00.secondOf() // 40
```

##### millisecondOf(): Integer {#millisecondof-integer .stu}

If the input collection contains a single Date, DateTime or Time, this
function will return the millisecond component.

If the input collection is empty, or the millisecond is not present in
the value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:00.002-07:00.millisecondOf() // 2
```

##### timezoneOffsetOf(): Decimal {#timezoneoffsetof-decimal .stu}

If the input collection contains a single DateTime, this function will
return the timezone offset component.

If the input collection is empty, or the timezone offset is not present
in the value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:00.000-07:00.timezoneOffsetOf() // -7.0
```

##### dateOf(): Date {#dateof-date .stu}

If the input collection contains a single Date or DateTime, this
function will return the date component (up to the precision present in
the input value).

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:00.000-07:00.dateOf() // @2012-01-01
```

##### timeOf(): Time {#timeof-time .stu}

If the input collection contains a single DateTime, this function will
return the time component.

If the input collection is empty, or the time is not present in the
value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:00.000-07:00.timeOf() // @T12:30:00.000
```

## Operations

Operators are allowed to be used between any kind of path expressions
(e.g. expr op expr). Like functions, operators will generally propagate
an empty collection in any of their operands. This is true even when
comparing two empty collections using the equality operators, e.g.

``` fhirpath
{} = {}
true > {}
{} != 'dummy'
```

all result in `{}`{.language-plaintext .highlighter-rouge}.

### Equality

#### []{#equals}= (Equals) {#-equals}

Returns `true`{.language-plaintext .highlighter-rouge} if the left
collection is equal to the right collection:

As noted above, if either operand is an empty collection, the result is
an empty collection. Otherwise:

If both operands are collections with a single item, they must be of the
same type (or be implicitly convertible to the same type), and:

-   For primitives:
    -   `String`{.language-plaintext .highlighter-rouge}: comparison is
        based on Unicode values
    -   `Integer`{.language-plaintext .highlighter-rouge}: values must
        be exactly equal
    -   `Decimal`{.language-plaintext .highlighter-rouge}: values must
        be equal, trailing zeroes after the decimal are ignored
    -   `Boolean`{.language-plaintext .highlighter-rouge}: values must
        be the same
    -   `Date`{.language-plaintext .highlighter-rouge}: must be exactly
        the same
    -   `DateTime`{.language-plaintext .highlighter-rouge}: must be
        exactly the same, respecting the timezone offset (though +00:00
        = -00:00 = Z)
    -   `Time`{.language-plaintext .highlighter-rouge}: must be exactly
        the same
-   For complex types, equality requires all child properties to be
    equal, recursively.

If both operands are collections with multiple items:

-   Each item must be equal
-   Comparison is order dependent

Otherwise, equals returns `false`{.language-plaintext
.highlighter-rouge}.

Note that this implies that if the collections have a different number
of items to compare, the result will be `false`{.language-plaintext
.highlighter-rouge}.

Typically, this operator is used with single fixed values as operands.
This means that `Patient.telecom.system = 'phone'`{.language-plaintext
.fhirpath .highlighter-rouge} will result in an error if there is more
than one `telecom`{.language-plaintext .highlighter-rouge} with a
`use`{.language-plaintext .highlighter-rouge}. Typically, you\'d want
`Patient.telecom.where(system = 'phone')`{.language-plaintext .fhirpath
.highlighter-rouge}

If one or both of the operands is the empty collection, this operation
returns an empty collection.

##### Quantity Equality

When comparing quantities for equality, the dimensions of each quantity
must be the same, but not necessarily the unit. For example, units of
`'cm'`{.language-plaintext .highlighter-rouge} and
`'m'`{.language-plaintext .highlighter-rouge} can be compared, but units
of `'cm2'`{.language-plaintext .highlighter-rouge} and
`'cm'`{.language-plaintext .highlighter-rouge} cannot. The comparison
will be made using the most granular unit of either input. Attempting to
operate on quantities with invalid units will result in empty
(`{ }`{.language-plaintext .highlighter-rouge}).

For time-valued quantities, note that calendar durations and definite
quantity durations above days (and weeks) are considered un-comparable:

``` fhirpath
1 year = 1 'a' // {} an empty collection
1 second = 1 's' // true
```

Implementations are not required to fully support operations on units,
but they must at least respect units, recognizing when units differ.

Implementations that do support units shall do so as specified by
[\[UCUM\]](#UCUM), as well as the calendar durations as defined in the
toQuantity function.

##### Date/Time Equality

For `Date`{.language-plaintext .highlighter-rouge},
`DateTime`{.language-plaintext .highlighter-rouge} and
`Time`{.language-plaintext .highlighter-rouge} equality, the comparison
is performed by considering each precision in order, beginning with
years (or hours for time values), and respecting timezone offsets. If
the values are the same, comparison proceeds to the next precision; if
the values are different, the comparison stops and the result is
`false`{.language-plaintext .highlighter-rouge}. If one input has a
value for the precision and the other does not, the comparison stops and
the result is empty (`{ }`{.language-plaintext .highlighter-rouge}); if
neither input has a value for the precision, or the last precision has
been reached, the comparison stops and the result is
`true`{.language-plaintext .highlighter-rouge}. For the purposes of
comparison, seconds and milliseconds are considered a single precision
using a decimal, with decimal equality semantics.

For example:

``` fhirpath
@2012 = @2012 // returns true
@2012 = @2013 // returns false
@2012-01 = @2012 // returns empty ({ })
@2012-01-01T10:30 = @2012-01-01T10:30 // returns true
@2012-01-01T10:30 = @2012-01-01T10:31 // returns false
@2012-01-01T10:30:31 = @2012-01-01T10:30 // returns empty ({ })
@2012-01-01T10:30:31.0 = @2012-01-01T10:30:31 // returns true
@2012-01-01T10:30:31.1 = @2012-01-01T10:30:31 // returns false
```

For `DateTime`{.language-plaintext .highlighter-rouge} values that do
not have a timezone offsets, whether or not to provide a default
timezone offset is a policy decision. In the simplest case, no default
timezone offset is provided, but some implementations may use the
client\'s or the evaluating system\'s timezone offset.

To support comparison of DateTime values, either both values have no
timezone offset specified, or both values are converted to a common
timezone offset. The timezone offset to use is an implementation
decision. In the simplest case, it\'s the timezone offset of the local
server. The following examples illustrate expected behavior:

``` fhirpath
@2017-11-05T01:30:00.0-04:00 > @2017-11-05T01:15:00.0-05:00 // false
@2017-11-05T01:30:00.0-04:00 < @2017-11-05T01:15:00.0-05:00 // true
@2017-11-05T01:30:00.0-04:00 = @2017-11-05T01:15:00.0-05:00 // false
@2017-11-05T01:30:00.0-04:00 = @2017-11-05T00:30:00.0-05:00 // true
```

Additional functions to support more sophisticated timezone offset
comparison (such as .toUTC()) may be defined in a future version.

#### \~ (Equivalent) {#-equivalent}

Returns `true`{.language-plaintext .highlighter-rouge} if the
collections are the same. In particular, comparing empty collections for
equivalence `{ } ~ { }`{.language-plaintext .fhirpath
.highlighter-rouge} will result in `true`{.language-plaintext
.highlighter-rouge}.

If both operands are collections with a single item, they must be of the
same type (or implicitly convertible to the same type), and:

-   For primitives
    -   `String`{.language-plaintext .highlighter-rouge}: the strings
        must be the same, ignoring case and locale, and normalizing
        whitespace (see [String Equivalence](#string-equivalence) for
        more details).
    -   `Integer`{.language-plaintext .highlighter-rouge}: exactly equal
    -   `Decimal`{.language-plaintext .highlighter-rouge}: values must
        be equal, comparison is done on values rounded to the precision
        of the least precise operand. Trailing zeroes after the decimal
        are ignored in determining precision.
    -   `Date`{.language-plaintext .highlighter-rouge},
        `DateTime`{.language-plaintext .highlighter-rouge} and
        `Time`{.language-plaintext .highlighter-rouge}: values must be
        equal, except that if the input values have different levels of
        precision, the comparison returns `false`{.language-plaintext
        .highlighter-rouge}, not empty (`{ }`{.language-plaintext
        .highlighter-rouge}).
    -   `Boolean`{.language-plaintext .highlighter-rouge}: the values
        must be the same
-   For complex types, equivalence requires all child properties to be
    equivalent, recursively.

If both operands are collections with multiple items:

-   Each item must be equivalent
-   Comparison is not order dependent

Note that this implies that if the collections have a different number
of items to compare, or if one input is a value and the other is empty
(`{ }`{.language-plaintext .highlighter-rouge}), the result will be
`false`{.language-plaintext .highlighter-rouge}.

##### Quantity Equivalence

When comparing quantities for equivalence, the dimensions of each
quantity must be the same, but not necessarily the unit. For example,
units of `'cm'`{.language-plaintext .highlighter-rouge} and
`'m'`{.language-plaintext .highlighter-rouge} can be compared, but units
of `'cm2'`{.language-plaintext .highlighter-rouge} and
`'cm'`{.language-plaintext .highlighter-rouge} cannot. The comparison
will be made using the most granular unit of either input. Attempting to
operate on quantities with invalid units will result in
`false`{.language-plaintext .highlighter-rouge}.

For time-valued quantities, calendar durations and definite quantity
durations are considered equivalent:

``` fhirpath
1 year ~ 1 'a' // true
1 second ~ 1 's' // true
```

Implementations are not required to fully support operations on units,
but they must at least respect units, recognizing when units differ.

Implementations that do support units shall do so as specified by
[\[UCUM\]](#UCUM) as well as the calendar durations as defined in the
toQuantity function.

##### Date/Time Equivalence

For `Date`{.language-plaintext .highlighter-rouge},
`DateTime`{.language-plaintext .highlighter-rouge} and
`Time`{.language-plaintext .highlighter-rouge} equivalence, the
comparison is the same as for equality, with the exception that if the
input values have different levels of precision, the result is
`false`{.language-plaintext .highlighter-rouge}, rather than empty
(`{ }`{.language-plaintext .highlighter-rouge}). As with equality, the
second and millisecond precisions are considered a single precision
using a decimal, with decimal equivalence semantics.

For example:

``` fhirpath
@2012 ~ @2012 // returns true
@2012 ~ @2013 // returns false
@2012-01 ~ @2012 // returns false as well
@2012-01-01T10:30 ~ @2012-01-01T10:30 // returns true
@2012-01-01T10:30 ~ @2012-01-01T10:31 // returns false
@2012-01-01T10:30:31 ~ @2012-01-01T10:30 // returns false as well
@2012-01-01T10:30:31.0 ~ @2012-01-01T10:30:31 // returns true
@2012-01-01T10:30:31.1 ~ @2012-01-01T10:30:31 // returns false
```

##### String Equivalence

For strings, equivalence returns true if the strings are the same value
while ignoring case and locale, and normalizing whitespace. Normalizing
whitespace means that all whitespace characters are treated as
equivalent, with whitespace characters as defined in the
[Whitespace](#whitespace) lexical category.

#### != (Not Equals) {#-not-equals}

The converse of the equals operator, returning
`true`{.language-plaintext .highlighter-rouge} if equal returns
`false`{.language-plaintext .highlighter-rouge};
`false`{.language-plaintext .highlighter-rouge} if equal returns
`true`{.language-plaintext .highlighter-rouge}; and empty
(`{ }`{.language-plaintext .highlighter-rouge}) if equal returns empty.
In other words, `A != B`{.language-plaintext .fhirpath
.highlighter-rouge} is short-hand for
`(A = B).not()`{.language-plaintext .fhirpath .highlighter-rouge}.

#### !\~ (Not Equivalent) {#-not-equivalent}

The converse of the equivalent operator, returning
`true`{.language-plaintext .highlighter-rouge} if equivalent returns
`false`{.language-plaintext .highlighter-rouge} and
`false`{.language-plaintext .highlighter-rouge} is equivalent returns
`true`{.language-plaintext .highlighter-rouge}. In other words,
`A !~ B`{.language-plaintext .fhirpath .highlighter-rouge} is short-hand
for `(A ~ B).not()`{.language-plaintext .fhirpath .highlighter-rouge}.

### Comparison

-   The comparison operators are defined for strings, integers,
    decimals, quantities, dates, datetimes and times.
-   If one or both of the arguments is an empty collection, a comparison
    operator will return an empty collection.
-   Both arguments must be collections with single values, and the
    evaluator will throw an error if either collection has more than one
    item.
-   Both arguments must be of the same type (or implicitly convertible
    to the same type), and the evaluator will throw an error if the
    types differ.
-   When comparing integers and decimals, the integer will be converted
    to a decimal to make comparison possible.
-   String ordering is strictly lexical and is based on the Unicode
    value of the individual characters.

When comparing quantities, the dimensions of each quantity must be the
same, but not necessarily the unit. For example, units of
`'cm'`{.language-plaintext .highlighter-rouge} and
`'m'`{.language-plaintext .highlighter-rouge} can be compared, but units
of `'cm2'`{.language-plaintext .highlighter-rouge} and
`'cm'`{.language-plaintext .highlighter-rouge} cannot. The comparison
will be made using the most granular unit of either input. Attempting to
operate on quantities with invalid units will result in empty
(`{ }`{.language-plaintext .highlighter-rouge}).

For time-valued quantities, note that calendar durations and definite
quantity durations above days (and weeks) are considered un-comparable:

``` fhirpath
1 year > 1 `a` // { } (empty)
10 seconds > 1 's' // true
```

Implementations are not required to fully support operations on units,
but they must at least respect units, recognizing when units differ.

Implementations that do support units shall do so as specified by
[\[UCUM\]](#UCUM) as well as the calendar durations as defined in the
toQuantity function.

For partial Date, DateTime, and Time values, the comparison is performed
by comparing the values at each precision, beginning with years, and
proceeding to the finest precision specified in either input, and
respecting timezone offsets. If one value is specified to a different
level of precision than the other, the result is empty
(`{ }`{.language-plaintext .highlighter-rouge}) to indicate that the
result of the comparison is unknown. As with equality and equivalence,
the second and millisecond precisions are considered a single precision
using a decimal, with decimal comparison semantics.

See the [Equals](#equals) operator for discussion on respecting timezone
offsets in comparison operations.

#### \> (Greater Than) {#-greater-than}

The greater than operator (`>`{.language-plaintext .highlighter-rouge})
returns true if the first operand is strictly greater than the second.
The operands must be of the same type, or convertible to the same type
using an implicit conversion.

``` fhirpath
10 > 5 // true
10 > 5.0 // true; note the 10 is converted to a decimal to perform the comparison
'abc' > 'ABC' // true
4 'm' > 4 'cm' // true (or { } if the implementation does not support unit conversion)
@2018-03-01 > @2018-01-01 // true
@2018-03 > @2018-03-01 // empty ({ })
@2018-03-01T10:30:00 > @2018-03-01T10:00:00 // true
@2018-03-01T10 > @2018-03-01T10:30 // empty ({ })
@2018-03-01T10:30:00 > @2018-03-01T10:30:00.0 // false
@T10:30:00 > @T10:00:00 // true
@T10 > @T10:30 // empty ({ })
@T10:30:00 > @T10:30:00.0 // false
```

#### \< (Less Than) {#-less-than}

The less than operator (`<`{.language-plaintext .highlighter-rouge})
returns true if the first operand is strictly less than the second. The
operands must be of the same type, or convertible to the same type using
implicit conversion.

``` fhirpath
10 < 5 // false
10 < 5.0 // false; note the 10 is converted to a decimal to perform the comparison
'abc' < 'ABC' // false
4 'm' < 4 'cm' // false (or { } if the implementation does not support unit conversion)
@2018-03-01 < @2018-01-01 // false
@2018-03 < @2018-03-01 // empty ({ })
@2018-03-01T10:30:00 < @2018-03-01T10:00:00 // false
@2018-03-01T10 < @2018-03-01T10:30 // empty ({ })
@2018-03-01T10:30:00 < @2018-03-01T10:30:00.0 // false
@T10:30:00 < @T10:00:00 // false
@T10 < @T10:30 // empty ({ })
@T10:30:00 < @T10:30:00.0 // false
```

#### \<= (Less or Equal) {#-less-or-equal}

The less or equal operator (`\<=`{.language-plaintext
.highlighter-rouge}) returns true if the first operand is less than or
equal to the second. The operands must be of the same type, or
convertible to the same type using implicit conversion.

``` fhirpath
10 <= 5 // true
10 <= 5.0 // true; note the 10 is converted to a decimal to perform the comparison
'abc' <= 'ABC' // true
4 'm' <= 4 'cm' // false (or { } if the implementation does not support unit conversion)
@2018-03-01 <= @2018-01-01 // false
@2018-03 <= @2018-03-01 // empty ({ })
@2018-03-01T10:30:00 <= @2018-03-01T10:00:00 // false
@2018-03-01T10 <= @2018-03-01T10:30 // empty ({ })
@2018-03-01T10:30:00 <= @2018-03-01T10:30:00.0 // true
@T10:30:00 <= @T10:00:00 // false
@T10 <= @T10:30 // empty ({ })
@T10:30:00 <= @T10:30:00.0 // true
```

#### \>= (Greater or Equal) {#-greater-or-equal}

The greater or equal operator (`>=`{.language-plaintext
.highlighter-rouge}) returns true if the first operand is greater than
or equal to the second. The operands must be of the same type, or
convertible to the same type using implicit conversion.

``` fhirpath
10 >= 5 // false
10 >= 5.0 // false; note the 10 is converted to a decimal to perform the comparison
'abc' >= 'ABC' // false
4 'm' >= 4 'cm' // true (or { } if the implementation does not support unit conversion)
@2018-03-01 >= @2018-01-01 // true
@2018-03 >= @2018-03-01 // empty ({ })
@2018-03-01T10:30:00 >= @2018-03-01T10:00:00 // true
@2018-03-01T10 >= @2018-03-01T10:30 // empty ({ })
@2018-03-01T10:30:00 >= @2018-03-01T10:30:00.0 // true
@T10:30:00 >= @T10:00:00 // true
@T10 >= @T10:30 // empty ({ })
@T10:30:00 >= @T10:30:00.0 // true
```

### Types

#### is *type specifier*

If the left operand is a collection with a single item and the second
operand is a type identifier, this operator returns
`true`{.language-plaintext .highlighter-rouge} if the type of the left
operand is the type specified in the second operand, or a subclass
thereof. If the input value is not of the type, this operator returns
`false`{.language-plaintext .highlighter-rouge}. If the identifier
cannot be resolved to a valid type identifier, the evaluator will throw
an error. If the input collections contains more than one item, the
evaluator will throw an error. In all other cases this operator returns
`false`{.language-plaintext .highlighter-rouge}.

A *type specifier* is an identifier that must resolve to the name of a
type in a model. Type specifiers can have qualifiers, e.g.
`FHIR.Patient`{.language-plaintext .highlighter-rouge}, where the
qualifier is the name of the model.

``` fhirpath
Bundle.entry.resource.all($this is Observation implies status = 'finished')
```

This example returns `true`{.language-plaintext .highlighter-rouge} if
all Observation resources in the bundle have a status of finished.

#### is(type : *type specifier*) {#istype--type-specifier}

The `is()`{.language-plaintext .highlighter-rouge} function is supported
for backwards compatibility with previous implementations of FHIRPath.
Just as with the `is`{.language-plaintext .highlighter-rouge} keyword,
the `type`{.language-plaintext .highlighter-rouge} argument is an
identifier that must resolve to the name of a type in a model. For
implementations with compile-time typing, this requires special-case
handling when processing the argument to treat it as a type specifier
rather than an identifier expression:

``` fhirpath
Bundle.entry.resource.all($this.is(Observation) implies status = 'finished')
```

> **Note:** The `is()`{.language-plaintext .highlighter-rouge} function
> is defined for backwards compatibility only and may be deprecated in a
> future release.

#### as *type specifier*

If the left operand is a collection with a single item and the second
operand is an identifier, this operator returns the value of the left
operand if it is of the type specified in the second operand, or a
subclass thereof. If the identifier cannot be resolved to a valid type
identifier, the evaluator will throw an error. If there is more than one
item in the input collection, the evaluator will throw an error.
Otherwise, this operator returns the empty collection.

A *type specifier* is an identifier that must resolve to the name of a
type in a model. Type specifiers can have qualifiers, e.g.
`FHIR.Patient`{.language-plaintext .highlighter-rouge}, where the
qualifier is the name of the model.

``` fhirpath
Observation.component.where(value as Quantity > 30 'mg')
```

#### as(type : *type specifier*) {#astype--type-specifier}

The `as()`{.language-plaintext .highlighter-rouge} function is supported
for backwards compatibility with previous implementations of FHIRPath.
Just as with the `as`{.language-plaintext .highlighter-rouge} keyword,
the `type`{.language-plaintext .highlighter-rouge} argument is an
identifier that must resolve to the name of a type in a model. For
implementations with compile-time typing, this requires special-case
handling when processing the argument to treat is a type specifier
rather than an identifier expression:

``` fhirpath
Observation.component.where(value.as(Quantity) > 30 'mg')
```

> **Note:** The `as()`{.language-plaintext .highlighter-rouge} function
> is defined for backwards compatibility only and may be deprecated in a
> future release.

### Collections

#### \| (union collections) {#-union-collections}

Merge the two collections into a single collection, eliminating any
duplicate values (using [equals](#equals) (`=`{.language-plaintext
.highlighter-rouge})) to determine equality). There is no expectation of
order in the resulting collection.

See the [union](#unionother-collection) function for more detail.

#### in (membership)

If the left operand is a collection with a single item, this operator
returns true if the item is in the right operand using equality
semantics. If the left-hand side of the operator is empty, the result is
empty, if the right-hand side is empty, the result is false. If the left
operand has multiple items, an exception is thrown.

The following example returns true if `'Joe'`{.language-plaintext
.highlighter-rouge} is in the list of given names for the Patient:

``` fhirpath
'Joe' in Patient.name.given
```

#### contains (containership)

If the right operand is a collection with a single item, this operator
returns true if the item is in the left operand using equality
semantics. If the right-hand side of the operator is empty, the result
is empty, if the left-hand side is empty, the result is false. This is
the converse operation of in.

The following example returns true if the list of given names for the
Patient has `'Joe'`{.language-plaintext .highlighter-rouge} in it:

``` fhirpath
Patient.name.given contains 'Joe'
```

### Boolean logic

For all boolean operators, the collections passed as operands are first
evaluated as Booleans (as described in [Singleton Evaluation of
Collections](#singleton-evaluation-of-collections)). The operators then
use three-valued logic to propagate empty operands.

> **Note:** To ensure that FHIRPath expressions can be freely rewritten
> by underlying implementations, there is no expectation that an
> implementation respect short-circuit evaluation. With regard to
> performance, implementations may use short-circuit evaluation to
> reduce computation, but authors should not rely on such behavior, and
> implementations must not change semantics with short-circuit
> evaluation. If short-circuit evaluation is needed to avoid effects
> (e.g. runtime exceptions), use the [`iif()`{.language-plaintext
> .highlighter-rouge}](#iif) function.

#### and

Returns `true`{.language-plaintext .highlighter-rouge} if both operands
evaluate to `true`{.language-plaintext .highlighter-rouge},
`false`{.language-plaintext .highlighter-rouge} if either operand
evaluates to `false`{.language-plaintext .highlighter-rouge}, and the
empty collection (`{ }`{.language-plaintext .highlighter-rouge})
otherwise.

  and         true                                                    false                                             empty
  ----------- ------------------------------------------------------- ------------------------------------------------- -------------------------------------------------------
  **true**    `true`{.language-plaintext .highlighter-rouge}          `false`{.language-plaintext .highlighter-rouge}   empty (`{ }`{.language-plaintext .highlighter-rouge})
  **false**   `false`{.language-plaintext .highlighter-rouge}         `false`{.language-plaintext .highlighter-rouge}   `false`{.language-plaintext .highlighter-rouge}
  **empty**   empty (`{ }`{.language-plaintext .highlighter-rouge})   `false`{.language-plaintext .highlighter-rouge}   empty (`{ }`{.language-plaintext .highlighter-rouge})

#### or

Returns `false`{.language-plaintext .highlighter-rouge} if both operands
evaluate to `false`{.language-plaintext .highlighter-rouge},
`true`{.language-plaintext .highlighter-rouge} if either operand
evaluates to `true`{.language-plaintext .highlighter-rouge}, and empty
(`{ }`{.language-plaintext .highlighter-rouge}) otherwise:

  or          true                                             false                                                   empty
  ----------- ------------------------------------------------ ------------------------------------------------------- -------------------------------------------------------
  **true**    `true`{.language-plaintext .highlighter-rouge}   `true`{.language-plaintext .highlighter-rouge}          `true`{.language-plaintext .highlighter-rouge}
  **false**   `true`{.language-plaintext .highlighter-rouge}   `false`{.language-plaintext .highlighter-rouge}         empty (`{ }`{.language-plaintext .highlighter-rouge})
  **empty**   `true`{.language-plaintext .highlighter-rouge}   empty (`{ }`{.language-plaintext .highlighter-rouge})   empty (`{ }`{.language-plaintext .highlighter-rouge})

#### not() : Boolean {#not--boolean}

Returns `true`{.language-plaintext .highlighter-rouge} if the input
collection evaluates to `false`{.language-plaintext .highlighter-rouge},
and `false`{.language-plaintext .highlighter-rouge} if it evaluates to
`true`{.language-plaintext .highlighter-rouge}. Otherwise, the result is
empty (`{ }`{.language-plaintext .highlighter-rouge}):

  not          
  ----------- -------------------------------------------------------
  **true**    `false`{.language-plaintext .highlighter-rouge}
  **false**   `true`{.language-plaintext .highlighter-rouge}
  **empty**   empty (`{ }`{.language-plaintext .highlighter-rouge})

#### xor

Returns `true`{.language-plaintext .highlighter-rouge} if exactly one of
the operands evaluates to `true`{.language-plaintext
.highlighter-rouge}, `false`{.language-plaintext .highlighter-rouge} if
either both operands evaluate to `true`{.language-plaintext
.highlighter-rouge} or both operands evaluate to
`false`{.language-plaintext .highlighter-rouge}, and the empty
collection (`{ }`{.language-plaintext .highlighter-rouge}) otherwise:

  xor         true                                                    false                                                   empty
  ----------- ------------------------------------------------------- ------------------------------------------------------- -------------------------------------------------------
  **true**    `false`{.language-plaintext .highlighter-rouge}         `true`{.language-plaintext .highlighter-rouge}          empty (`{ }`{.language-plaintext .highlighter-rouge})
  **false**   `true`{.language-plaintext .highlighter-rouge}          `false`{.language-plaintext .highlighter-rouge}         empty (`{ }`{.language-plaintext .highlighter-rouge})
  **empty**   empty (`{ }`{.language-plaintext .highlighter-rouge})   empty (`{ }`{.language-plaintext .highlighter-rouge})   empty (`{ }`{.language-plaintext .highlighter-rouge})

#### implies

If the left operand evaluates to `true`{.language-plaintext
.highlighter-rouge}, this operator returns the boolean evaluation of the
right operand. If the left operand evaluates to
`false`{.language-plaintext .highlighter-rouge}, this operator returns
`true`{.language-plaintext .highlighter-rouge}. Otherwise, this operator
returns `true`{.language-plaintext .highlighter-rouge} if the right
operand evaluates to `true`{.language-plaintext .highlighter-rouge}, and
the empty collection (`{ }`{.language-plaintext .highlighter-rouge})
otherwise.

  implies     true                                             false                                                   empty
  ----------- ------------------------------------------------ ------------------------------------------------------- -------------------------------------------------------
  **true**    `true`{.language-plaintext .highlighter-rouge}   `false`{.language-plaintext .highlighter-rouge}         empty (`{ }`{.language-plaintext .highlighter-rouge})
  **false**   `true`{.language-plaintext .highlighter-rouge}   `true`{.language-plaintext .highlighter-rouge}          `true`{.language-plaintext .highlighter-rouge}
  **empty**   `true`{.language-plaintext .highlighter-rouge}   empty (`{ }`{.language-plaintext .highlighter-rouge})   empty (`{ }`{.language-plaintext .highlighter-rouge})

The implies operator is useful for testing conditionals. For example, if
a given name is present, then a family name must be as well:

``` fhirpath
Patient.name.given.exists() implies Patient.name.family.exists()
CareTeam.onBehalfOf.exists() implies (CareTeam.member.resolve() is Practitioner)
StructureDefinition.contextInvariant.exists() implies StructureDefinition.type = 'Extension'
```

Note that implies may use short-circuit evaluation in the case that the
first operand evaluates to false.

### Math

The math operators require each operand to be a single element. Both
operands must be of the same type, or of compatible types according to
the rules for implicit conversion. Each operator below specifies which
types are supported.

If there is more than one item, or an incompatible item, the evaluation
of the expression will end and signal an error to the calling
environment.

As with the other operators, the math operators will return an empty
collection if one or both of the operands are empty.

When operating on quantities, the dimensions of each quantity must be
the same, but not necessarily the unit. For example, units of
`'cm'`{.language-plaintext .highlighter-rouge} and
`'m'`{.language-plaintext .highlighter-rouge} can be compared, but units
of `'cm2'`{.language-plaintext .highlighter-rouge} and
`'cm'`{.language-plaintext .highlighter-rouge} cannot. The unit of the
result will be the most granular unit of either input. Attempting to
operate on quantities with invalid units will result in empty
(`{ }`{.language-plaintext .highlighter-rouge}).

Implementations are not required to fully support operations on units,
but they must at least respect units, recognizing when units differ.

Implementations that do support units shall do so as specified by
[\[UCUM\]](#UCUM) as well as the calendar durations as defined in the
toQuantity function.

Operations that cause arithmetic overflow or underflow will result in
empty (`{ }`{.language-plaintext .highlighter-rouge}).

#### \* (multiplication) {#-multiplication}

Multiplies both arguments (supported for Integer, Decimal, and
Quantity). For multiplication involving quantities, the resulting
quantity will have the appropriate unit:

``` fhirpath
12 'cm' * 3 'cm' // 36 'cm2'
3 'cm' * 12 'cm2' // 36 'cm3'
```

#### / (division) {#-division}

Divides the left operand by the right operand (supported for Integer,
Decimal, and Quantity). The result of a division is always Decimal, even
if the inputs are both Integer. For integer division, use the
`div`{.language-plaintext .highlighter-rouge} operator.

If an attempt is made to divide by zero, the result is empty.

For division involving quantities, the resulting quantity will have the
appropriate unit:

``` fhirpath
12 'cm2' / 3 'cm' // 4.0 'cm'
12 / 0 // empty ({ })
```

#### + (addition) {#-addition}

For Integer, Decimal, and quantity, adds the operands. For strings,
concatenates the right operand to the left operand.

When adding quantities, the dimensions of each quantity must be the
same, but not necessarily the unit.

``` fhirpath
3 'm' + 3 'cm' // 303 'cm'
```

#### - (subtraction) {#--subtraction}

Subtracts the right operand from the left operand (supported for
Integer, Decimal, and Quantity).

When subtracting quantities, the dimensions of each quantity must be the
same, but not necessarily the unit.

``` fhirpath
3 'm' - 3 'cm' // 297 'cm'
```

#### div

Performs truncated division of the left operand by the right operand
(supported for Integer and Decimal). In other words, the division that
ignores any remainder:

``` fhirpath
5 div 2 // 2
5.5 div 0.7 // 7
5 div 0 // empty ({ })
```

#### mod

Computes the remainder of the truncated division of its arguments
(supported for Integer and Decimal).

``` fhirpath
5 mod 2 // 1
5.5 mod 0.7 // 0.6
5 mod 0 // empty ({ })
```

#### & (String concatenation) {#-string-concatenation}

For strings, will concatenate the strings, where an empty operand is
taken to be the empty string. This differs from `+`{.language-plaintext
.highlighter-rouge} on two strings, which will result in an empty
collection when one of the operands is empty. This operator is
specifically included to simplify treating an empty collection as an
empty string, a common use case in string manipulation.

``` fhirpath
'ABC' + 'DEF' // 'ABCDEF'
'ABC' + { } + 'DEF' // { }
'ABC' & 'DEF' // 'ABCDEF'
'ABC' & { } & 'DEF' // 'ABCDEF'
```

### Date/Time Arithmetic

Date and time arithmetic operators are used to add time-valued
quantities to date/time values. The left operand must be a
`Date`{.language-plaintext .highlighter-rouge},
`DateTime`{.language-plaintext .highlighter-rouge}, or
`Time`{.language-plaintext .highlighter-rouge} value, and the right
operand must be a `Quantity`{.language-plaintext .highlighter-rouge}
with a time-valued unit:

-   `year`{.language-plaintext .highlighter-rouge},
    `years`{.language-plaintext .highlighter-rouge}
-   `month`{.language-plaintext .highlighter-rouge},
    `months`{.language-plaintext .highlighter-rouge}
-   `week`{.language-plaintext .highlighter-rouge},
    `weeks`{.language-plaintext .highlighter-rouge}
-   `day`{.language-plaintext .highlighter-rouge},
    `days`{.language-plaintext .highlighter-rouge}
-   `hour`{.language-plaintext .highlighter-rouge},
    `hours`{.language-plaintext .highlighter-rouge}
-   `minute`{.language-plaintext .highlighter-rouge},
    `minutes`{.language-plaintext .highlighter-rouge}
-   `second`{.language-plaintext .highlighter-rouge},
    `seconds`{.language-plaintext .highlighter-rouge}, or
    `'s'`{.language-plaintext .highlighter-rouge}
-   `millisecond`{.language-plaintext .highlighter-rouge},
    `milliseconds`{.language-plaintext .highlighter-rouge}, or
    `'ms'`{.language-plaintext .highlighter-rouge}

To avoid the potential confusion of calendar-based date/time arithmetic
with definite duration date/time arithmetic, FHIRPath defines
definite-duration date/time arithmetic for seconds and below, and
calendar-based date/time arithmetic for seconds and above. At the
second, calendar-based and definite-duration-based date/time arithmetic
are identical. If a definite-quantity duration above seconds appears in
a date/time arithmetic calculation, the evaluation will end and signal
an error to the calling environment.

Within FHIRPath, calculations involving date/times and calendar
durations shall use calendar semantics as specified in
[\[ISO8601\]](#ISO8601). Specifically:

  ------------- -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  year          The year, positive or negative, is added to the year component of the date or time value. If the resulting year is out of range, an error is thrown. If the month and day of the date or time value is not a valid date in the resulting year, the last day of the calendar month is used.
  month         The month, positive or negative is divided by 12, and the integer portion of the result is added to the year component. The remaining portion of months is added to the month component. If the resulting date is not a valid date in the resulting year, the last day of the resulting calendar month is used.
  week          The week, positive or negative, is multiplied by 7, and the resulting value is added to the day component, respecting calendar month and calendar year lengths.
  day           The day, positive or negative, is added to the day component, respecting calendar month and calendar year lengths.
  hour          The hours, positive or negative, are added to the hour component, with each 24 hour block counting as a calendar day, and respecting calendar month and calendar year lengths.
  minute        The minutes, positive or negative, are added to the minute component, with each 60 minute block counting as an hour, and respecting calendar month and calendar year lengths.
  second        The seconds, positive or negative, are added to the second component, with each 60 second block counting as a minute, and respecting calendar month and calendar year lengths.
  millisecond   The milliseconds, positive or negative, are added to the millisecond component, with each 1000 millisecond block counting as a second, and respecting calendar month and calendar year lengths.
  ------------- -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

If there is more than one item, or an item of an incompatible type, the
evaluation of the expression will end and signal an error to the calling
environment.

If either or both arguments are empty (`{ }`{.language-plaintext
.highlighter-rouge}), the result is empty (`{ }`{.language-plaintext
.highlighter-rouge}).

#### + (addition) {#-addition-1}

Returns the value of the given `Date`{.language-plaintext
.highlighter-rouge}, `DateTime`{.language-plaintext .highlighter-rouge},
or `Time`{.language-plaintext .highlighter-rouge}, incremented by the
time-valued quantity, respecting variable length periods for calendar
years and months.

For `Date`{.language-plaintext .highlighter-rouge} values, the quantity
unit must be one of: `years`{.language-plaintext .highlighter-rouge},
`months`{.language-plaintext .highlighter-rouge},
`weeks`{.language-plaintext .highlighter-rouge}, or
`days`{.language-plaintext .highlighter-rouge}

For `DateTime`{.language-plaintext .highlighter-rouge} values, the
quantity unit must be one of: `years`{.language-plaintext
.highlighter-rouge}, `months`{.language-plaintext .highlighter-rouge},
`weeks`{.language-plaintext .highlighter-rouge},
`days`{.language-plaintext .highlighter-rouge},
`hours`{.language-plaintext .highlighter-rouge},
`minutes`{.language-plaintext .highlighter-rouge},
`seconds`{.language-plaintext .highlighter-rouge}, or
`milliseconds`{.language-plaintext .highlighter-rouge} (or an equivalent
unit), or the evaluation will end and signal an error to the calling
environment.

For `Time`{.language-plaintext .highlighter-rouge} values, the quantity
unit must be one of: `hours`{.language-plaintext .highlighter-rouge},
`minutes`{.language-plaintext .highlighter-rouge},
`seconds`{.language-plaintext .highlighter-rouge}, or
`milliseconds`{.language-plaintext .highlighter-rouge} (or an equivalent
unit), or the evaluation will end and signal an error to the calling
environment.

For precisions above `seconds`{.language-plaintext .highlighter-rouge},
the decimal portion of the time-valued quantity is ignored, since
date/time arithmetic above seconds is performed with calendar duration
semantics.

For partial date/time values where the time-valued quantity is more
precise than the partial date/time, the operation is performed by
converting the time-valued quantity to the highest precision in the
partial (removing any decimal value off) and then adding to the
date/time value. For example:

``` fhirpath
@2014 + 24 months
@2019-03-01 + 24 months // @2021-03-01
```

The first example above will evaluate to the value
`@2016`{.language-plaintext .highlighter-rouge} even though the
date/time value is not specified to the level of precision of the
time-valued quantity. The second example will evaluate to
`@2021-03-01`{.language-plaintext .highlighter-rouge}.

``` fhirpath
@2014 + 23 months
@2016 + 365 days
```

The first example above returns \@2015, because 23 months only
constitutes one year. The second example returns 2017 because even
though 2016 is a leap-year, the time-valued quantity
(`365 days`{.language-plaintext .highlighter-rouge}) is converted to
`1 year`{.language-plaintext .highlighter-rouge}, a standard calendar
year of 365 days.

Calculations involving weeks are equivalent to multiplying the number of
weeks by 7 and performing the calculation for the resulting number of
days.

#### - (subtraction) {#--subtraction-1}

Returns the value of the given `Date`{.language-plaintext
.highlighter-rouge}, `DateTime`{.language-plaintext .highlighter-rouge},
or `Time`{.language-plaintext .highlighter-rouge}, decremented by the
time-valued quantity, respecting variable length periods for calendar
years and months.

For `Date`{.language-plaintext .highlighter-rouge} values, the quantity
unit must be one of: `years`{.language-plaintext .highlighter-rouge},
`months`{.language-plaintext .highlighter-rouge},
`weeks`{.language-plaintext .highlighter-rouge}, or
`days`{.language-plaintext .highlighter-rouge}

For `DateTime`{.language-plaintext .highlighter-rouge} values, the
quantity unit must be one of: `years`{.language-plaintext
.highlighter-rouge}, `months`{.language-plaintext .highlighter-rouge},
`weeks`{.language-plaintext .highlighter-rouge},
`days`{.language-plaintext .highlighter-rouge},
`hours`{.language-plaintext .highlighter-rouge},
`minutes`{.language-plaintext .highlighter-rouge},
`seconds`{.language-plaintext .highlighter-rouge}, or
`milliseconds`{.language-plaintext .highlighter-rouge} (or an equivalent
unit), or the evaluation will end and signal an error to the calling
environment.

For `Time`{.language-plaintext .highlighter-rouge} values, the quantity
unit must be one of: `hours`{.language-plaintext .highlighter-rouge},
`minutes`{.language-plaintext .highlighter-rouge},
`seconds`{.language-plaintext .highlighter-rouge}, or
`milliseconds`{.language-plaintext .highlighter-rouge} (or an equivalent
unit), or the evaluation will end and signal an error to the calling
environment.

For precisions above `seconds`{.language-plaintext .highlighter-rouge},
the decimal portion of the time-valued quantity is ignored, since
date/time arithmetic above seconds is performed with calendar duration
semantics.

For partial date/time values where the time-valued quantity is more
precise than the partial date/time, the operation is performed by
converting the time-valued quantity to the highest precision in the
partial (removing any decimal value off) and then subtracting from the
date/time value. For example:

``` fhirpath
@2014 - 24 months
@2019-03-01 - 24 months // @2017-03-01
```

The first example above will evaluate to the value
`@2012`{.language-plaintext .highlighter-rouge} even though the
date/time value is not specified to the level of precision of the
time-valued quantity. The second example will evaluate to
`@2017-03-01`{.language-plaintext .highlighter-rouge}.

Calculations involving weeks are equivalent to multiplying the number of
weeks by 7 and performing the calculation for the resulting number of
days.

### Operator precedence

Precedence of operations, in order from high to low:

``` txt
#01 . (path/function invocation)
#02 [] (indexer)
#03 unary + and -
#04: *, /, div, mod
#05: +, -, &
#06: is, as
#07: |
#08: >, <, >=, <=
#09: =, ~, !=, !~
#10: in, contains
#11: and
#12: xor, or
#13: implies
```

As customary, precedence may be established explicitly using parentheses
(`( )`{.language-plaintext .highlighter-rouge}).

As an example, consider the following expression:

``` fhirpath
-7.combine(3)
```

Because the invocation operator (`.`{.language-plaintext
.highlighter-rouge}) has a higher precedence than the unary negation
(`-`{.language-plaintext .highlighter-rouge}), the unary negation will
be applied to the result of the combine of 7 and 3, resulting in an
error (because unary negation cannot be applied to a list):

``` fhirpath
-(7.combine(3)) // ERROR
```

Use parentheses to ensure the unary negation applies to the
`7`{.language-plaintext .highlighter-rouge}:

``` fhirpath
(-7).combine(3) // { -7, 3 }
```

## Aggregates

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

FHIRPath supports a general-purpose aggregate function to enable the
calculation of aggregates such as sum, min, and max to be expressed:

### aggregate(aggregator : expression \[, init : value\]) : value {#aggregateaggregator--expression--init--value--value .stu}

Performs general-purpose aggregation by evaluating the aggregator
expression for each element of the input collection. Within this
expression, the standard iteration variables of
`$this`{.language-plaintext .highlighter-rouge} and
`$index`{.language-plaintext .highlighter-rouge} can be accessed, but
also a `$total`{.language-plaintext .highlighter-rouge} aggregation
variable.

The value of the `$total`{.language-plaintext .highlighter-rouge}
variable is set to `init`{.language-plaintext .highlighter-rouge}, or
empty (`{ }`{.language-plaintext .highlighter-rouge}) if no
`init`{.language-plaintext .highlighter-rouge} value is supplied, and is
set to the result of the aggregator expression after every iteration.\
The result of the aggregate function is the value of
`$total`{.language-plaintext .highlighter-rouge} after the last
iteration.

Using this function, sum can be expressed as:

``` stu
value.aggregate($this + $total, 0)
```

Min can be expressed as:

``` stu
value.aggregate(iif($total.empty(), $this, iif($this < $total, $this, $total)))
```

and average would be expressed as:

``` stu
value.aggregate($total + $this, 0) / value.count()
```

## Lexical Elements

FHIRPath defines the following lexical elements:

  Element          Description
  ---------------- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **Whitespace**   Whitespace defines the separation between tokens in the language
  **Comment**      Comments are ignored by the language, allowing for descriptive text
  **Literal**      Literals allow basic values to be represented within the language
  **Symbol**       Symbols such as `+`{.language-plaintext .highlighter-rouge}, `-`{.language-plaintext .highlighter-rouge}, `*`{.language-plaintext .highlighter-rouge}, and `/`{.language-plaintext .highlighter-rouge}
  **Keyword**      Grammar-recognized tokens such as `and`{.language-plaintext .highlighter-rouge}, `or`{.language-plaintext .highlighter-rouge} and `in`{.language-plaintext .highlighter-rouge}
  **Identifier**   Labels such as type names and property names

### Whitespace

FHIRPath defines *tab* (`\t`{.language-plaintext .highlighter-rouge}),
*space* (``{.language-plaintext .highlighter-rouge}), *line feed*
(`\n`{.language-plaintext .highlighter-rouge}) and *carriage return*
(`\r`{.language-plaintext .highlighter-rouge}) as *whitespace*, meaning
they are only used to separate other tokens within the language. Any
number of whitespace characters can appear, and the language does not
use whitespace for anything other than delimiting tokens.

### Comments

FHIRPath defines two styles of comments, *single-line*, and
*multi-line*. A single-line comment consists of two forward slashes,
followed by any text up to the end of the line:

``` fhirpath
2 + 2 // This is a single-line comment
```

To begin a multi-line comment, the typical forward slash-asterisk token
is used. The comment is closed with an asterisk-forward slash, and
everything enclosed is ignored:

``` fhirpath
/*
This is a multi-line comment
Any text enclosed within is ignored
*/
```

### Literals

Literals provide for the representation of values within FHIRPath. The
following types of literals are supported:

  Literal                                                     Description
  ----------------------------------------------------------- -----------------------------------------------------------------------------------------------------------------------------
  **Empty** (`{ }`{.language-plaintext .highlighter-rouge})   The empty collection
  **[Boolean](#boolean)**                                     The boolean literals (`true`{.language-plaintext .highlighter-rouge} and `false`{.language-plaintext .highlighter-rouge})
  **[Integer](#integer)**                                     Sequences of digits in the range 0..2^32^-1
  **[Decimal](#decimal)**                                     Sequences of digits with a decimal point, in the range (-10^28^+1)/10^8^..(10^28^-1)/10^8^
  **[String](#string)**                                       Strings of any character enclosed within single-ticks (`'`{.language-plaintext .highlighter-rouge})
  **[Date](#date)**                                           The at-symbol (`@`{.language-plaintext .highlighter-rouge}) followed by a date (**YYYY-MM-DD**)
  **[DateTime](#datetime)**                                   The at-symbol (`@`{.language-plaintext .highlighter-rouge}) followed by a datetime (**YYYY-MM-DDThh:mm:ss.fff(+\|-)hh:mm**)
  **[Time](#time)**                                           The at-symbol (`@`{.language-plaintext .highlighter-rouge}) followed by a time (**Thh:mm:ss.fff(+\|-)hh:mm**)
  **[Quantity](#quantity)**                                   An integer or decimal literal followed by a datetime precision specifier, or a [\[UCUM\]](#UCUM) unit specifier

For a more detailed discussion of the semantics of each type, refer to
the link for each type.

### Symbols

Symbols provide structure to the language and allow symbolic invocation
of common operators such as addition. FHIRPath defines the following
symbols:

  Symbol                                                      Description
  ----------------------------------------------------------- -----------------------------------------------------------
  `()`{.language-plaintext .highlighter-rouge}                Parentheses for delimiting groups within expressions
  `[]`{.language-plaintext .highlighter-rouge}                Brackets for indexing into lists and strings
  `{}`{.language-plaintext .highlighter-rouge}                Braces for delimiting exclusively empty lists
  `.`{.language-plaintext .highlighter-rouge}                 Period for qualifiers, accessors, and dot-invocation
  `,`{.language-plaintext .highlighter-rouge}                 Comma for delimiting items in a syntactic list
  `= != \<= < > >=`{.language-plaintext .highlighter-rouge}   Comparison operators for comparing values
  `+ - * / \| &`{.language-plaintext .highlighter-rouge}      Arithmetic and other operators for performing computation

### Keywords

Keywords are tokens that are recognized by the parser and used to build
the various language constructs. FHIRPath defines the following
keywords:

  ---------------------------------------------------- ------------------------------------------------------- -------------------------------------------------------- --------------------------------------------------
  `$index`{.language-plaintext .highlighter-rouge}     `div`{.language-plaintext .highlighter-rouge}           `milliseconds`{.language-plaintext .highlighter-rouge}   `true`{.language-plaintext .highlighter-rouge}
  `$this`{.language-plaintext .highlighter-rouge}      `false`{.language-plaintext .highlighter-rouge}         `minute`{.language-plaintext .highlighter-rouge}         `week`{.language-plaintext .highlighter-rouge}
  `$total`{.language-plaintext .highlighter-rouge}     `hour`{.language-plaintext .highlighter-rouge}          `minutes`{.language-plaintext .highlighter-rouge}        `weeks`{.language-plaintext .highlighter-rouge}
  `and`{.language-plaintext .highlighter-rouge}        `hours`{.language-plaintext .highlighter-rouge}         `mod`{.language-plaintext .highlighter-rouge}            `xor`{.language-plaintext .highlighter-rouge}
  `as`{.language-plaintext .highlighter-rouge}         `implies`{.language-plaintext .highlighter-rouge}       `month`{.language-plaintext .highlighter-rouge}          `year`{.language-plaintext .highlighter-rouge}
  `contains`{.language-plaintext .highlighter-rouge}   `in`{.language-plaintext .highlighter-rouge}            `months`{.language-plaintext .highlighter-rouge}         `years`{.language-plaintext .highlighter-rouge}
  `day`{.language-plaintext .highlighter-rouge}        `is`{.language-plaintext .highlighter-rouge}            `or`{.language-plaintext .highlighter-rouge}             `second`{.language-plaintext .highlighter-rouge}
  `days`{.language-plaintext .highlighter-rouge}       `millisecond`{.language-plaintext .highlighter-rouge}   `seconds`{.language-plaintext .highlighter-rouge}         
  ---------------------------------------------------- ------------------------------------------------------- -------------------------------------------------------- --------------------------------------------------

In general, keywords within FHIRPath are also considered *reserved*
words, meaning that it is illegal to use them as identifiers. FHIRPath
keywords are reserved words, with the exception of the following
keywords that may also be used as identifiers:

  ---------------------------------------------- ----------------------------------------------------
  `as`{.language-plaintext .highlighter-rouge}   `contains`{.language-plaintext .highlighter-rouge}
  `is`{.language-plaintext .highlighter-rouge}    
  ---------------------------------------------- ----------------------------------------------------

If necessary, identifiers that clash with a reserved word can be
delimited using a backtick (`` ` ``{.language-plaintext
.highlighter-rouge}):

``` fhirpath
Patient.text.`div`.empty()
```

The `div`{.language-plaintext .highlighter-rouge} element of the
`Patient.text`{.language-plaintext .highlighter-rouge} must be offset
with backticks (`` ` ``{.language-plaintext .highlighter-rouge}) because
`div`{.language-plaintext .highlighter-rouge} is both a keyword and a
reserved word.

### Identifiers

Identifiers are used as labels to allow expressions to reference
elements such as model types and properties. FHIRPath supports two types
of identifiers, *simple* and *delimited*.

A simple identifier is any alphabetical character or an underscore,
followed by any number of alpha-numeric characters or underscores. For
example, the following are all valid simple identifiers:

``` fhirpath
Patient
_id
valueDateTime
_1234
```

A delimited identifier is any sequence of characters enclosed in
backticks (`` ` ``{.language-plaintext .highlighter-rouge}):

``` fhirpath
`QI-Core Patient`
`US-Core Diagnostic Request`
`us-zip`
```

The use of backticks allows identifiers to contains spaces, commas, and
other characters that would not be allowed within simple identifiers.
This allows identifiers to be more descriptive, and also enables
expressions to reference models that have property or type names that
are not valid simple identifiers.

FHIRPath [escape sequences](#string) for strings also work for delimited
identifiers.

When resolving an identifier that is also the root of a FHIRPath
expression, it is resolved as a type name first, and if it resolves to a
type, it must resolve to the type of the context (or a supertype).
Otherwise, it is resolved as a path on the context. If the identifier
cannot be resolved, the evaluation will end and signal an error to the
calling environment.

### Case-Sensitivity

FHIRPath is a case-sensitive language, meaning that case is considered
when matching keywords in the language. However, because FHIRPath can be
used with different models, the case-sensitivity of type and property
names is defined by each model.

## Environment variables

A token introduced by a % refers to a value that is passed into the
evaluation engine by the calling environment. Using environment
variables, authors can avoid repetition of fixed values and can pass in
external values and data.

The following environmental values are set for all contexts:

``` fhirpath
%ucum       // (string) url for UCUM (http://unitsofmeasure.org, per http://hl7.org/fhir/ucum.html)
%context    // The original node that was passed to the evaluation engine before starting evaluation
```

Implementers should note that using additional environment variables is
a formal extension point for the language. Various usages of FHIRPath
may define their own externals, and implementers should provide some
appropriate configuration framework to allow these constants to be
provided to the evaluation engine at run-time. E.g.:

``` fhirpath
%`us-zip` = '[0-9]{5}(-[0-9]{4}){0,1}'
```

Note that the identifier portion of the token is allowed to be either a
simple identifier (as in `%ucum`{.language-plaintext
.highlighter-rouge}), or a delimited identifier to allow for alternative
characters (as in `` %`us-zip` ``{.language-plaintext
.highlighter-rouge}).

Note also that these tokens are not restricted to simple types, and they
may have values that are not defined fixed values known prior to
evaluation at run-time, though there is no way to define these kind of
values in implementation guides.

Attempting to access an undefined environment variable will result in an
error, but accessing a defined environment variable that does not have a
value specified results in empty (`{ }`{.language-plaintext
.highlighter-rouge}).

> **Note:** For backwards compatibility with some existing
> implementations, the token for an environment variable may also be a
> string, as in `%'us-zip'`{.language-plaintext .highlighter-rouge},
> with no difference in semantics.

## Types and Reflection

### Models

Because FHIRPath is defined to work in multiple contexts, each context
provides the definition for the structures available in that context.
These structures are the *model* available for FHIRPath expressions. For
example, within FHIR, the FHIR data types and resources are the model.
To prevent namespace clashes, the type names within each model are
prefixed (or namespaced) with the name of the model. For example, the
fully qualified name of the Patient resource in FHIR is
`FHIR.Patient`{.language-plaintext .highlighter-rouge}. The system types
defined within FHIRPath directly are prefixed with the namespace
`System`{.language-plaintext .highlighter-rouge}.

To allow type names to be referenced in expressions such as the
`is`{.language-plaintext .highlighter-rouge} and
`as`{.language-plaintext .highlighter-rouge} operators, the language
includes a *type specifier*, an optionally qualified identifier that
must resolve to the name of a model type.

When resolving a type name, the context-specific model is searched
first. If no match is found, the `System`{.language-plaintext
.highlighter-rouge} model (containing only the built-in types defined in
the [Literals](#literals) section) is searched.

When resolving an identifier that is also the root of a FHIRPath
expression, it is resolved as a type name first, and if it resolves to a
type, it must resolve to the type of the context (or a supertype).
Otherwise, it is resolved as a path on the context.

### Reflection

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

FHIRPath supports reflection to provide the ability for expressions to
access type information describing the structure of values. The
`type()`{.language-plaintext .highlighter-rouge} function returns the
type information for each element of the input collection, using one of
the following concrete subtypes of `TypeInfo`{.language-plaintext
.highlighter-rouge}:

#### Primitive Types {#primitive-types .stu}

For primitive types such as `String`{.language-plaintext
.highlighter-rouge} and `Integer`{.language-plaintext
.highlighter-rouge}, the result is a
`SimpleTypeInfo`{.language-plaintext .highlighter-rouge}:

::: {.language-typescript .stu .highlighter-rouge}
::: highlight
``` highlight
SimpleTypeInfo { namespace: string, name: string, baseType: TypeSpecifier }
```
:::
:::

For example:

``` stu
('John' | 'Mary').type()
```

Results in:

::: {.language-typescript .stu .highlighter-rouge}
::: highlight
``` highlight
{
  SimpleTypeInfo { namespace: 'System', name: 'String', baseType: 'System.Any' },
  SimpleTypeInfo { namespace: 'System', name: 'String', baseType: 'System.Any' }
}
```
:::
:::

#### Class Types {#class-types .stu}

For class types, the result is a `ClassInfo`{.language-plaintext
.highlighter-rouge}:

::: {.language-typescript .stu .highlighter-rouge}
::: highlight
``` highlight
ClassInfoElement { name: string, type: TypeSpecifier, isOneBased: Boolean }
ClassInfo { namespace: string, name: string, baseType: TypeSpecifier, element: List<ClassInfoElement> }
```
:::
:::

For example:

``` stu
Patient.maritalStatus.type()
```

Results in:

::: {.language-typescript .stu .highlighter-rouge}
::: highlight
``` highlight
{
  ClassInfo {
    namespace: 'FHIR',
    name: 'CodeableConcept',
    baseType: 'FHIR.Element',
    element: {
      ClassInfoElement { name: 'coding', type: 'List<Coding>', isOneBased: false },
      ClassInfoElement { name: 'text', type: 'FHIR.string' }
    }
  }
}
```
:::
:::

#### Collection Types {#collection-types .stu}

For collection types, the result is a `ListTypeInfo`{.language-plaintext
.highlighter-rouge}:

::: {.language-typescript .stu .highlighter-rouge}
::: highlight
``` highlight
ListTypeInfo { elementType: TypeSpecifier }
```
:::
:::

For example:

``` stu
Patient.address.type()
```

Results in:

::: {.language-typescript .stu .highlighter-rouge}
::: highlight
``` highlight
{
  ListTypeInfo { elementType: 'FHIR.Address' }
}
```
:::
:::

#### Anonymous Types {#anonymous-types .stu}

Anonymous types are structured types that have no associated name, only
the elements of the structure. For example, in FHIR, the
`Patient.contact`{.language-plaintext .highlighter-rouge} element has
multiple sub-elements, but is not explicitly named. For types such as
this, the result is a `TupleTypeInfo`{.language-plaintext
.highlighter-rouge}:

::: {.language-typescript .stu .highlighter-rouge}
::: highlight
``` highlight
TupleTypeInfoElement { name: string, type: TypeSpecifier, isOneBased: Boolean }
TupleTypeInfo { element: List<TupleTypeInfoElement> }
```
:::
:::

For example:

``` stu
Patient.contact.single().type()
```

Results in:

::: {.language-typescript .stu .highlighter-rouge}
::: highlight
``` highlight
{
  TupleTypeInfo {
    element: {
      TupleTypeInfoElement { name: 'relationship', type: 'List<FHIR.CodeableConcept>', isOneBased: false },
      TupleTypeInfoElement { name: 'name', type: 'FHIR.HumanName', isOneBased: false },
      TupleTypeInfoElement { name: 'telecom', type: 'List<FHIR.ContactPoint>', isOneBased: false },
      TupleTypeInfoElement { name: 'address', type: 'FHIR.Address', isOneBased: false },
      TupleTypeInfoElement { name: 'gender', type: 'FHIR.code', isOneBased: false },
      TupleTypeInfoElement { name: 'organization', type: 'FHIR.Reference', isOneBased: false },
      TupleTypeInfoElement { name: 'period', type: 'FHIR.Period', isOneBased: false }
    }
  }
}
```
:::
:::

> **Note:** These structures are a subset of the abstract metamodel used
> by the [Clinical Quality Language
> Tooling](https://github.com/cqframework/clinical_quality_language).

## Type safety and strict evaluation

Strongly typed languages are intended to help authors avoid mistakes by
ensuring that the expressions describe meaningful operations. For
example, a strongly typed language would typically disallow the
expression:

``` fhirpath
1 + 'John'
```

because it performs an invalid operation, namely adding numbers and
strings. However, there are cases where the author knows that a
particular invocation may be safe, but the compiler is not aware of, or
cannot infer, the reason. In these cases, type-safety errors can become
an unwelcome burden, especially for experienced developers.

Because FHIRPath may be used in different situations and environments
requiring different levels of type safety, implementations may make
different choices about how much type checking should be done at
compile-time versus run-time, and in what situations. Some
implementations requiring a high degree of type-safety may choose to
perform strict type-checking at compile-time for all invocations. On the
other hand, some implementations may be unconcerned with compile-time
versus run-time checking and may choose to defer all correctness checks
to run-time.

For example, since some functions and most operators will only accept a
single item as input (and throw a run-time exception otherwise):

``` fhirpath
Patient.name.given + ' ' + Patient.name.family
```

will work perfectly fine, as long as the patient has a single name, but
will fail otherwise. It is in fact \"safer\" to formulate such
statements as either:

``` fhirpath
Patient.name.select(given + ' ' + family)
```

which would return a collection of concatenated first and last names,
one for each name of a patient. Of course, if the patient turns out to
have multiple given names, even this statement will fail and the author
would need to choose the first name in each collection explicitly:

``` fhirpath
Patient.name.first().select(given.first() + ' ' + family.first())
```

It is clear that, although more robust, the last expression is also much
more elaborate, certainly in situations where, because of external
constraints, the author is sure names will not repeat, even if the
unconstrained object model allows repetition.

Apart from throwing exceptions, unexpected outcomes may result because
of the way the equality operators are defined. The expression

``` fhirpath
Patient.name.given = 'Wouter'
```

will return false as soon as a patient has multiple names, even though
one of those may well be \'Wouter\'. Again, this can be corrected:

``` fhirpath
Patient.name.where(given = 'Wouter').exists()
```

but is still less concise than would be possible if constraints were
well known in advance.

In cases where compile-time checking like this is desirable,
implementations may choose to protect against such cases by employing
strict typing. Based on the definitions of the operators and functions
involved in the expression, and given the types of the inputs, a
compiler can analyze the expression and determine whether \"unsafe\"
situations can occur.

Unsafe uses are:

-   A function that requires an input collection with a single item is
    called on an output that is not guaranteed to have only one item.
-   A function is passed an argument that is not guaranteed to be a
    single value.
-   A function is passed an input value or argument that is not of the
    expected type
-   An operator that requires operands to be collections with a single
    item is called with arguments that are not guaranteed to have only
    one item.
-   An operator has operands that are not of the expected type
-   Equality operators are used on operands that are not both
    collections or collections containing a single item of the same
    type.

There are a few constructs in the FHIRPath language where the compiler
cannot determine the type:

-   The `children()`{.language-plaintext .highlighter-rouge} and
    `descendants()`{.language-plaintext .highlighter-rouge} functions
-   The `resolve()`{.language-plaintext .highlighter-rouge} function
-   A member which is polymorphic (e.g. a
    `choice[x]`{.language-plaintext .highlighter-rouge} type in FHIR)

Note that the `resolve()`{.language-plaintext .highlighter-rouge}
function is defined by the FHIR context, it is not part of FHIRPath
directly. For more information see the
[FHIRPath](https://hl7.org/fhir/fhirpath.html#functions) section of the
FHIR specification.

Authors can use the `as`{.language-plaintext .highlighter-rouge}
operator or `ofType()`{.language-plaintext .highlighter-rouge} function
directly after such constructs to inform the compiler of the expected
type.

In cases where a compiler finds places where a collection of multiple
items can be present while just a single item is expected, the author
will need to make explicit how repetitions are dealt with. Depending on
the situation one may:

-   Use `first()`{.language-plaintext .highlighter-rouge},
    `last()`{.language-plaintext .highlighter-rouge} or indexer
    (`[ ]`{.language-plaintext .highlighter-rouge}) to select a single
    item
-   Use `select()`{.language-plaintext .highlighter-rouge} and
    `where()`{.language-plaintext .highlighter-rouge} to turn the
    expression into one that evaluates each of the repeating items
    individually (as in the examples above)

## Formal Specifications

### Formal Syntax

The formal syntax for FHIRPath is specified as an [Antlr
4.0](http://www.antlr.org/) grammar file (g4) and included in this
specification at the following link:

[grammar.html](grammar.html)

> **Note:** If there are discrepancies between this documentation and
> the grammar included at the above link, the grammar is considered the
> source of truth.

### Model Information

The model information returned by the reflection function
`type()`{.language-plaintext .highlighter-rouge} is specified as an XML
Schema document (xsd) and included in this specification at the
following link:

[modelinfo.xsd](modelinfo.xsd)

> **Note:** The model information file included here is not a normative
> aspect of the FHIRPath specification. It is the same model information
> file used by the [Clinical Quality Framework
> Tooling](http://github.com/cqframework/clinical_quality_language) and
> is included for reference as a simple formalism that meets the
> requirements described in the normative [Reflection](#reflection)
> section above.

As discussed in the section on case-sensitivity, each model used within
FHIRPath determines whether or not identifiers in the model are
case-sensitive. This information is provided as part of the model
information and tooling should respect the case-sensitive settings for
each model.

### URI and Media Types

To uniquely identify the FHIRPath language, the following URI is
defined:

``` txt
http://hl7.org/fhirpath
```

In addition, a media type is defined to support describing FHIRPath
content:

``` txt
text/fhirpath
```

> **Note:** The appendices are included for informative purposes and are
> not a normative part of the specification.

[]{#hl7v2}

## Use of FHIRPath on HL7 Version 2 messages {#use-of-fhirpath-on-hl7-version-2-messages .appendix}

FHIRPath can be used against HL7 V2 messages. This UML diagram
summarizes the Object Model on which the FHIRPath statements are
written:

![Class Model for HL7
V2](v2-class-model.png){height="456\",width=\"760"}

In this Object Model:

-   The object graph always starts with a message.
-   Each message has a list of segments.
-   In addition, Abstract Message Syntax is available through the
    groups() function, for use where the message follows the Abstract
    Message Syntax sufficiently for the parser to reconcile the segment
    list with the structure.
-   The names of the groups are the names published in the
    specification, e.g. \'PATIENT_OBSERVATION\' (with spaces, where
    present, replaced by underscores. In case of doubt, consult the V2
    XML schemas).
-   Each Segment has a list of fields, which each have a list of
    \"Cells\". This is necessary to allow for repeats, but users are
    accustomed to just jumping to Element - use the function elements()
    which returns all repeats with the given index.
-   A \"cell\" can be either an Element, a Component or a
    Sub-Components. Elements can contain Components, which can contain
    Sub-Components. Sub-Sub-Components are not allowed.
-   Calls may have a simple text content, or a series of
    (sub-)components. The simple() function returns either the text, if
    it exists, or the return value of simple() from the first component
-   A V2 data type (e.g. ST, SN, CE etc) is a profile on Cell that
    specifies whether it has simple content, or complex content.
-   todo: this object model doesn\'t make provision for non-syntax
    escapes in the simple content (e.g. `\.b\`{.language-plaintext
    .highlighter-rouge}).
-   all the lists are 1 based. That means the first item in the list is
    numbered 1, not 0.

Some example queries:

``` fhirpath
Message.segment.where(code = 'PID').field[3].element.first().simple()
```

Get the value of the first component in the first repeat of PID-3

``` fhirpath
Message.segment[2].elements(3).simple()
```

Get a collection with is the string values of all the repeats in the 3rd
element of the 2nd segment. Typically, this assumes that there are no
repeats, and so this is a simple value.

``` fhirpath
Message.segment.where(code = 'PID').field[3].element.where(component[4].value = 'MR').simple()
```

Pick out the MR number from PID-3 (assuming, in this case, that there\'s
only one PID segment in the message. No good for an A17). Note that this
returns the whole Cell - e.g. `|value^^MR|`{.language-plaintext
.highlighter-rouge}, though often more components will be present)

``` fhirpath
Message.segment.where(code = 'PID').elements(3).where(component[4].value = 'MR').component[1].text
```

Same as the last, but pick out just the MR value

``` fhirpath
Message.group('PATIENT').group('PATIENT_OBSERVATION').item.ofType(Segment)
  .where(code = 'OBX' and elements(2).exists(components(2) = 'LN')))
```

Return any OBXs from the patient observations (and ignore others e.g. in
a R01 message) segments that have LOINC codes. Note that if the parser
cannot properly parse the Abstract Message Syntax, group() must fail
with an error message.

## FHIRPath Tooling and Implementation {#fhirpath-tooling-and-implementation .appendix}

The list of known tooling and implementation projects for the FHIRPath
language has been moved to the [HL7 confluence
site](https://confluence.hl7.org/display/FHIRI/FHIRPath+Implementations){target="_blank"}

## References {#references .appendix}

[]{#bibliography}

-   []{#ANTLR}\[ANTLR\] Another Tool for Language Recognition (ANTLR)
    [http://www.antlr.org/](http://www.antlr.org/){target="_blank"}
-   []{#ISO8601}\[ISO8601\] Date and time format - ISO 8601.
    [https://www.iso.org/iso-8601-date-and-time-format.html](https://www.iso.org/iso-8601-date-and-time-format.html){target="_blank"}
-   []{#CQL}\[CQL\] HL7 Cross-Paradigm Specification: Clinical Quality
    Language, Release 1, STU Release 1.3.
    [http://www.hl7.org/implement/standards/product_brief.cfm?product_id=400](http://www.hl7.org/implement/standards/product_brief.cfm?product_id=400){target="_blank"}
-   []{#MOF}\[MOF\] Meta Object Facility.
    [https://www.omg.org/spec/MOF/](https://www.omg.org/spec/MOF/){target="_blank"},
    version 2.5.1, November 2016
-   []{#XMLRE}\[XMLRE\] Regular Expressions. XML Schema 1.1.
    [https://www.w3.org/TR/xmlschema11-2/#regexs](https://www.w3.org/TR/xmlschema11-2/#regexs){target="_blank"}
-   []{#PCRE}\[PCRE\] Pearl-Compatible Regular Expressions.
    [http://www.pcre.org/](http://www.pcre.org/){target="_blank"}
-   []{#UCUM}\[UCUM\] Unified Code for Units of Measure (UCUM)
    [http://unitsofmeasure.org/ucum.html](http://unitsofmeasure.org/ucum.html){target="_blank"},
    Version 2.1, Revision 442 (2017-11-21)
-   []{#FHIR}\[FHIR\] HL7 Fast Healthcare Interoperability Resources
    [http://hl7.org/fhir](http://hl7.org/fhir){target="_blank"}
-   [grammar.html](grammar.html)
-   [modelinfo.xsd](modelinfo.xsd)
-   []{#fluent}\[Fluent\] Fluent interface pattern.
    [https://en.wikipedia.org/wiki/Fluent_interface](https://en.wikipedia.org/wiki/Fluent_interface){target="_blank"}

## Dependencies

<div>

  [IG]{title="Implementation Guide Reference"}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         [Package]{title="The NPM Package Id"}                                                                [FHIR]{title="The version of FHIR that the package is based on"}   [Comment]{title="Explains why this dependency exists"}
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- ---------------------------------------------------------------------------------------------------- ------------------------------------------------------------------ --------------------------------------------------------------------------------
  ![.](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAAWCAYAAAABxvaqAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAB3RJTUUH3wYeFzIs1vtcMQAAAB1pVFh0Q29tbWVudAAAAAAAQ3JlYXRlZCB3aXRoIEdJTVBkLmUHAAAAE0lEQVQI12P4//8/AxMDAwNdCABMPwMo2ctnoQAAAABJRU5ErkJggg==){.hierarchy style="background-color: inherit"}![.](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAAACXBIWXMAAAsTAAALEwEAmpwYAAAAB3RJTUUH3QYZCik0qECmbAAAAB1pVFh0Q29tbWVudAAAAAAAQ3JlYXRlZCB3aXRoIEdJTVBkLmUHAAABtUlEQVQoz2P8//8/Ayb4///v8xfM4iIMzKxoMkwMOMC3DVv/vXmEKY5Dw///Pw8c+Xn2CLEa/r55+/P07t/nrxKl4f/PX6+8g1mlP/44c4eAhn8fP33bsOV1dBoT51V2+z/MohIfeyYy/P2LXcPfFy8/NLb9efTs//utgjUfft35xRUY8Hnm/N/Xb2LT8P//h5ZOnvjoj+11giXcTH//MTLp/v/x9c/z279uXsOi4e/LRwzsbz6vmMyTLM6m9fnXE3Y2l+yv+yez2nL9eX0Wi4afF5eyW4r/+3qYP1Xjz/N3P17ZsRsKMrJd4rL9wcTxDIsGZqEvLEKv2VT/Mgl+/XGbXSB72s/zxXyBYmwqLCxSLFg0sKkHsAg+YJX7xfBTmt204s/NXE4dQTZtT2beH6yqFv9ebkDXwMhnxixrySb68f9fcWYRTSbGA6ya2QwMsixCvxk/djByiWIJVkbBBGaNOoafO5nYWVlkPRjYtP5/388s+JdJkI+R1xquDMl9zOqMQioM7BIMjJ8Z2IUYWOQZeT0Y/u5mEF+G7AdGLMn7712GnycY/v1g+HOZga+WgUkYWRIAe6ip9OnE1GoAAAAASUVORK5CYII= "NPM Package"){.hierarchy style="background-color: white; background-color: inherit"} [FHIRPath Specification]{title="Canonical: http://hl7.org/fhirpath"}                                                                                                                                                                                                                                                                                                                                                                                 [hl7.fhirpath#3.0.0-ballot](https://simplifier.net/packages/hl7.fhirpath/3.0.0-ballot)               [R4](http://hl7.org/fhir/R4)                                       
  ![.](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAAWCAYAAAABxvaqAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAB3RJTUUH3wYeFzIs1vtcMQAAAB1pVFh0Q29tbWVudAAAAAAAQ3JlYXRlZCB3aXRoIEdJTVBkLmUHAAAAE0lEQVQI12P4//8/AxMDAwNdCABMPwMo2ctnoQAAAABJRU5ErkJggg==){.hierarchy style="background-color: inherit"}![.](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAWCAYAAADJqhx8AAAACXBIWXMAAAsTAAALEwEAmpwYAAAAB3RJTUUH3wYeFzI3XJ6V3QAAAB1pVFh0Q29tbWVudAAAAAAAQ3JlYXRlZCB3aXRoIEdJTVBkLmUHAAAANklEQVQ4y+2RsQ0AIAzDav7/2VzQwoCY4iWbZSmo1QGoUgNMghvWaIejPQW/CrrNCylIwcOCDYfLNRcNer4SAAAAAElFTkSuQmCC){.hierarchy style="background-color: inherit"}![.](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAAACXBIWXMAAAsTAAALEwEAmpwYAAAAB3RJTUUH3QYZCik0qECmbAAAAB1pVFh0Q29tbWVudAAAAAAAQ3JlYXRlZCB3aXRoIEdJTVBkLmUHAAABtUlEQVQoz2P8//8/Ayb4///v8xfM4iIMzKxoMkwMOMC3DVv/vXmEKY5Dw///Pw8c+Xn2CLEa/r55+/P07t/nrxKl4f/PX6+8g1mlP/44c4eAhn8fP33bsOV1dBoT51V2+z/MohIfeyYy/P2LXcPfFy8/NLb9efTs//utgjUfft35xRUY8Hnm/N/Xb2LT8P//h5ZOnvjoj+11giXcTH//MTLp/v/x9c/z279uXsOi4e/LRwzsbz6vmMyTLM6m9fnXE3Y2l+yv+yez2nL9eX0Wi4afF5eyW4r/+3qYP1Xjz/N3P17ZsRsKMrJd4rL9wcTxDIsGZqEvLEKv2VT/Mgl+/XGbXSB72s/zxXyBYmwqLCxSLFg0sKkHsAg+YJX7xfBTmt204s/NXE4dQTZtT2beH6yqFv9ebkDXwMhnxixrySb68f9fcWYRTSbGA6ya2QwMsixCvxk/djByiWIJVkbBBGaNOoafO5nYWVlkPRjYtP5/388s+JdJkI+R1xquDMl9zOqMQioM7BIMjJ8Z2IUYWOQZeT0Y/u5mEF+G7AdGLMn7712GnycY/v1g+HOZga+WgUkYWRIAe6ip9OnE1GoAAAAASUVORK5CYII= "NPM Package"){.hierarchy style="background-color: #F7F7F7; background-color: inherit"} [HL7 Terminology (THO)](http://terminology.hl7.org/6.2.0 "Canonical: http://terminology.hl7.org")        [hl7.terminology.r4#6.2.0](https://simplifier.net/packages/hl7.terminology.r4/6.2.0)                 [R4](http://hl7.org/fhir/R4)                                       Automatically added as a dependency - all IGs depend on HL7 Terminology
  ![.](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAAWCAYAAAABxvaqAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAB3RJTUUH3wYeFzIs1vtcMQAAAB1pVFh0Q29tbWVudAAAAAAAQ3JlYXRlZCB3aXRoIEdJTVBkLmUHAAAAE0lEQVQI12P4//8/AxMDAwNdCABMPwMo2ctnoQAAAABJRU5ErkJggg==){.hierarchy style="background-color: inherit"}![.](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAWCAYAAADJqhx8AAAACXBIWXMAAAsTAAALEwEAmpwYAAAAB3RJTUUH3wYeFzME+lXFigAAAB1pVFh0Q29tbWVudAAAAAAAQ3JlYXRlZCB3aXRoIEdJTVBkLmUHAAAANklEQVQ4y+3OsRUAIAjEUOL+O8cJABttJM11/x1qZAGqRBEVcNIqdWj1efDqQbb3HwwwwEfABmQUHSPM9dtDAAAAAElFTkSuQmCC){.hierarchy style="background-color: inherit"}![.](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAAACXBIWXMAAAsTAAALEwEAmpwYAAAAB3RJTUUH3QYZCik0qECmbAAAAB1pVFh0Q29tbWVudAAAAAAAQ3JlYXRlZCB3aXRoIEdJTVBkLmUHAAABtUlEQVQoz2P8//8/Ayb4///v8xfM4iIMzKxoMkwMOMC3DVv/vXmEKY5Dw///Pw8c+Xn2CLEa/r55+/P07t/nrxKl4f/PX6+8g1mlP/44c4eAhn8fP33bsOV1dBoT51V2+z/MohIfeyYy/P2LXcPfFy8/NLb9efTs//utgjUfft35xRUY8Hnm/N/Xb2LT8P//h5ZOnvjoj+11giXcTH//MTLp/v/x9c/z279uXsOi4e/LRwzsbz6vmMyTLM6m9fnXE3Y2l+yv+yez2nL9eX0Wi4afF5eyW4r/+3qYP1Xjz/N3P17ZsRsKMrJd4rL9wcTxDIsGZqEvLEKv2VT/Mgl+/XGbXSB72s/zxXyBYmwqLCxSLFg0sKkHsAg+YJX7xfBTmt204s/NXE4dQTZtT2beH6yqFv9ebkDXwMhnxixrySb68f9fcWYRTSbGA6ya2QwMsixCvxk/djByiWIJVkbBBGaNOoafO5nYWVlkPRjYtP5/388s+JdJkI+R1xquDMl9zOqMQioM7BIMjJ8Z2IUYWOQZeT0Y/u5mEF+G7AdGLMn7712GnycY/v1g+HOZga+WgUkYWRIAe6ip9OnE1GoAAAAASUVORK5CYII= "NPM Package"){.hierarchy style="background-color: white; background-color: inherit"} [FHIR Extensions Pack](http://hl7.org/fhir/extensions/5.1.0 "Canonical: http://hl7.org/fhir/extensions")   [hl7.fhir.uv.extensions.r4#5.1.0](https://simplifier.net/packages/hl7.fhir.uv.extensions.r4/5.1.0)   [R4](http://hl7.org/fhir/R4)                                       Automatically added as a dependency - all IGs depend on the HL7 Extension Pack

</div>

## Cross Version Analysis

This is an R4 IG. None of the features it uses are changed in R4B, so it
can be used as is with R4B systems. Packages for both [R4
(hl7.fhirpath.r4)](package.r4.tgz) and [R4B
(hl7.fhirpath.r4b)](package.r4b.tgz) are available.

## Global Profiles

*There are no Global profiles defined*

## IP Statements

This publication includes IP covered under the following statements.

-   The UCUM codes, UCUM table (regardless of format), and UCUM
    Specification are copyright 1999-2009, Regenstrief Institute, Inc.
    and the Unified Codes for Units of Measures (UCUM) Organization. All
    rights reserved. <https://ucum.org/trac/wiki/TermsOfUse>

    ::: {#ipp_1 onclick="if (document.getElementById('ipp2_1').innerHTML != '') {document.getElementById('ipp_1').innerHTML = document.getElementById('ipp2_1').innerHTML; document.getElementById('ipp2_1').innerHTML = ''}"}
    [Show
    Usage]{style="cursor: pointer; border: 1px grey solid; background-color: #fcdcb3; padding-left: 3px; padding-right: 3px; color: black"}
    :::

    ::: {#ipp2_1 style="display: none"}
    -   [Unified Code for Units of Measure
        (UCUM)](http://terminology.hl7.org/6.2.0/CodeSystem-v3-ucum.html):
        [Observation/example](Observation-example.html)
    :::
-   This material contains content from [LOINC](http://loinc.org). LOINC
    is copyright © 1995-2020, Regenstrief Institute, Inc. and the
    Logical Observation Identifiers Names and Codes (LOINC) Committee
    and is available at no cost under the
    [license](http://loinc.org/license). LOINC® is a registered United
    States trademark of Regenstrief Institute, Inc.

    ::: {#ipp_2 onclick="if (document.getElementById('ipp2_2').innerHTML != '') {document.getElementById('ipp_2').innerHTML = document.getElementById('ipp2_2').innerHTML; document.getElementById('ipp2_2').innerHTML = ''}"}
    [Show
    Usage]{style="cursor: pointer; border: 1px grey solid; background-color: #fcdcb3; padding-left: 3px; padding-right: 3px; color: black"}
    :::

    ::: {#ipp2_2 style="display: none"}
    -   [LOINC](http://terminology.hl7.org/6.2.0/CodeSystem-v3-loinc.html):
        [Observation/example](Observation-example.html)
    :::
-   This material contains content that is copyright of SNOMED
    International. Implementers of these specifications must have the
    appropriate SNOMED CT Affiliate license - for more information
    contact <https://www.snomed.org/get-snomed> or <info@snomed.org>.

    ::: {#ipp_3 onclick="if (document.getElementById('ipp2_3').innerHTML != '') {document.getElementById('ipp_3').innerHTML = document.getElementById('ipp2_3').innerHTML; document.getElementById('ipp2_3').innerHTML = ''}"}
    [Show
    Usage]{style="cursor: pointer; border: 1px grey solid; background-color: #fcdcb3; padding-left: 3px; padding-right: 3px; color: black"}
    :::

    ::: {#ipp2_3 style="display: none"}
    -   [SNOMED Clinical Terms&reg; (SNOMED
        CT&reg;)](http://hl7.org/fhir/R4/codesystem-snomedct.html):
        [CQFQuestionnaire2012](Questionnaire-3141.html),
        [CalendarUnits](CodeSystem-calendar-units.html) and
        [Observation/example](Observation-example.html)
    :::
-   This material derives from the HL7 Terminology (THO). THO is
    copyright ©1989+ Health Level Seven International and is made
    available under the CC0 designation. For more licensing information
    see: <https://terminology.hl7.org/license.html>

    ::: {#ipp_4 onclick="if (document.getElementById('ipp2_4').innerHTML != '') {document.getElementById('ipp_4').innerHTML = document.getElementById('ipp2_4').innerHTML; document.getElementById('ipp2_4').innerHTML = ''}"}
    [Show
    Usage]{style="cursor: pointer; border: 1px grey solid; background-color: #fcdcb3; padding-left: 3px; padding-right: 3px; color: black"}
    :::

    ::: {#ipp2_4 style="display: none"}
    -   [DataAbsentReason](http://terminology.hl7.org/6.2.0/CodeSystem-data-absent-reason.html):
        [CQFQuestionnaire2012](Questionnaire-3141.html)
    -   [Observation Category
        Codes](http://terminology.hl7.org/6.2.0/CodeSystem-observation-category.html):
        [Observation/example](Observation-example.html)
    -   [contactRole2](http://terminology.hl7.org/6.2.0/CodeSystem-v2-0131.html):
        [Patient/example](Patient-example.html)
    -   [identifierType](http://terminology.hl7.org/6.2.0/CodeSystem-v2-0203.html):
        [Patient/example](Patient-example.html)
    :::
:::
:::
:::
:::
:::

[]{#bottom}

::: {#segment-footer .segment igtool="footer"}
::: container
::: inner-wrapper
IG © 2014+ [Implementable Technology Specifications
WG](http://www.hl7.org/Special/committees/xml/index.cfm){style="color:var(--footer-hyperlink-text-color)"}.
Package hl7.fhirpath#3.0.0-ballot based on [FHIR
4.0.1](http://hl7.org/fhir/R4/){style="color: var(--footer-hyperlink-text-color)"}.
Generated [2025-01-28]{title="Tue, Jan 28, 2025 21:54+0000"}\
[ Links: [Table of
Contents](toc.html){style="color: var(--footer-hyperlink-text-color)"}
\| [QA
Report](qa.html){style="color: var(--footer-hyperlink-text-color)"}
]{style="color: var(--footer-highlight-text-color)"}
:::
:::
:::

::: {#segment-post-footer .segment .hidden}
::: container
:::
:::

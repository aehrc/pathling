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

-   The `children()` and
    `descendants()` functions
-   The `resolve()` function
-   A member which is polymorphic (e.g. a
    `choice[x]` type in FHIR)

Note that the `resolve()`
function is defined by the FHIR context, it is not part of FHIRPath
directly. For more information see the
[FHIRPath](https://hl7.org/fhir/fhirpath.html#functions) section of the
FHIR specification.

Authors can use the `as`
operator or `ofType()` function
directly after such constructs to inform the compiler of the expected
type.

In cases where a compiler finds places where a collection of multiple
items can be present while just a single item is expected, the author
will need to make explicit how repetitions are dealt with. Depending on
the situation one may:

-   Use `first()`,
    `last()` or indexer
    (`[ ]`) to select a single
    item
-   Use `select()` and
    `where()` to turn the
    expression into one that evaluates each of the repeating items
    individually (as in the examples above)
## Path selection

FHIRPath allows navigation through the tree by composing a path of
concatenated labels, e.g.

``` fhirpath
name.given
```

This would result in a collection of nodes, one with the value
`'Wouter'` and one with the
value `'Gert'`. In fact, each
step in such a path results in a collection of nodes by selecting nodes
with the given label from the step before it. The input collection at
the beginning of the evaluation contained all elements from Patient, and
the path `name` selected just
those named `name`. Since the
`name` element repeats, the next
step `given` along the path,
will contain all nodes labeled `given` from all nodes `name` in the preceding step.

The path may start with the type of the root node (which otherwise does
not have a name), but this is optional. To illustrate this point, the
path `name.given` above can be
evaluated as an expression on a set of data of any type. However the
expression may be prefixed with the name of the type of the root:

``` fhirpath
Patient.name.given
```

The two expressions have the same outcome, but when evaluating the
second, the evaluation will only produce results when used on data of
type `Patient`. When resolving
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
    `distinct()` and the union
    operator `|` produce
    collections of unique elements, but in general, duplicate elements
    are allowed.
-   Indexed - Each item in a collection can be addressed by its index,
    i.e. ordinal position within the collection (e.g.
    `a[2]`).
-   Unless specified otherwise by the underlying Object Model, the first
    item in a collection has index 0. Note that if the underlying model
    specifies that a collection is 1-based (the only reasonable
    alternative to 0-based collections), *any collections generated from
    operations on the 1-based list are 0-based*.
-   Countable - The number of items in a given collection can always be
    determined using the `count()`
    .highlighter-rouge} function

Note that the outcome of functions like `children()` and `descendants()` cannot be assumed to be in any meaningful order, and
`first()`,
`last()`,
`tail()`,
`skip()` and
`take()` should not be used on
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

The `is` operator can be used to
determine whether or not a given value is of a given type:

``` fhirpath
Observation.value is Quantity // returns true if the value is of type Quantity
```

The `as` operator can be used to
treat a value as a specific type:

``` fhirpath
Observation.value as Quantity // returns value as a Quantity if it is of type Quantity, and an empty result otherwise
```

The list of available types that can be passed as an argument to the
`ofType()` function and
`is` and
`as` operators is determined by
the underlying object model. Within FHIRPath, they are just identifiers,
either delimited or simple.
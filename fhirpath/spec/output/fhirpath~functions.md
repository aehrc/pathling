## Functions

Functions are distinguished from path navigation names by the fact that
they are followed by a `()` with
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
    single value (e.g. `startsWith(prefix: String)`
    .highlighter-rouge}) and it is passed an argument that evaluates to
    a collection with multiple items, or to a collection with an item
    that is not of the required type (or cannot be converted to the
    required type), the evaluation of the expression will end and an
    error will be signaled to the calling environment.
-   If the function takes an `expression`
    .highlighter-rouge} as a parameter, the function will evaluate the
    expression passed for the parameter with respect to each of the
    items in the input collection. These expressions may refer to the
    special `$this` and
    `$index` elements, which
    represent the item from the input collection currently under
    evaluation, and its index in the collection, respectively. For
    example, in
    `name.given.where($this > 'ba' and $this < 'bc')`
    .fhirpath .highlighter-rouge} the `where()`
    .highlighter-rouge} function will iterate over each item in the
    input collection (elements named `given`
    .highlighter-rouge}) and `$this`
    .highlighter-rouge} will be set to each item when the expression
    passed to `where()` is
    evaluated.

For the [aggregate](#aggregates) function, expressions may also refer to
the special `$total` element,
representing the result of the aggregation.

Note that the bracket notation in function signatures indicates optional
parameters.

Note also that although all functions return collections, if a given
function is defined to return a single element, the return type is
simplified to just the type of the single element, rather than the list
type.

### Existence

#### empty() : Boolean 

Returns `true` if the input
collection is empty (`{ }`) and
`false` otherwise.

#### exists(\[criteria : expression\]) : Boolean 

Returns `true` if the input
collection has any elements (optionally filtered by the criteria), and
`false` otherwise. This is the
opposite of `empty()`, and as
such is a shorthand for `empty().not()`. If the input collection is empty
(`{ }`), the result is
`false`.

Using the optional criteria can be considered a shorthand for
`where(criteria).exists()`.

Note that a common term for this function is *any*.

The following examples illustrate some potential uses of the
`exists()` function:

``` fhirpath
Patient.name.exists()
Patient.identifier.exists(use = 'official')
Patient.telecom.exists(system = 'phone' and use = 'mobile')
Patient.generalPractitioner.exists(resolve() is Practitioner) // this example is wrong
```

The first example returns `true`
if the `Patient` has any
`name` elements.

The second example returns `true` if the `Patient` has any `identifier` elements that have a `use` element equal to `'official'`.

The third example returns `true`
if the `Patient` has any
`telecom` elements that have a
`system` element equal to
`'phone'` and a
`use` element equal to
`'mobile'`.

And finally, the fourth example returns `true` if the `Patient` has any `generalPractitioner` elements of type `Practitioner`.

#### all(criteria : expression) : Boolean 

Returns `true` if for every
element in the input collection, `criteria` evaluates to `true`. Otherwise, the result is
`false`. If the input collection
is empty (`{ }`), the result is
`true`.

``` fhirpath
generalPractitioner.all($this.resolve() is Practitioner)
```

This example returns true if all of the
`generalPractitioner` elements
are of type `Practitioner`.

#### allTrue() : Boolean 

Takes a collection of Boolean values and returns
`true` if all the items are
`true`. If any items are
`false`, the result is
`false`. If the input is empty
(`{ }`), the result is
`true`.

The following example returns `true` if all of the components of the Observation have a
value greater than 90 mm\[Hg\]:

``` fhirpath
Observation.select(component.value > 90 'mm[Hg]').allTrue()
```

#### anyTrue() : Boolean 

Takes a collection of Boolean values and returns
`true` if any of the items are
`true`. If all the items are
`false`, or if the input is
empty (`{ }`), the result is
`false`.

The following example returns `true` if any of the components of the Observation have a
value greater than 90 mm\[Hg\]:

``` fhirpath
Observation.select(component.value > 90 'mm[Hg]').anyTrue()
```

#### allFalse() : Boolean 

Takes a collection of Boolean values and returns
`true` if all the items are
`false`. If any items are
`true`, the result is
`false`. If the input is empty
(`{ }`), the result is
`true`.

The following example returns `true` if none of the components of the Observation have a
value greater than 90 mm\[Hg\]:

``` fhirpath
Observation.select(component.value > 90 'mm[Hg]').allFalse()
```

#### anyFalse() : Boolean 

Takes a collection of Boolean values and returns
`true` if any of the items are
`false`. If all the items are
`true`, or if the input is empty
(`{ }`), the result is
`false`.

The following example returns `true` if any of the components of the Observation have a
value that is not greater than 90 mm\[Hg\]:

``` fhirpath
Observation.select(component.value > 90 'mm[Hg]').anyFalse()
```

#### subsetOf(other : collection) : Boolean 

Returns `true` if all items in
the input collection are members of the collection passed as the
`other` argument. Membership is
determined using the [equals](#equals) (`=`) operation.

Conceptually, this function is evaluated by testing each element in the
input collection for membership in the `other` collection, with a default of
`true`. This means that if the
input collection is empty (`{ }`), the result is `true`, otherwise if the `other` collection is empty (`{ }`), the result is `false`.

The following example returns true if the tags defined in any contained
resource are a subset of the tags defined in the MedicationRequest
resource:

``` fhirpath
MedicationRequest.contained.meta.tag.subsetOf(MedicationRequest.meta.tag)
```

#### supersetOf(other : collection) : Boolean 

Returns `true` if all items in
the collection passed as the `other` argument are members of the input collection.
Membership is determined using the [equals](#equals)
(`=`) operation.

Conceptually, this function is evaluated by testing each element in the
`other` collection for
membership in the input collection, with a default of
`true`. This means that if the
`other` collection is empty
(`{ }`), the result is
`true`, otherwise if the input
collection is empty (`{ }`), the
result is `false`.

The following example returns true if the tags defined in any contained
resource are a superset of the tags defined in the MedicationRequest
resource:

``` fhirpath
MedicationRequest.contained.meta.tag.supersetOf(MedicationRequest.meta.tag)
```

#### count() : Integer 

Returns the integer count of the number of items in the input
collection. Returns 0 when the input collection is empty.

#### distinct() : collection 

Returns a collection containing only the unique items in the input
collection. To determine whether two items are the same, the
[equals](#equals) (`=`) operator
is used, as defined below.

If the input collection is empty (`{ }`), the result is empty.

Note that the order of elements in the input collection is not
guaranteed to be preserved in the result.

The following example returns the distinct list of tags on the given
Patient:

``` fhirpath
Patient.meta.tag.distinct()
```

#### isDistinct() : Boolean 

Returns `true` if all the items
in the input collection are distinct. To determine whether two items are
distinct, the [equals](#equals) (`=`) operator is used, as defined below.

Conceptually, this function is shorthand for a comparison of the
`count()` of the input
collection against the `count()`
of the `distinct()` of the input
collection:

``` fhirpath
X.count() = X.distinct().count()
```

This means that if the input collection is empty
(`{ }`), the result is true.

### Filtering and projection

#### where(criteria : expression) : collection 

Returns a collection containing only those elements in the input
collection for which the stated `criteria` expression evaluates to `true`. Elements for which the expression evaluates to
`false` or empty
(`{ }`) are not included in the
result.

If the input collection is empty (`{ }`), the result is empty.

If the result of evaluating the condition is other than a single boolean
value, the evaluation will end and signal an error to the calling
environment, consistent with singleton evaluation of collections
behavior.

The following example returns the list of `telecom` elements that have a `use` element with the value of
`'official'`:

``` fhirpath
Patient.telecom.where(use = 'official')
```

#### select(projection: expression) : collection 

Evaluates the `projection`
expression for each item in the input collection. The result of each
evaluation is added to the output collection. If the evaluation results
in a collection with multiple items, all items are added to the output
collection (collections resulting from evaluation of
`projection` are *flattened*).
This means that if the evaluation for an element results in the empty
collection (`{ }`), no element
is added to the result, and that if the input collection is empty
(`{ }`), the result is empty as
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
system of `phone` for all the
patients in the bundle.

``` fhirpath
Patient.name.where(use = 'usual').select(given.first() + ' ' + family)
```

This example returns a collection containing, for each \"usual\" name
for the Patient, the concatenation of the first given and family names.

#### repeat(projection: expression) : collection 

A version of `select` that will
repeat the `projection` and add
items to the output collection only if they are not already in the
output collection as determined by the [equals](#equals)
(`=`) operator.

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

Will repeat finding children called `contains`, until no new nodes are found.

``` fhirpath
Questionnaire.repeat(item)
```

Will repeat finding children called `item`, until no new nodes are found.

Note that this is slightly different from:

``` fhirpath
Questionnaire.descendants().select(item)
```

which would find *any* descendants called `item`, not just the ones nested inside other
`item` elements.

The order of items returned by the `repeat()` function is undefined.

#### ofType(type : *type specifier*) : collection 

Returns a collection that contains all items in the input collection
that are of the given type or a subclass thereof. If the input
collection is empty (`{ }`), the
result is empty. The `type`
argument is an identifier that must resolve to the name of a type in a
model. For implementations with compile-time typing, this requires
special-case handling when processing the argument to treat it as type
specifier rather than an identifier expression:

``` fhirpath
Bundle.entry.resource.ofType(Patient)
```

In the above example, the symbol `Patient` must be treated as a type identifier rather than a
reference to a Patient in context.

### Subsetting

#### \[ index : Integer \] : collection 

The indexer operation returns a collection with only the
`index`-th item (0-based index).
If the input collection is empty (`{ }`), or the index lies outside the boundaries of the
input collection, an empty collection is returned.

> **Note:** Unless specified otherwise by the underlying Object Model,
> the first item in a collection has index 0. Note that if the
> underlying model specifies that a collection is 1-based (the only
> reasonable alternative to 0-based collections), *any collections
> generated from operations on the 1-based list are 0-based*.

The following example returns the element in the
`name` collection of the Patient
with index 0:

``` fhirpath
Patient.name[0]
```

#### single() : collection 

Will return the single item in the input if there is just one item. If
the input collection is empty (`{ }`), the result is empty. If there are multiple items,
an error is signaled to the evaluation environment. This function is
useful for ensuring that an error is returned if an assumption about
cardinality is violated at run-time.

The following example returns the name of the Patient if there is one.
If there are no names, an empty collection, and if there are multiple
names, an error is signaled to the evaluation environment:

``` fhirpath
Patient.name.single()
```

#### first() : collection 

Returns a collection containing only the first item in the input
collection. This function is equivalent to `item[0]`, so it will return an empty collection if the input
collection has no items.

#### last() : collection 

Returns a collection containing only the last item in the input
collection. Will return an empty collection if the input collection has
no items.

#### tail() : collection 

Returns a collection containing all but the first item in the input
collection. Will return an empty collection if the input collection has
no items, or only one item.

#### skip(num : Integer) : collection 

Returns a collection containing all but the first
`num` items in the input
collection. Will return an empty collection if there are no items
remaining after the indicated number of items have been skipped, or if
the input collection is empty. If `num` is less than or equal to zero, the input collection
is simply returned.

#### take(num : Integer) : collection 

Returns a collection containing the first `num` items in the input collection, or less if there are
less than `num` items. If num is
less than or equal to 0, or if the input collection is empty
(`{ }`),
`take` returns an empty
collection.

#### intersect(other: collection) : collection 

Returns the set of elements that are in both collections. Duplicate
items will be eliminated by this function. Order of items is not
guaranteed to be preserved in the result of this function.

#### exclude(other: collection) : collection 

Returns the set of elements that are not in the
`other` collection. Duplicate
items will not be eliminated by this function, and order will be
preserved.

e.g. `(1 | 2 | 3).exclude(2)` .fhirpath returns `(1 | 3)`.

### Combining

#### []union(other : collection) 

Merge the two collections into a single collection, eliminating any
duplicate values (using [equals](#equals) (`=`) to determine equality). There is no expectation of
order in the resulting collection.

In other words, this function returns the distinct list of elements from
both inputs. For example, consider two lists of integers
`A: 1, 1, 2, 3` and
`B: 2, 3`:

``` fhirpath
A.union( B ) // 1, 2, 3
A.union( { } ) // 1, 2, 3
```

This function can also be invoked using the `|` operator.

e.g. `x.union(y)` .fhirpath .highlighter-rouge} is
synonymous with `x | y` .fhirpath

e.g. `name.select(use.union(given))` .fhirpath is the same as
`name.select(use | given)` .fhirpath, noting that the union function does not introduce
an iteration context, in this example the select introduces the
iteration context on the name property.

#### combine(other : collection) : collection 

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

[]

#### iif(criterion: expression, true-result: collection \[, otherwise-result: collection\]) : collection 

The `iif` function in FHIRPath
is an *immediate if*, also known as a conditional operator (such as C\'s
`? :` operator).

The `criterion` expression is
expected to evaluate to a Boolean.

If `criterion` is true, the
function returns the value of the `true-result` argument.

If `criterion` is
`false` or an empty collection,
the function returns `otherwise-result`, unless the optional
`otherwise-result` is not given,
in which case the function returns an empty collection.

Note that short-circuit behavior is expected in this function. In other
words, `true-result` should only
be evaluated if the `criterion`
evaluates to true, and `otherwise-result` should only be evaluated otherwise. For
implementations, this means delaying evaluation of the arguments.

#### Boolean Conversion Functions

##### toBoolean() : Boolean 

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
  **String**    `'true'`, `'t'`, `'yes'`, `'y'`, `'1'`, `'1.0'`   `true`
                `'false'`, `'f'`, `'no'`, `'n'`, `'0'`, `'0.0'`   `false`
  **Integer**   `1`                                                                                                                                                                                                                                                       `true`
                `0`                                                                                                                                                                                                                                                       `false`
  **Decimal**   `1.0`                                                                                                                                                                                                                                                     `true`
                `0.0`                                                                                                                                                                                                                                                     `false`

Note for the purposes of string representations, case is ignored (so
that both `'T'` and
`'t'` are considered
`true`).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToBoolean() : Boolean 

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

##### toInteger() : Integer 

If the input collection contains a single item, this function will
return a single integer if:

-   the item is an Integer
-   the item is a String and is convertible to an integer
-   the item is a Boolean, where `true`
    .highlighter-rouge} results in a 1 and `false`
    .highlighter-rouge} results in a 0.

If the item is not one the above types, the result is empty.

If the item is a String, but the string is not convertible to an integer
(using the regex format `(\+|-)?\d+`), the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToInteger() : Boolean 

If the input collection contains a single item, this function will
return true if:

-   the item is an Integer
-   the item is a String and is convertible to an Integer
-   the item is a Boolean

If the item is not one of the above types, or the item is a String, but
is not convertible to an Integer (using the regex format
`(\+|-)?\d+`), the result is
false.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### toLong() : Long 

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

If the input collection contains a single item, this function will
return a single integer if:

-   the item is an Integer or Long
-   the item is a String and is convertible to a 64 bit integer
-   the item is a Boolean, where `true`
    .highlighter-rouge} results in a 1 and `false`
    .highlighter-rouge} results in a 0.

If the item is not one the above types, the result is empty.

If the item is a String, but the string is not convertible to a 64 bit
integer (using the regex format `(\+|-)?\d+`), the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToLong() : Boolean 

If the input collection contains a single item, this function will
return true if:

-   the item is an Integer or Long
-   the item is a String and is convertible to a Long
-   the item is a Boolean

If the item is not one of the above types, or the item is a String, but
is not convertible to an Integer (using the regex format
`(\+|-)?\d+`), the result is
false.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

#### Date Conversion Functions

##### toDate() : Date 

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

##### convertsToDate() : Boolean 

If the input collection contains a single item, this function will
return true if:

-   the item is a Date
-   the item is a DateTime
-   the item is a String and is convertible to a Date

If the item is not one of the above types, or is not convertible to a
Date (using the format **YYYY-MM-DD**), the result is false.

If the item contains a partial date (e.g.
`'2012-01'`), the result is a
partial date.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

#### DateTime Conversion Functions

##### toDateTime() : DateTime 

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
`'2012-01-01T10:00'`), the
result is a partial datetime.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToDateTime() : Boolean 

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

##### toDecimal() : Decimal 

If the input collection contains a single item, this function will
return a single decimal if:

-   the item is an Integer or Decimal
-   the item is a String and is convertible to a Decimal
-   the item is a Boolean, where `true`
    .highlighter-rouge} results in a `1.0`
    .highlighter-rouge} and `false`
    .highlighter-rouge} results in a `0.0`
    .highlighter-rouge}.

If the item is not one of the above types, the result is empty.

If the item is a String, but the string is not convertible to a Decimal
(using the regex format `(\+|-)?\d+(\.\d+)?`), the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToDecimal() : Boolean 

If the input collection contains a single item, this function will true
if:

-   the item is an Integer or Decimal
-   the item is a String and is convertible to a Decimal
-   the item is a Boolean

If the item is not one of the above types, or is not convertible to a
Decimal (using the regex format `(\+|-)?\d+(\.\d+)?`), the result is false.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

#### Quantity Conversion Functions

##### toQuantity(\[unit : String\]) : Quantity 

If the input collection contains a single item, this function will
return a single quantity if:

-   the item is an Integer, or Decimal, where the resulting quantity
    will have the default unit (`'1'`
    .highlighter-rouge})
-   the item is a Quantity
-   the item is a String and is convertible to a Quantity
-   the item is a Boolean, where `true`
    .highlighter-rouge} results in the quantity
    `1.0 '1'`, and
    `false` results in the
    quantity `0.0 '1'`

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

If the `unit` argument is
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
  `1 year`     `12 months` or `365 days`
  `1 month`    `30 days`
  `1 day`      `24 hours`
  `1 hour`     `60 minutes`
  `1 minute`   `60 seconds`
  `1 second`   `1 's'`

Note that calendar duration conversion factors are only used when
time-valued quantities appear in unanchored calculations. See [Date/Time
Arithmetic](#datetime-arithmetic) for more information on using
time-valued quantities in FHIRPath.

If `q` is a Quantity of
`'kg'` and one wants to convert
to a Quantity in `'g'` (grams):

``` fhirpath
q.toQuantity('g') // changes the value and units in the quantity according to UCUM conversion rules
```

> Implementations are not required to support a complete UCUM
> implementation, and may return empty (`{ }`
> .highlighter-rouge}) when the `unit`
> .highlighter-rouge} argument is used and it is different than the
> input quantity unit.

##### convertsToQuantity(\[unit : String\]) : Boolean 

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

If the `unit` argument is
provided, it must be the string representation of a UCUM code (or a
FHIRPath calendar duration keyword), and is used to determine whether
the input quantity can be converted to the given unit, according to the
unit conversion rules specified by UCUM. If the input quantity can be
converted, the result is true, otherwise, the result is false.

> Implementations are not required to support a complete UCUM
> implementation, and may return false when the
> `unit` argument is used and it
> is different than the input quantity unit.

#### String Conversion Functions

##### toString() : String 

If the input collection contains a single item, this function will
return a single String if:

-   the item in the input collection is a String
-   the item in the input collection is an Integer, Decimal, Date, Time,
    DateTime, or Quantity the output will contain its String
    representation
-   the item is a Boolean, where `true`
    .highlighter-rouge} results in `'true'`
    .highlighter-rouge} and `false`
    .highlighter-rouge} in `'false'`
    .highlighter-rouge}.

If the item is not one of the above types, the result is false.

The String representation uses the following formats:

  Type           Representation
  -------------- ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **Boolean**    `true` or `false`
  **Integer**    `(\+|-)?\d+`
  **Decimal**    `(\+|-)?\d+(.\d+)?`
  **Quantity**   `(\+|-)?\d+(.\d+)? '.*'` e.g. `(4 days).toString()` .fhirpath .highlighter-rouge} returns `4 'd'` because the FHIRPath literal temporal units are short-hands for the UCUM equivalents.
  **Date**       **YYYY-MM-DD**
  **DateTime**   **YYYY-MM-DDThh:mm:ss.fff(+\|-)hh:mm**
  **Time**       **hh:mm:ss.fff(+\|-)hh:mm**

Note that for partial dates and times, the result will only be specified
to the level of precision in the value being converted.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToString() : String 

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

##### toTime() : Time 

If the input collection contains a single item, this function will
return a single time if:

-   the item is a Time
-   the item is a String and is convertible to a Time

If the item is not one of the above types, the result is empty.

If the item is a String, but the string is not convertible to a Time
(using the format **hh:mm:ss.fff(+\|-)hh:mm**), the result is empty.

If the item contains a partial time (e.g. `'10:00'`), the result is a partial time.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

If the input collection is empty, the result is empty.

##### convertsToTime() : Boolean 

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
use filters like `where()` and
`select()`:

``` fhirpath
Patient.name.given.select(substring(0))
```

This example returns a collection containing the first character of all
the given names for a patient.

#### indexOf(substring : String) : Integer 

Returns the 0-based index of the first position
`substring` is found in the
input string, or -1 if it is not found.

If `substring` is an empty
string (`''`), the function
returns 0.

If the input or `substring` is
empty (`{ }`), the result is
empty (`{ }`).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abcdefg'.indexOf('bc') // 1
'abcdefg'.indexOf('x') // -1
'abcdefg'.indexOf('abcdefg') // 0
```

#### lastIndexOf(substring : String) : Integer 

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

Returns the 0-based index of the last position
`substring` is found in the
input string, or -1 if it is not found.

If `substring` is an empty
string (`''`), the function
returns 0.

If the input or `substring` is
empty (`{ }`), the result is
empty (`{ }`).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
'abcdefg'.lastIndexOf('bc') // 1
'abcdefg'.lastIndexOf('x') // -1
'abcdefg'.lastIndexOf('abcdefg') // 0
'abc abc'.lastIndexOf('a') // 4
```

#### substring(start : Integer \[, length : Integer\]) : String 

Returns the part of the string starting at position
`start` (zero-based). If
`length` is given, will return
at most `length` number of
characters from the input string.

If `start` lies outside the
length of the string, the function returns empty
(`{ }`). If there are less
remaining characters in the string than indicated by
`length`, the function returns
just the remaining characters.

If the input or `start` is
empty, the result is empty.

If an empty `length` is
provided, the behavior is the same as if `length` had not been provided.

If a negative or zero `length`
is provided, the function returns an empty string
(`''`).

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

#### startsWith(prefix : String) : Boolean 

Returns `true` when the input
string starts with the given `prefix`.

If `prefix` is the empty string
(`''`), the result is
`true`.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abcdefg'.startsWith('abc') // true
'abcdefg'.startsWith('xyz') // false
```

#### endsWith(suffix : String) : Boolean 

Returns `true` when the input
string ends with the given `suffix`.

If `suffix` is the empty string
(`''`), the result is
`true`.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abcdefg'.endsWith('efg') // true
'abcdefg'.endsWith('abc') // false
```

#### contains(substring : String) : Boolean 

Returns `true` when the given
`substring` is a substring of
the input string.

If `substring` is the empty
string (`''`), the result is
`true`.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abc'.contains('b') // true
'abc'.contains('bc') // true
'abc'.contains('d') // false
```

> **Note:** The `.contains()`
> function described here is a string function that looks for a
> substring in a string. This is different than the
> `contains` operator, which is
> a list operator that looks for an element in a list.

#### upper() : String 

Returns the input string with all characters converted to upper case.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abcdefg'.upper() // 'ABCDEFG'
'AbCdefg'.upper() // 'ABCDEFG'
```

#### lower() : String 

Returns the input string with all characters converted to lower case.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'ABCDEFG'.lower() // 'abcdefg'
'aBcDEFG'.lower() // 'abcdefg'
```

#### replace(pattern : String, substitution : String) : String 

Returns the input string with all instances of
`pattern` replaced with
`substitution`. If the
substitution is the empty string (`''`), instances of `pattern` are removed from the result. If
`pattern` is the empty string
(`''`), every character in the
input string is surrounded by the substitution, e.g.
`'abc'.replace('','x')` .fhirpath becomes `'xaxbxcx'`.

If the input collection, `pattern`, or `substitution` are empty, the result is empty
(`{ }`).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abcdefg'.replace('cde', '123') // 'ab123fg'
'abcdefg'.replace('cde', '') // 'abfg'
'abc'.replace('', 'x') // 'xaxbxcx'
```

#### matches(regex : String) : Boolean 

Returns `true` when the value
matches the given regular expression. Regular expressions should
function consistently, regardless of any culture- and locale-specific
settings in the environment, should be case-sensitive, use \'single
line\' mode and allow Unicode characters. The start/end of line markers
`^`, `$` can be used to match the entire string.

If the input collection or `regex` are empty, the result is empty
(`{ }`).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'http://fhir.org/guides/cqf/common/Library/FHIR-ModelInfo|4.0.1'.matches('Library') // returns true
'N8000123123'.matches('^N[0-9]{8}$') // returns false as the string is not an 8 char number (it has 10)
'N8000123123'.matches('N[0-9]{8}') // returns true as the string has an 8 number sequence in it starting with `N`
```

#### matchesFull(regex : String) : Boolean 

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

Returns `true` when the value
completely matches the given regular expression (implying that the
start/end of line markers `^`,
`$` are always surrounding the
regex expression provided).

Regular expressions should function consistently, regardless of any
culture- and locale-specific settings in the environment, should be
case-sensitive, use \'single line\' mode and allow Unicode characters.

If the input collection or `regex` are empty, the result is empty
(`{ }`).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
'http://fhir.org/guides/cqf/common/Library/FHIR-ModelInfo|4.0.1'.matchesFull('Library') // returns false
'N8000123123'.matchesFull('N[0-9]{8}') // returns false as the string is not an 8 char number (it has 10)
'N8000123123'.matchesFull('N[0-9]{10}') // returns true as the string has an 10 number sequence in it starting with `N`
```

#### replaceMatches(regex : String, substitution: String) : String 

Matches the input using the regular expression in
`regex` and replaces each match
with the `substitution` string.
The substitution may refer to identified match groups in the regular
expression.

If the input collection, `regex`, or `substitution` are empty, the result is empty
(`{ }`).

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

This example of `replaceMatches()` will convert a string with a date formatted as
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

#### length() : Integer 

Returns the length of the input string. If the input collection is empty
(`{ }`), the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

#### toChars() : collection 

Returns the list of characters in the input string. If the input
collection is empty (`{ }`), the
result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` fhirpath
'abc'.toChars() // { 'a', 'b', 'c' }
```

### Additional String Functions

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

#### encode(format : String) : String 

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

#### decode(format : String) : String 

The decode function takes a singleton encoded string and returns the
result of decoding that string according to the given format. The format
parameter defines the encoding format. Available formats are listed in
the encode function.

If the input is empty, the result is empty.

If no format is specified, the result is empty.

#### escape(target : String) : String 

The escape function takes a singleton string and escapes it for a given
target, as specified in the following table:

  ------ ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  html   The string is escaped such that it can appear as valid HTML content (at least open bracket (`<`), ampersand (`&`), and quotes (`"`), but ideally anything with a character encoding above 127)
  json   The string is escaped such that it can appear as a valid JSON string (quotes (`"`) are escaped as (`\"`)); additional escape characters are described in the [String](#string) escape section
  ------ ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

If the input is empty, the result is empty.

If no target is specified, the result is empty.

#### unescape(target : String) : String 

The unescape function takes a singleton string and unescapes it for a
given target. The available targets are specified in the escape function
description.

If the input is empty, the result is empty.

If no target is specified, the result is empty.

#### trim() : String 

The trim function trims whitespace characters from the beginning and
ending of the input string, with whitespace characters as defined in the
[Whitespace](#whitespace) lexical category.

If the input is empty, the result is empty.

#### split(separator: String) : collection 

The split function splits a singleton input string into a list of
strings, using the given separator.

If the input is empty, the result is empty.

If the input string does not contain any appearances of the separator,
the result is the input string.

The following example illustrates the behavior of the
`.split` operator:

``` stu
('A,B,C').split(',') // { 'A', 'B', 'C' }
('ABC').split(',') // { 'ABC' }
'A,,C'.split(',') // { 'A', '', 'C' }
```

#### join(\[separator: String\]) : String 

The join function takes a collection of strings and *joins* them into a
single string, optionally using the given separator.

If the input is empty, the result is empty.

If no separator is specified, the strings are directly concatenated.

The following example illustrates the behavior of the
`.join` operator:

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

#### abs() : Integer \| Decimal \| Quantity 

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

#### ceiling() : Integer 

Returns the first integer greater than or equal to the input.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
1.ceiling() // 1
1.1.ceiling() // 2
(-1.1).ceiling() // -1
```

#### exp() : Decimal 

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

#### floor() : Integer 

Returns the first integer less than or equal to the input.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
1.floor() // 1
2.1.floor() // 2
(-2.1).floor() // -3
```

#### ln() : Decimal 

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

#### log(base : Decimal) : Decimal 

Returns the logarithm base `base` of the input number.

When used with Integers, the arguments will be implicitly converted to
Decimal.

If `base` is empty, the result
is empty.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
16.log(2) // 4.0
100.0.log(10.0) // 2.0
```

#### power(exponent : Integer \| Decimal) : Integer \| Decimal 

Raises a number to the `exponent` power. If this function is used with Integers, the
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

#### round(\[precision : Integer\]) : Decimal 

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

#### sqrt() : Decimal 

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

#### truncate() : Integer 

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

#### children() : collection 

Returns a collection with all immediate child nodes of all items in the
input collection. Note that the ordering of the children is undefined
and using functions like `first()` on the result may return different results on
different platforms.

#### descendants() : collection 

Returns a collection with all descendant nodes of all items in the input
collection. The result does not include the nodes in the input
collection themselves. This function is a shorthand for
`repeat(children())`. Note that
the ordering of the children is undefined and using functions like
`first()` on the result may
return different results on different platforms.

> **Note:** Many of these functions will result in a set of nodes of
> different underlying types. It may be necessary to use
> `ofType()` as described in the
> previous section to maintain type safety. See [Type safety and strict
> evaluation](#type-safety-and-strict-evaluation) for more information
> about type safe use of FHIRPath expressions.

### Utility functions

#### trace(name : String \[, projection: Expression\]) : collection 

Adds a String representation of the input collection to the diagnostic
log, using the `name` argument
as the name in the log. This log should be made available to the user in
some appropriate fashion. Does not change the input, so returns the
input collection as output.

If the `projection` argument is
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

##### now() : DateTime 

Returns the current date and time, including timezone offset.

##### timeOfDay() : Time 

Returns the current time.

##### today() : Date 

Returns the current date.

[]

#### defineVariable(name: String \[, expr: expression\]) 

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

Defines a variable named `name`
that is accessible in subsequent expressions and has the value of
`expr` if present, otherwise the
value of the input collection. In either case the function does not
change the input and the output is the same as the input collection.

If the name already exists in the current expression scope, the
evaluation will end and signal an error to the calling environment. Note
that functions that take an `expression` as an argument establish a scope for the iteration
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

#### lowBoundary(\[precision: Integer\]): Decimal \| Date \| DateTime \| Time 

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

#### highBoundary(\[precision: Integer\]): Decimal \| Date \| DateTime \| Time 

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

#### precision() : Integer 

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

#### Extract Date/DateTime/Time components 

##### yearOf(): Integer 

If the input collection contains a single Date or DateTime, this
function will return the year component.

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2014-01-05T10:30:00.000.yearOf() // 2014
```

##### monthOf(): Integer 

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

##### dayOf(): Integer 

If the input collection contains a single Date or DateTime, this
function will return the day component.

If the input collection is empty, or the day is not present in the
value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2014-01-05T10:30:00.000.dayOf() // 5
```

##### hourOf(): Integer 

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

##### minuteOf(): Integer 

If the input collection contains a single Date, DateTime or Time, this
function will return the minute component.

If the input collection is empty, or the minute is not present in the
value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:40.002-07:00.minuteOf() // 30
```

##### secondOf(): Integer 

If the input collection contains a single Date, DateTime or Time, this
function will return the second component.

If the input collection is empty, or the second is not present in the
value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:40.002-07:00.secondOf() // 40
```

##### millisecondOf(): Integer 

If the input collection contains a single Date, DateTime or Time, this
function will return the millisecond component.

If the input collection is empty, or the millisecond is not present in
the value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:00.002-07:00.millisecondOf() // 2
```

##### timezoneOffsetOf(): Decimal 

If the input collection contains a single DateTime, this function will
return the timezone offset component.

If the input collection is empty, or the timezone offset is not present
in the value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:00.000-07:00.timezoneOffsetOf() // -7.0
```

##### dateOf(): Date 

If the input collection contains a single Date or DateTime, this
function will return the date component (up to the precision present in
the input value).

If the input collection is empty, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:00.000-07:00.dateOf() // @2012-01-01
```

##### timeOf(): Time 

If the input collection contains a single DateTime, this function will
return the time component.

If the input collection is empty, or the time is not present in the
value, the result is empty.

If the input collection contains multiple items, the evaluation of the
expression will end and signal an error to the calling environment.

``` stu
@2012-01-01T12:30:00.000-07:00.timeOf() // @T12:30:00.000
```
## Lexical Elements

FHIRPath defines the following lexical elements:

  Element          Description
  ---------------- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **Whitespace**   Whitespace defines the separation between tokens in the language
  **Comment**      Comments are ignored by the language, allowing for descriptive text
  **Literal**      Literals allow basic values to be represented within the language
  **Symbol**       Symbols such as `+`, `-`, `*`, and `/`
  **Keyword**      Grammar-recognized tokens such as `and`, `or` and `in`
  **Identifier**   Labels such as type names and property names

### Whitespace

FHIRPath defines *tab* (`\t`),
*space* (``), *line feed*
(`\n`) and *carriage return*
(`\r`) as *whitespace*, meaning
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
  **Empty** (`{ }`)   The empty collection
  **[Boolean](#boolean)**                                     The boolean literals (`true` and `false`)
  **[Integer](#integer)**                                     Sequences of digits in the range 0..2^32^-1
  **[Decimal](#decimal)**                                     Sequences of digits with a decimal point, in the range (-10^28^+1)/10^8^..(10^28^-1)/10^8^
  **[String](#string)**                                       Strings of any character enclosed within single-ticks (`'`)
  **[Date](#date)**                                           The at-symbol (`@`) followed by a date (**YYYY-MM-DD**)
  **[DateTime](#datetime)**                                   The at-symbol (`@`) followed by a datetime (**YYYY-MM-DDThh:mm:ss.fff(+\|-)hh:mm**)
  **[Time](#time)**                                           The at-symbol (`@`) followed by a time (**Thh:mm:ss.fff(+\|-)hh:mm**)
  **[Quantity](#quantity)**                                   An integer or decimal literal followed by a datetime precision specifier, or a [\[UCUM\]](#UCUM) unit specifier

For a more detailed discussion of the semantics of each type, refer to
the link for each type.

### Symbols

Symbols provide structure to the language and allow symbolic invocation
of common operators such as addition. FHIRPath defines the following
symbols:

  Symbol                                                      Description
  ----------------------------------------------------------- -----------------------------------------------------------
  `()`                Parentheses for delimiting groups within expressions
  `[]`                Brackets for indexing into lists and strings
  `{}`                Braces for delimiting exclusively empty lists
  `.`                 Period for qualifiers, accessors, and dot-invocation
  `,`                 Comma for delimiting items in a syntactic list
  `= != \<= < > >=`   Comparison operators for comparing values
  `+ - * / \| &`      Arithmetic and other operators for performing computation

### Keywords

Keywords are tokens that are recognized by the parser and used to build
the various language constructs. FHIRPath defines the following
keywords:

  ---------------------------------------------------- ------------------------------------------------------- -------------------------------------------------------- --------------------------------------------------
  `$index`     `div`           `milliseconds`   `true`
  `$this`      `false`         `minute`         `week`
  `$total`     `hour`          `minutes`        `weeks`
  `and`        `hours`         `mod`            `xor`
  `as`         `implies`       `month`          `year`
  `contains`   `in`            `months`         `years`
  `day`        `is`            `or`             `second`
  `days`       `millisecond`   `seconds`         
  ---------------------------------------------------- ------------------------------------------------------- -------------------------------------------------------- --------------------------------------------------

In general, keywords within FHIRPath are also considered *reserved*
words, meaning that it is illegal to use them as identifiers. FHIRPath
keywords are reserved words, with the exception of the following
keywords that may also be used as identifiers:

  ---------------------------------------------- ----------------------------------------------------
  `as`   `contains`
  `is`    
  ---------------------------------------------- ----------------------------------------------------

If necessary, identifiers that clash with a reserved word can be
delimited using a backtick (`` ` ``):

``` fhirpath
Patient.text.`div`.empty()
```

The `div` element of the
`Patient.text` must be offset
with backticks (`` ` ``) because
`div` is both a keyword and a
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
backticks (`` ` ``):

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
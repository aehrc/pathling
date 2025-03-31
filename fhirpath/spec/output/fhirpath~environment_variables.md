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
simple identifier (as in `%ucum`), or a delimited identifier to allow for alternative
characters (as in `` %`us-zip` ``).

Note also that these tokens are not restricted to simple types, and they
may have values that are not defined fixed values known prior to
evaluation at run-time, though there is no way to define these kind of
values in implementation guides.

Attempting to access an undefined environment variable will result in an
error, but accessing a defined environment variable that does not have a
value specified results in empty (`{ }`).

> **Note:** For backwards compatibility with some existing
> implementations, the token for an environment variable may also be a
> string, as in `%'us-zip'`,
> with no difference in semantics.
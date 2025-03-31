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
`type()` is specified as an XML
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

[]
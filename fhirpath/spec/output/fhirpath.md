# FHIRPath

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
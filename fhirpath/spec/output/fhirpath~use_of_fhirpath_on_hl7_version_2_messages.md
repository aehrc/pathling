## Use of FHIRPath on HL7 Version 2 messages 

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
    escapes in the simple content (e.g. `\.b\`
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
returns the whole Cell - e.g. `|value^^MR|`, though often more components will be present)

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
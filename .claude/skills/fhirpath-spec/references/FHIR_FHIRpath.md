This is the Continuous Integration Build of FHIR (will be incorrect/inconsistent at times).  
See the [Directory of published versions ![icon](https://build.fhir.org/external.png)](http://hl7.org/fhir/directory.html)

## 2.1.9 FHIRPath[](https://build.fhir.org/fhirpath.html#2.1.9 "link to here")

The FHIR Specification uses [FHIRPath (release 2) ![icon](https://build.fhir.org/external.png)](http://hl7.org/fhirpath/r2) for path-based navigation and extraction. FHIRPath is a separate specification published at [http://hl7.org/fhirpath ![icon](https://build.fhir.org/external.png)](http://hl7.org/fhirpath/r2) in order to support wider re-use across multiple specifications.

FHIRPath is used in several places in the FHIR and related specifications:

+   [invariants in ElementDefinition](https://build.fhir.org/elementdefinition-definitions.html#ElementDefinition.constraint.expression) - used to apply co-occurrence and other rules to the contents (e.g. value.empty() or code!=component.code)
+   [slicing discriminator](https://build.fhir.org/elementdefinition-definitions.html#ElementDefinition.slicing.discriminator.path) - used to indicate what element(s) define uniqueness (e.g. Observation.category)
+   [search parameter paths](https://build.fhir.org/searchparameter-definitions.html#SearchParameter.expression) - used to define what contents the parameter refers to (e.g. Observation.dataAbsentReason)
+   [error message locations in OperationOutcome](https://build.fhir.org/operationoutcome-definitions.html#OperationOutcome.issue.expression)
+   [FHIRPath-based Patch](https://build.fhir.org/fhirpatch.html)
+   [Invariants in the TestScript resource ![icon](https://build.fhir.org/external.png)](https://build.fhir.org/ig/hl7/fhir-testing-ig/index.html/StructureDefinitions-TestScript-definitions.html#TestScript.setup.action.assert.expression)

In addition, FHIRPath is used in [pre-fetch templates in Smart on FHIR's CDS-Hooks ![icon](https://build.fhir.org/external.png)](http://cds-hooks.hl7.org/ballots/2018May/specification/1.0/#prefetch-template) .

### 2.1.9.1 Using FHIRPath with Resources[](https://build.fhir.org/fhirpath.html#rules "link to here")

In FHIRPath, like XPath, operations are expressed in terms of the logical content of hierarchical data models, and support traversal, selection and filtering of data.

FHIRPath uses a tree model that abstracts away the actual underlying data model of the data being queried. For FHIR, this means that the contents of the resources and data types as described in the Logical views (or the UML diagrams) are used as the model, rather than the JSON and XML formats, so specific xml or json features are not visible to the FHIRPath language (such as comments and the split representation of primitives).

More specifically:

#### 2.1.9.1.1 Polymorphism in FHIR[](https://build.fhir.org/fhirpath.html#polymorphism "link to here")

For [choice elements](https://build.fhir.org/formats.html#choice), where elements can be one of multiple types, e.g. `Patient.deceased[x]`. In actual instances these will be present as either `Patient.deceasedBoolean` or `Patient.deceasedDateTime`. In FHIRPath, choice elements are labeled according to the name without the '\[x\]' suffix, and children can be explicitly treated as a specific type using the `as` operation:

```
(Observation.value.ofType(Quantity)).unit
```

FHIRPath statements can start with a full resource name:

```
Patient.name.given
```

The name can also include super types such as DomainResource:

```
DomainResource.contained(id = 23).exists()
```

These statements apply to any resource that specializes [DomainResource](https://build.fhir.org/domainresource.html).

#### 2.1.9.1.2 Using FHIR types in expressions[](https://build.fhir.org/fhirpath.html#types "link to here")

The namespace for the types defined in FHIR (primitive datatypes, datatypes, resources) is FHIR. So, for example:

```
Patient.is(FHIR.Patient)
```

The first element - the type name - is not namespaced, but the parameter to the is() operation is.

Understanding the primitive types is critical: FHIR.string is a different type to System.String. The FHIR.string type specializes FHIR.Element, and has the properties id, extension, and also the implicit value property that is actually of type of System.String.

The evaluation engine will automatically convert the value of FHIR types representing primitives to FHIRPath types when they are used in expressions according to the following mapping:

| FHIR primitive type | FHIRPath type |
| --- | --- |
| FHIR.boolean | System.Boolean |
| FHIR.string, FHIR.uri, FHIR.code, FHIR.oid, FHIR.id, FHIR.uuid, FHIR.markdown, FHIR.base64Binary | System.String |
| FHIR.integer, FHIR.unsignedInt, FHIR.positiveInt | System.Integer |
| FHIR.integer64 | System.Long |
| FHIR.decimal | System.Decimal |
| FHIR.date, FHIR.dateTime, FHIR.instant | System.DateTime |
| FHIR.time | System.Time |
| FHIR.Quantity | System.Quantity (see below) |

Since FHIR primitives may contain extensions, the following expressions are *not* mutually exclusive:

```
Patient.name.given = 'Ewout'         // value of Patient.name.given as a string
Patient.name.given.extension.first().value = true   // extension of the primitive value
```

The automatic conversion means that in most respects, a FHIR primitive can generally be treated as if it was the equivalent FHIRPath system type. The primary exception is the is() operation, where the difference is explicit:

```
Patient.name.given.is(FHIR.string);
Patient.name.given.is(System.string).not();
Patient.name.given.getValue().is(System.string);
```

As shown, all FHIR primitives have the operation `getValue()` defined (see below) for the few edge cases where the automatic conversion isn't appropriate. Note that ofType() does not have such restrictions - both of the following are valid:

```
Patient.name.given.ofType(FHIR.string);
Patient.name.given.ofType(System.string);
```

#### 2.1.9.1.3 Use of FHIR Quantity[](https://build.fhir.org/fhirpath.html#quantity "link to here")

The Mapping from FHIR Quantity to FHIRPath System.Quantity can only be applied if the FHIR Quantity has a UCUM code - i.e. a system of `http://unitsofmeasure.org`, and a code is present.

As part of the mapping, time-valued UCUM units are mapped to the [calendar duration units ![icon](https://build.fhir.org/external.png)](http://hl7.org/fhirpath/R2/index.html#time-valued-quantities) defined in FHIRPath, according to the following map:

<table class="grid"><tbody><tr><td>a</td><td>year</td></tr><tr><td>mo</td><td>month</td></tr><tr><td>d</td><td>day</td></tr><tr><td>h</td><td>hour</td></tr><tr><td>min</td><td>minute</td></tr><tr><td>s</td><td>second</td></tr></tbody></table>

![](https://build.fhir.org/assets/images/dragon.png "Here Be Dragons!")

i.e. The FHIR Quantity 1 'a' would be implicitly converted to the FHIRPath System.Quantity 1 'year'. Note that there's a subtle difference between the UCUM definitions for `a` and `mo`, which are definition durations of `365.25` and `30` days respectively, while `year` and `month` are calendar based durations, and their length of time varies. See [Time-valued Quantities ![icon](https://build.fhir.org/external.png)](http://hl7.org/fhirpath/N1/#time-valued-quantities) for further discussion. Implementers should be aware of these subtle differences, but in general, this approach matches what users expect most closely.

#### 2.1.9.1.4 FHIR Specific Variables[](https://build.fhir.org/fhirpath.html#variables "link to here")

FHIR defines two specific variables that are always in scope when FHIRPath is used in any of the contexts above:

```
%resource // the resource that contains the original node that is in %context
%rootResource // the container resource for the resource identified by %resource
```

The resource is very often the context, such that %resource = %context. When a DomainResource contains another resource, and that contained resource is the focus (%resource) then %rootResource refers to the container resource. Note that in most cases, the resource is not contained by another resource, and then %rootResource is the same as %resource.

In addition to the general purpose variables above, additional variables and/or guidance about variable use are defined when working with specific resources. Refer to the following for additional FHIRPath guidance defined for:

#### 2.1.9.1.5 Additional functions[](https://build.fhir.org/fhirpath.html#functions "link to here")

FHIR adds (compatible) functionality to the set of common FHIRPath functions. Some of these functions are candidates for elevation to the base version of FHIRPath when the next version is released.

In addition to the general purpose functions below, additional functions function use are defined when working with specific resources. Refer to the following for additional FHIRPath guidance defined for:

**extension(url : string) : collection**

Will filter the input collection for items named "extension" with the given url. This is a syntactical shortcut for `.extension.where(url = string)`, but is simpler to write. Will return an empty collection if the input collection is empty or the url is empty.

* * *

**hasValue() : Boolean**

Returns true if the input collection contains a single value which is a FHIR primitive, and it has a primitive value (e.g. as opposed to not having a value and just having extensions). Otherwise, the return value is empty.

> **Note to implementers**: The FHIR conceptual model talks about "primitives" as subclasses of the type Element that also have id and extensions. What this actually means is that a FHIR primitive is not a primitive in an implementation language. The introduction (section 2 above) describes the navigation tree as if the FHIR model applies - primitives are both primitives and elements with children.
>
> In FHIRPath, this means that FHIR primitives have a `value` child, but, as described above, they are automatically cast to FHIRPath primitives when comparisons are made, and that the primitive value will be included in the set returned by `children()` or `descendants()`.

* * *

**getValue() : System.\[type\]**

Return the underlying system value for the FHIR primitive if the input collection contains a single value which is a FHIR primitive, and it has a primitive value (see discussion for hasValue()). Otherwise the return value is empty.

* * *

**resolve() : collection**

For each item in the collection, if it is a string that is a [uri](https://build.fhir.org/datatypes.html#uri) (or [canonical](https://build.fhir.org/datatypes.html#canonical) or [url](https://build.fhir.org/datatypes.html#url)), locate the target of the reference, and add it to the resulting collection. If the item does not resolve to a resource, the item is ignored and nothing is added to the output collection.

The items in the collection may also represent a Reference, in which case the `Reference.reference` is resolved. If the input is empty, the output will be empty.

* * *

**ofType(type : identifier) : collection**

An alias for ofType() maintained purely for backwards compatibility.

* * *

**ofType(type : identifier) : collection**

Returns a collection that contains all items in the input collection that are of the given type or a subclass thereof. This works the same as in the base FHIRPath specification, but implementers should be aware that in FHIR, only concrete core types are allowed as an argument. All primitives are considered to be independent types (so `markdown` is **not** a subclass of `string`). Profiled types are not allowed, so to select `SimpleQuantity` one would pass `Quantity` as an argument.

* * *

**elementDefinition() : collection**

Returns the FHIR element definition information for each element in the input collection. If the input collection is empty, the return value will be empty.

* * *

**slice(structure : string, name : string) : collection**

Returns the given slice as defined in the given structure definition. The structure argument is a uri that resolves to the structure definition, and the name must be the name of a slice within that structure definition. If the structure cannot be resolved, or the name of the slice within the resolved structure is not present, or those parameters are empty, and empty value is returned.

For every element in the input collection, if the resolved slice is present on the element, it will be returned. If the slice does not match any element in the input collection, or if the input collection is empty, the result is an empty collection (`{ }`).

* * *

**checkModifiers(modifier : string) : collection**

For each element in the input collection, verifies that there are no modifying extensions defined other than the ones given by the `modifier` argument (comma-separated string). If the check passes, the input collection is returned. Otherwise, an error is thrown, including if modifier is empty.

* * *

**conformsTo(structure : string) : Boolean**

Returns `true` if the single input element conforms to the profile specified by the `structure` argument, and false otherwise. If the input is not a single item, the structure is empty, or the structure cannot be resolved to a valid profile, the result is empty.

* * *

**memberOf(valueset : string) : Boolean**

When invoked on a single code-valued element, returns true if the code is a member of the given valueset. When invoked on a single concept-valued element, returns true if any code in the concept is a member of the given valueset. When invoked on a single string, returns true if the string is equal to a code in the valueset, so long as the valueset only contains one codesystem. If the valueset in this case contains more than one codesystem, the return value is empty.

If the valueset cannot be resolved as a uri to a value set, or the input is empty or has more than one value, the return value is empty.

Note that implementations are encouraged to make use of a terminology service to provide this functionality.

For example:

```
Observation.component.where(code.memberOf('http://hl7.org/fhir/ValueSet/observation-vitalsignresult'))
```

This expression returns components that have a code that is a member of the observation-vitalsignresult valueset.

* * *

**subsumes(code : Coding | CodeableConcept) : Boolean**

When invoked on a Coding-valued element and the given code is Coding-valued, returns true if the source code is equivalent to the given code, or if the source code subsumes the given code (i.e. the source code is an ancestor of the given code in a subsumption hierarchy), and false otherwise.

If the Codings are from different code systems, the relationships between the code systems must be well-defined or the return value is an empty value.

When the source or given elements are CodeableConcepts, returns true if any Coding in the source or given elements is equivalent to or subsumes the given code.

If either the input or the code parameter are not single value collections, the return value is empty.

Note that implementations are encouraged to make use of a terminology service to provide this functionality.

* * *

**subsumedBy(code: Coding | CodeableConcept) : Boolean**

When invoked on a Coding-valued element and the given code is Coding-valued, returns true if the source code is equivalent to the given code, or if the source code is subsumed by the given code (i.e. the given code is an ancestor of the source code in a subsumption hierarchy), and false otherwise.

If the Codings are from different code systems, the relationships between the code systems must be well-defined or a run-time error is thrown.

When the source or given elements are CodeableConcepts, returns true if any Coding in the source or given elements is equivalent to or subsumed by the given code.

If either the input or the code parameter are not single value collections, the return value is empty.

Note that implementations are encouraged to make use of a terminology service to provide this functionality.

* * *

**htmlChecks : Boolean**

When invoked on a single [xhtml](https://build.fhir.org/narrative.html#xhtml) element returns true if the [rules around HTML usage](https://build.fhir.org/narrative.html#rules) are met, and false if they are not. The return value is empty on any other kind of element, or a collection of xhtml elements.

* * *

**lowBoundary : T**

This function returns the lowest possible value in the natural range expressed by the type it is invoked on. E.g. the lowBoundary of `1.0` is `0.95000000000`, and the lowBoundary of `2010-10-10` is `2010-10-10T00:00:00.000+14:00`. This function can be invoked in any singleton primitive type that has a value domain with a natural sort order: `decimal`, `integer`, `dateTime`, `instant`, `date`, `time` and `Quantity`. This function is defined for use with continuously distributed value domains to help deal with precision issues. The return value is considered to have arbitrarily high precision (as precise as the underlying implementation can be). The function is not very useful on integer, since it is not a continuously distributed value domain, and the lowBoundary of an integer is always the same value, but it is defined on integer for language consistency.

This function is intended to be added to the core FHIRPath specification in a future version.

* * *

**highBoundary : T**

This function returns the lowest possible value in the natural range expressed by the type it is invoked on. E.g. the highBoundary of `1.0` is `1.05000000000`, and the highBoundary of `2010-10-10` is `2010-10-10T23:59:59.999-12:00`. This function can be invoked in any singleton primitive type that has a value domain with a natural sort order: `decimal`, `integer`, `dateTime`, `instant`, `date`, `time` and `Quantity`. This function is defined for use with continuously distributed value domains to help deal with precision issues. The return value is considered to have arbitrarily high precision (as precise as the underlying implementation can be). The function is not very useful on integer, since it is not a continuously distributed value domain, and the highBoundary of an integer is always the same value, but it is defined on integer for language consistency.

This function is intended to be added to the core FHIRPath specification in a future version.

* * *

**comparable(quantity) : boolean**

This function returns true if the engine executing the FHIRPath statement can compare the singleton Quantity with the singleton other Quantity and determine their relationship to each other. Comparable means that both have values and that the code and system for the units are the same (irrespective of system) or both have code + system, system is recognized by the FHIRPath implementation and the codes are comparable within that code system. E.g. days and hours or inches and cm.

This function is intended to be added to the core FHIRPath specification in a future version.

* * *

#### 2.1.9.1.6 Changes to operators[](https://build.fhir.org/fhirpath.html#changes "link to here")

**~ (Equivalence)**

Equivalence works in exactly the same manner, but with the addition that for complex types, equality requires all child properties to be equal, **except for "id" elements**.

In addition, for Coding values, equivalence is defined based on the code and system elements only. The version, display, and userSelected elements are ignored for the purposes of determining Coding equivalence.

For CodeableConcept values, equivalence is defined as a non-empty intersection of Coding elements, using equivalence. In other words, two CodeableConcepts are considered equivalent if any Coding in one is equivalent to any Coding in the other.

#### 2.1.9.1.7 Environment variables[](https://build.fhir.org/fhirpath.html#vars "link to here")

The FHIR specification adds support for additional environment variables:

The following environmental values are set for all contexts:

```
%sct        // (string) url for snomed ct
%loinc      // (string) url for loinc
%`vs-[name]` // (string) full url for the provided HL7 value set with id [name]
%`ext-[name]` // (string) full url for the provided HL7 extension with id [name]
%resource	// The original resource current context is part of. When evaluating a datatype, this would be the resource the element is part of. Do not go past a root resource into a bundle, if it is contained in a bundle.

// Note that the names of the `vs-` and `ext-` constants are quoted (just like paths) to allow "-" in the name.
```

For example:

```
Observation.component.where(code.memberOf(%`vs-observation-vitalsignresult`))
```

This expression returns components that have a code that is a member of the observation-vitalsignresult valueset.

> **Implementation Note:** Implementation Guides are allowed to define their own externals, and implementers should provide some appropriate configuration framework to allow these constants to be provided to the evaluation engine at run-time. E.g.:
>
> ```
> %`us-zip` = '[0-9]{5}(-[0-9]{4}){0,1}'
> ```

Authors of Implementation Guides should be aware that adding specific environment variables restricts the use of the FHIRPath to their particular context.

Note that these tokens are not restricted to simple types, and they may have fixed values that are not known before evaluation at run-time, though there is no way to define these kinds of values in implementation guides.

### 2.1.9.2 Restricted Subset ("Simple")[](https://build.fhir.org/fhirpath.html#simple "link to here")

This page documents a restricted subset of the [FHIRPath language ![icon](https://build.fhir.org/external.png)](http://hl7.org/fhirpath) that is used in a few contexts in this specification. When the restricted FHIRPath language subset is in use, the following rules apply:

These rules exist to keep processing the path simple to support use of the path by processors that are not backed by a full FHIRPath implementation.

The following locations use this restricted FHIRPath language:

Unlike this rest of this page, the Factory API, the FHIR Terminology service API and the general server API (see below) are only draft (Maturity = 0). They will be advanced to a more mature status following the usual [Maturity Model](https://build.fhir.org/versions.html#maturity) for FHIR.

### 2.1.9.3 Type Factory[](https://build.fhir.org/fhirpath.html#factory "link to here")

The variable %factory is a reference to a class factory that provides the following type methods. Note that a future version of FHIRPath may provide a factory framework directly, in which case this factory API may be withdrawn or deprecated.

This API provides specific methods for constructing common types, and some general methods for constructing any type.

For the specific type constructors, all the parameters are optional. Note that since all variables / outputs in FHIRPath are collections, all the parameters are inherently collections, but when the underlying element referred to is a singleton element, the collection cannot contain more than one item. Use the value `{}` if there is no value to provide.

**primitives**

```
%factory.{primitive}(value, extensions) : {primitive}
```

Create an instance of the type with the value and possibly one or more extensions. e.g. `%factory.code('final')`.

Parameters:

**Return Value:** the primitive type, or an error.

```
%factory.Extension(url, value) : Extension
```

Creates an extension with the given url and value: `%factory.extension('http://hl7.org/fhir/StructureDefinition/artifact-copyrightLabel', 'CC0-1.0')`.

Parameters:

**Return Value:** An extension with the specified properties.

**Identifier**

```
%factory.Identifier{system, value, use, type) : Identifier
```

Creates an identifier with the given properties: `%factory.Identifier('urn:ietf:rfc:3986', 'urn:oid:1.2.3.4.5', 'official')`.

Parameters:

**Return Value:** An identifier with the specified properties .

**HumanName**

```
%factory.HumanName(family, given, prefix, suffix, text, use) : HumanName
```

Create a human name with the given properties: `%factory.HumanName('Smith', 'Julia', {}, {}, 'Julia Smith')`.

Parameters:

**Return Value:** a HumanName.

**ContactPoint**

```
%factory.ContactPoint(system, value, use) : ContactPoint
```

Creates a ContactPoint: `%factory.ContactPoint('email', 'coyote@acme.com', 'work')`

Parameters:

**Return Value:** a ContactPoint.

**Address**

```
%factory.Address(line, city, state, postalCode, country, use, type) : Address
```

Creates an Address: `%factory.Address('5 Nowhere Road', 'coyote@acme.com', 'EW', '0000', {}, 'home', 'physical')`

Parameters:

**Return Value:** An address.

**Quantity**

```
%factory.Quantity(system, code, value, unit) : Quantity
```

Creates a Quantity: `%factory.Quantity('http://unitsofmeasure.org', 'mg/dL', '5.03', 'mg/dL')`

Parameters:

**Return Value:** a Quantity.

**Coding**

```
%factory.Coding(system, code, display, version) : Coding
```

Creates a Coding: `%factory.Coding('http://loinc.org', '1234-5, 'An example test', '1.02')`

Parameters:

**Return Value:** A coding.

**CodeableConcept**

```
%factory.CodeableConcept(value, extensions) : 
```

Creates a CodeableConcept: `%factory.CodeableConcept(%factory.Coding(...), "Example Test")`

Parameters:

**Return Value:** a CodeableConcept.

For the general type constructors, all the parameters are mandatory. Note that since all variables / outputs in FHIRPath are collections, all the parameters are inherently collections, but when the underlying property referred to is a singleton element, the collection cannot contain more than one item. Use the value `{}` if there is no value to provide.

```
  %factory.create(type) : {type} 
```

Create an instance of the named type: `%factory.create(SampledData)`

Parameters:

**Return Value:** an instance of the named type.

**withExtension**

```
%factory.withExtension(instance, url, value) : 
```

Add an extension, and return the new type: `%factory.withExtension(%factory.create(Money), 'http:/acme.com/extension/example', %factory.code('test'))`

Parameters:

**Return Value:** A copy of the instance of the type with the extension added. Extensions that already exist with the same url are not removed.

**withProperty**

```
%factory.withProperty(instance, name, value) : T
```

Set a property value, and return the new type: `%factory.withProperty(%factory.create(Address), 'http:/acme.com/extension/example', %factory.create(Period))`

Parameters:

**Return Value:** A copy of the instance of the type with the named property set. Any existing value(s) for the named property will be deleted.

### 2.1.9.4 Terminology Service API[](https://build.fhir.org/fhirpath.html#txapi "link to here")

In order to support terminological reasoning in FHIRPath statements, FHIR defines a general %terminologies object that FHIRPath implementations should make available. Calls to this object are passed through a [standard FHIR terminology service](https://build.fhir.org/terminology-service.html).

Summary:

```
%terminologies.expand(valueSet, params) : ValueSet
%terminologies.lookup(coded, params) : Parameters
%terminologies.validateVS(valueSet, coded, params) : Parameters
%terminologies.validateCS(codeSystem, coded, params) : Parameters
%terminologies.subsumes(system, coded1, coded2, params) : code
%terminologies.translate(conceptMap, code, params) : Parameters
```

All these functions return an empty value if any of the parameters are empty, or a collection with more than one value, or one or more of the parameters are not valid.

**expand**

```
%terminologes.expand(valueSet, params) : ValueSet
```

This calls the [Terminology Service $expand](https://build.fhir.org/terminology-service.html#expand) operation ([formal definition](https://build.fhir.org/valueset-operation-expand.html)).

Parameters:

**Return Value:** a [ValueSet](https://build.fhir.org/valueset.html) with an expansion, or an empty value if an error occurs.

**lookup**

```
%terminologies.lookup(coded, params) : Parameters
```

This calls the [Terminology Service $lookup](https://build.fhir.org/terminology-service.html#lookup) operation ([formal definition](https://build.fhir.org/codesystem-operation-lookup.html)).

Parameters:

**Return Value:**

**validateVS**

```
%terminologies.validateVS(valueSet, coded, params) : Parameters
```

This calls the [Terminology Service $validate-code](https://build.fhir.org/terminology-service.html#expand) operation on a value set ([formal definition](https://build.fhir.org/valueset-operation-validate-code.html)).

Parameters:

**Return Value:** A [Parameters](https://build.fhir.org/parameters.html) resource with the results of the validation operation.

**validateCS**

```
%terminologies.validateCS(codeSystem, coded, params) : Parameters
```

This calls the [Terminology Service $validate-code](https://build.fhir.org/terminology-service.html#expand) operation on a code system ([formal definition](https://build.fhir.org/codesystem-operation-validate-code.html)).

Parameters:

**Return Value:** A [Parameters](https://build.fhir.org/parameters.html) resource with the results of the validation operation.

**subsumes**

```
%terminologies.subsumes(system, coded1, coded2, params) : code
```

This calls the [Terminology Service $subsumes](https://build.fhir.org/terminology-service.html#subsumes) operation ([formal definition](https://build.fhir.org/codesystem-operation-subsumes.html)).

Parameters:

**Return Value:** a code as specified for the subsumes operation.

**translate**

```
%terminologies.translate(conceptMap, coded, params) : Parameters
```

This calls the [Terminology Service $translate](https://build.fhir.org/terminology-service.html#translate) operation ([formal definition](https://build.fhir.org/conceptmap-operation-translate.html)).

Parameters:

**Return Value:** A [Parameters](https://build.fhir.org/parameters.html) resource with the results of the translation operation.

### 2.1.9.5 General Service API[](https://build.fhir.org/fhirpath.html#srvr-api "link to here")

In order to support interaction with a server in FHIRPath statements, FHIR defines a general %server object that FHIRPath implementations should make available. Calls to this object are passed through a [FHIR RESTful framework](https://build.fhir.org/http.html).

Summary:

```
%server : Server // default server (application controls context)
%server.at(url) : Server // server at specified address

%server.read(type, id) : Resource
%server.create(resource) : Resource
%server.update(resource) : Resource
%server.delete(resource) : boolean
%server.patch(parameters) : Resource
%server.search(doPost, parameters) : Bundle
%server.capabilities(mode) : Resource
%server.validate(resource, mode, parameters) : OperationOutcome
%server.transform(source, content) : Resource
%server.everything(type, id, parameters) : Bundle
%server.apply(resource, subject, parameters) : Bundle
```

```
  %terminologies.at(url) : Server
```

Get a server object pointing at a particular server. Note: The %server object points to the default server as specified by the application evaluating the FHIRPath.

Parameters:

**Return Value:** A server that points at the specified URL. No errors - they will come when/if the server object is used.

```
  %server.read(type, id) : Resource
```

Get a resource from the server.

Parameters:

**Return Value:** The resource at type/id, or null.

```
  %server.create(resource) : Resource
```

Create a resource on the server.

Parameters:

**Return Value:** The resource after it was stored, or null if the create operation failed.

```
  %server.update(resource) : Resource
```

Store a resource on the server.

Parameters:

**Return Value:** The resource after it was stored, or null if the create operation failed.

```
  %server.delete(resource) : boolean
```

Delete a resource on the server.

Parameters:

**Return Value:** true if the resource was deleted, or false.

```
  %server.search(doPost, parameters) : Bundle
```

Perform a search on the server.

Parameters:

**Return Value:** A bundle with the search results, or null.

```
  %server.patch(parameters) : Resource
```

Perform a patch operation on the server.

Parameters:

**Return Value:** The resource after the patch, or null.

```
  %server.capabilities(mode) : Resource
```

Get the capabilities from the server

Parameters:

**Return Value:** The resource returned (CapabilitiesStatement or TerminologyCapabilities resource), or null.

```
  %server.validate(resource, mode, parameters) : OperationOutcome
```

Validate a resource on the server.

Parameters:

**Return Value:** An operation outcome with issues, or null if the validation couldn't be performed.

```
  %server.transform(source, content) : Resource
```

Run the $transform operation on the server.

Parameters:

**Return Value:** The resource returned from the transform, or null.

```
  %server.everything(type, id, parameters) : Bundle
```

Get a resource from the server.

Parameters:

**Return Value:** The Bundle for type/id, or null.

```
  %server.apply(resource, subject, parameters) : Bundle
```

Get a resource from the server.

Parameters:

**Return Value:** The bundle from $apply, or null.
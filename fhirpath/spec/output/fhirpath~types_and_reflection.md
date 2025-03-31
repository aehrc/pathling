## Types and Reflection

### Models

Because FHIRPath is defined to work in multiple contexts, each context
provides the definition for the structures available in that context.
These structures are the *model* available for FHIRPath expressions. For
example, within FHIR, the FHIR data types and resources are the model.
To prevent namespace clashes, the type names within each model are
prefixed (or namespaced) with the name of the model. For example, the
fully qualified name of the Patient resource in FHIR is
`FHIR.Patient`. The system types
defined within FHIRPath directly are prefixed with the namespace
`System`.

To allow type names to be referenced in expressions such as the
`is` and
`as` operators, the language
includes a *type specifier*, an optionally qualified identifier that
must resolve to the name of a model type.

When resolving a type name, the context-specific model is searched
first. If no match is found, the `System` model (containing only the built-in types defined in
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
`type()` function returns the
type information for each element of the input collection, using one of
the following concrete subtypes of `TypeInfo`:

#### Primitive Types 

For primitive types such as `String` and `Integer`, the result is a
`SimpleTypeInfo`:

``` highlight
SimpleTypeInfo { namespace: string, name: string, baseType: TypeSpecifier }
```

For example:

``` stu
('John' | 'Mary').type()
```

Results in:

``` highlight
{
  SimpleTypeInfo { namespace: 'System', name: 'String', baseType: 'System.Any' },
  SimpleTypeInfo { namespace: 'System', name: 'String', baseType: 'System.Any' }
}
```

#### Class Types 

For class types, the result is a `ClassInfo`:

``` highlight
ClassInfoElement { name: string, type: TypeSpecifier, isOneBased: Boolean }
ClassInfo { namespace: string, name: string, baseType: TypeSpecifier, element: List<ClassInfoElement> }
```

For example:

``` stu
Patient.maritalStatus.type()
```

Results in:

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

#### Collection Types 

For collection types, the result is a `ListTypeInfo`:

``` highlight
ListTypeInfo { elementType: TypeSpecifier }
```

For example:

``` stu
Patient.address.type()
```

Results in:

``` highlight
{
  ListTypeInfo { elementType: 'FHIR.Address' }
}
```

#### Anonymous Types 

Anonymous types are structured types that have no associated name, only
the elements of the structure. For example, in FHIR, the
`Patient.contact` element has
multiple sub-elements, but is not explicitly named. For types such as
this, the result is a `TupleTypeInfo`:

``` highlight
TupleTypeInfoElement { name: string, type: TypeSpecifier, isOneBased: Boolean }
TupleTypeInfo { element: List<TupleTypeInfoElement> }
```

For example:

``` stu
Patient.contact.single().type()
```

Results in:

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

> **Note:** These structures are a subset of the abstract metamodel used
> by the [Clinical Quality Language
> Tooling](https://github.com/cqframework/clinical_quality_language).
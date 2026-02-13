# Search Operations Reference

## Table of Contents

1. [Basic Search](#basic-search)
2. [String Parameters](#string-parameters)
3. [Token Parameters](#token-parameters)
4. [Date Parameters](#date-parameters)
5. [Reference Parameters](#reference-parameters)
6. [Quantity Parameters](#quantity-parameters)
7. [Composite Parameters](#composite-parameters)
8. [Multi-Value Parameters](#multi-value-parameters)
9. [Include and RevInclude](#include-and-revinclude)
10. [Sort Parameters](#sort-parameters)
11. [Paging](#paging)
12. [Named Queries](#named-queries)
13. [Compartment Searches](#compartment-searches)
14. [Custom Search Parameters](#custom-search-parameters)

## Basic Search

```java
@Search
public List<Patient> searchAll() {
    return patientDao.findAll();
}

@Search
public List<Patient> searchByFamily(
        @RequiredParam(name = Patient.SP_FAMILY) StringParam family) {
    return patientDao.findByFamily(family.getValue());
}
```

Use `@RequiredParam` for mandatory parameters, `@OptionalParam` for optional ones.

## String Parameters

### Basic String

```java
@Search
public List<Patient> searchByFamily(
        @OptionalParam(name = Patient.SP_FAMILY) StringParam family) {

    if (family == null) {
        return patientDao.findAll();
    }

    String value = family.getValue();
    boolean exact = family.isExact();      // :exact modifier
    boolean contains = family.isContains(); // :contains modifier

    if (exact) {
        return patientDao.findByFamilyExact(value);
    } else if (contains) {
        return patientDao.findByFamilyContains(value);
    } else {
        // Default: case-insensitive prefix match
        return patientDao.findByFamilyPrefix(value);
    }
}
```

### String Modifiers

| Modifier    | Method         | Behaviour                        |
| ----------- | -------------- | -------------------------------- |
| (none)      | default        | Case-insensitive prefix match    |
| `:exact`    | `isExact()`    | Case-sensitive exact match       |
| `:contains` | `isContains()` | Case-insensitive substring match |

## Token Parameters

Used for coded values with system|value format.

```java
@Search
public List<Patient> searchByIdentifier(
        @OptionalParam(name = Patient.SP_IDENTIFIER) TokenParam identifier) {

    if (identifier == null) {
        return patientDao.findAll();
    }

    String system = identifier.getSystem();
    String value = identifier.getValue();

    // Handle modifiers
    if (identifier.getModifier() == TokenParamModifier.NOT) {
        return patientDao.findByIdentifierNot(system, value);
    }

    if (system != null && value != null) {
        return patientDao.findByIdentifier(system, value);
    } else if (value != null) {
        return patientDao.findByIdentifierValue(value);
    } else if (system != null) {
        return patientDao.findByIdentifierSystem(system);
    }

    return patientDao.findAll();
}
```

### Token Modifiers

| Modifier  | Enum                        | Use Case                   |
| --------- | --------------------------- | -------------------------- |
| `:not`    | `TokenParamModifier.NOT`    | Exclude matching           |
| `:text`   | `TokenParamModifier.TEXT`   | Match on display text      |
| `:in`     | `TokenParamModifier.IN`     | Code in ValueSet           |
| `:not-in` | `TokenParamModifier.NOT_IN` | Code not in ValueSet       |
| `:below`  | `TokenParamModifier.BELOW`  | Concept below in hierarchy |
| `:above`  | `TokenParamModifier.ABOVE`  | Concept above in hierarchy |

## Date Parameters

### Simple Date

```java
@Search
public List<Observation> searchByDate(
        @OptionalParam(name = Observation.SP_DATE) DateParam date) {

    if (date == null) {
        return observationDao.findAll();
    }

    ParamPrefixEnum prefix = date.getPrefix();
    Date value = date.getValue();
    TemporalPrecisionEnum precision = date.getPrecision();

    switch (prefix) {
        case EQUAL:
            return observationDao.findByDateEquals(value, precision);
        case GREATERTHAN:
            return observationDao.findByDateAfter(value);
        case GREATERTHAN_OR_EQUALS:
            return observationDao.findByDateOnOrAfter(value);
        case LESSTHAN:
            return observationDao.findByDateBefore(value);
        case LESSTHAN_OR_EQUALS:
            return observationDao.findByDateOnOrBefore(value);
        case NOT_EQUAL:
            return observationDao.findByDateNotEquals(value);
        default:
            return observationDao.findByDateEquals(value, precision);
    }
}
```

### Date Range

```java
@Search
public List<Observation> searchByDateRange(
        @OptionalParam(name = Observation.SP_DATE) DateRangeParam dateRange) {

    if (dateRange == null) {
        return observationDao.findAll();
    }

    Date lowerBound = dateRange.getLowerBoundAsInstant();
    Date upperBound = dateRange.getUpperBoundAsInstant();

    if (lowerBound != null && upperBound != null) {
        return observationDao.findByDateBetween(lowerBound, upperBound);
    } else if (lowerBound != null) {
        return observationDao.findByDateOnOrAfter(lowerBound);
    } else if (upperBound != null) {
        return observationDao.findByDateOnOrBefore(upperBound);
    }

    return observationDao.findAll();
}
```

### Date Prefixes

| Prefix | Meaning               |
| ------ | --------------------- |
| `eq`   | Equals (default)      |
| `ne`   | Not equals            |
| `gt`   | Greater than          |
| `lt`   | Less than             |
| `ge`   | Greater than or equal |
| `le`   | Less than or equal    |
| `sa`   | Starts after          |
| `eb`   | Ends before           |
| `ap`   | Approximately         |

## Reference Parameters

### Basic Reference

```java
@Search
public List<Observation> searchBySubject(
        @OptionalParam(name = Observation.SP_SUBJECT) ReferenceParam subject) {

    if (subject == null) {
        return observationDao.findAll();
    }

    String resourceType = subject.getResourceType();  // "Patient"
    String resourceId = subject.getIdPart();          // "123"

    if (resourceType != null) {
        return observationDao.findBySubject(resourceType, resourceId);
    } else {
        return observationDao.findBySubjectId(resourceId);
    }
}
```

### Chained Reference

```java
@Search
public List<Observation> searchByPatientIdentifier(
        @OptionalParam(
            name = Observation.SP_SUBJECT + "." + Patient.SP_IDENTIFIER,
            chainWhitelist = {Patient.SP_IDENTIFIER, Patient.SP_NAME}
        ) ReferenceParam subject) {

    if (subject == null) {
        return observationDao.findAll();
    }

    // When chained, the value contains the chained parameter value
    String chain = subject.getChain();  // "identifier"
    String value = subject.getValue();  // "mrn|12345"

    if ("identifier".equals(chain)) {
        TokenParam token = subject.toTokenParam(fhirContext);
        return observationDao.findByPatientIdentifier(
            token.getSystem(), token.getValue());
    }

    return observationDao.findAll();
}
```

### Reference with Type Restriction

```java
@Search
public List<Observation> searchBySubject(
        @OptionalParam(
            name = Observation.SP_SUBJECT,
            targetTypes = {Patient.class, Group.class}
        ) ReferenceParam subject) {
    // Only accepts Patient or Group references
}
```

## Quantity Parameters

```java
@Search
public List<Observation> searchByValue(
        @OptionalParam(name = Observation.SP_VALUE_QUANTITY)
        QuantityParam quantity) {

    if (quantity == null) {
        return observationDao.findAll();
    }

    ParamPrefixEnum prefix = quantity.getPrefix();
    BigDecimal value = quantity.getValue();
    String system = quantity.getSystem();
    String units = quantity.getUnits();

    // Handle prefix similar to DateParam
    switch (prefix) {
        case GREATERTHAN:
            return observationDao.findByValueGreaterThan(
                value, system, units);
        case LESSTHAN:
            return observationDao.findByValueLessThan(
                value, system, units);
        default:
            return observationDao.findByValueEquals(
                value, system, units);
    }
}
```

## Composite Parameters

```java
@Search
public List<Observation> searchByCodeAndValue(
        @OptionalParam(name = Observation.SP_CODE_VALUE_QUANTITY)
        CompositeParam<TokenParam, QuantityParam> codeValue) {

    if (codeValue == null) {
        return observationDao.findAll();
    }

    TokenParam code = codeValue.getLeftValue();
    QuantityParam value = codeValue.getRightValue();

    return observationDao.findByCodeAndValue(
        code.getSystem(), code.getValue(),
        value.getValue(), value.getUnits()
    );
}
```

## Multi-Value Parameters

### OR Logic (Comma-Separated)

URL: `GET /Patient?family=Smith,Jones`

```java
@Search
public List<Patient> searchByFamilyOr(
        @OptionalParam(name = Patient.SP_FAMILY)
        StringOrListParam families) {

    if (families == null) {
        return patientDao.findAll();
    }

    List<String> familyNames = new ArrayList<>();
    for (StringParam family : families.getValuesAsQueryTokens()) {
        familyNames.add(family.getValue());
    }

    return patientDao.findByFamilyIn(familyNames);
}
```

### AND Logic (Repeated Parameter)

URL: `GET /Patient?family=Smith&family=Jones`

```java
@Search
public List<Patient> searchByFamilyAnd(
        @OptionalParam(name = Patient.SP_FAMILY)
        StringAndListParam families) {

    if (families == null) {
        return patientDao.findAll();
    }

    List<Patient> results = null;

    // Each StringOrListParam represents one parameter instance
    for (StringOrListParam familyOr : families.getValuesAsQueryTokens()) {
        List<String> familyNames = new ArrayList<>();
        for (StringParam family : familyOr.getValuesAsQueryTokens()) {
            familyNames.add(family.getValue());
        }

        List<Patient> matches = patientDao.findByFamilyIn(familyNames);

        if (results == null) {
            results = matches;
        } else {
            results.retainAll(matches);  // AND logic
        }
    }

    return results != null ? results : Collections.emptyList();
}
```

## Include and RevInclude

### \_include

```java
@Search
public List<IBaseResource> searchWithIncludes(
        @OptionalParam(name = Patient.SP_FAMILY) StringParam family,
        @IncludeParam Set<Include> includes) {

    List<Patient> patients = patientDao.findByFamily(
        family != null ? family.getValue() : null);

    List<IBaseResource> results = new ArrayList<>(patients);

    if (includes != null) {
        for (Include include : includes) {
            String paramName = include.getParamName();

            if ("Patient:organization".equals(paramName) ||
                "organization".equals(paramName)) {
                for (Patient patient : patients) {
                    if (patient.hasManagingOrganization()) {
                        Organization org = loadOrganization(
                            patient.getManagingOrganization());
                        if (org != null) {
                            results.add(org);
                        }
                    }
                }
            }
        }
    }

    return results;
}
```

### Restricting Includes

```java
@Search(allowUnknownParams = true)
@IncludeParam(allow = {
    "Patient:organization",
    "Patient:general-practitioner"
})
public List<IBaseResource> searchWithRestrictedIncludes(
        @OptionalParam(name = Patient.SP_FAMILY) StringParam family,
        @IncludeParam Set<Include> includes) {
    // Only specified includes are allowed
}
```

### \_revinclude

```java
@Search
public List<IBaseResource> searchWithRevIncludes(
        @OptionalParam(name = Patient.SP_FAMILY) StringParam family,
        @IncludeParam(reverse = true) Set<Include> revIncludes) {

    List<Patient> patients = patientDao.findByFamily(
        family != null ? family.getValue() : null);

    List<IBaseResource> results = new ArrayList<>(patients);

    if (revIncludes != null) {
        for (Include include : revIncludes) {
            if ("Observation:subject".equals(include.getParamName())) {
                for (Patient patient : patients) {
                    List<Observation> obs = observationDao
                        .findBySubject("Patient", patient.getIdPart());
                    results.addAll(obs);
                }
            }
        }
    }

    return results;
}
```

## Sort Parameters

```java
@Search
public List<Patient> searchWithSort(
        @OptionalParam(name = Patient.SP_FAMILY) StringParam family,
        @Sort SortSpec sort) {

    List<Patient> patients = patientDao.findByFamily(
        family != null ? family.getValue() : null);

    if (sort != null) {
        applySorting(patients, sort);
    }

    return patients;
}

private void applySorting(List<Patient> patients, SortSpec sort) {
    String paramName = sort.getParamName();
    SortOrderEnum order = sort.getOrder();

    Comparator<Patient> comparator = getComparator(paramName);
    if (order == SortOrderEnum.DESC) {
        comparator = comparator.reversed();
    }

    patients.sort(comparator);

    // Handle chained sort
    SortSpec chainedSort = sort.getChain();
    if (chainedSort != null) {
        // Apply secondary sort
        applySorting(patients, chainedSort);
    }
}

private Comparator<Patient> getComparator(String paramName) {
    switch (paramName) {
        case Patient.SP_FAMILY:
            return Comparator.comparing(
                p -> p.getNameFirstRep().getFamily(),
                Comparator.nullsLast(String::compareToIgnoreCase));
        case Patient.SP_BIRTHDATE:
            return Comparator.comparing(
                Patient::getBirthDate,
                Comparator.nullsLast(Date::compareTo));
        default:
            return (a, b) -> 0;
    }
}
```

## Paging

### Count and Offset

```java
@Search
public List<Patient> searchWithPaging(
        @OptionalParam(name = Patient.SP_FAMILY) StringParam family,
        @Count Integer count,
        @Offset Integer offset) {

    int pageSize = count != null ? count : 20;
    int startIndex = offset != null ? offset : 0;

    return patientDao.findByFamily(
        family != null ? family.getValue() : null,
        startIndex,
        pageSize
    );
}
```

### IBundleProvider for Large Results

```java
@Search
public IBundleProvider searchWithProvider(
        @OptionalParam(name = Patient.SP_FAMILY) StringParam family) {

    // Only fetch IDs, not full resources
    List<String> matchingIds = patientDao.findIdsByFamily(
        family != null ? family.getValue() : null);

    return new IBundleProvider() {
        @Override
        public Integer size() {
            return matchingIds.size();
        }

        @Override
        public List<IBaseResource> getResources(int from, int to) {
            List<IBaseResource> results = new ArrayList<>();
            int end = Math.min(to, matchingIds.size());

            for (int i = from; i < end; i++) {
                Patient patient = patientDao.findById(matchingIds.get(i));
                if (patient != null) {
                    results.add(patient);
                }
            }

            return results;
        }

        @Override
        public InstantType getPublished() {
            return InstantType.withCurrentTime();
        }

        @Override
        public String getUuid() {
            return UUID.randomUUID().toString();
        }

        @Override
        public Integer preferredPageSize() {
            return 20;
        }
    };
}
```

## Named Queries

```java
@Search(queryName = "patientsWithNoGender")
public List<Patient> findPatientsWithNoGender() {
    return patientDao.findByGenderNull();
}

@Search(queryName = "recentPatients")
public List<Patient> findRecentPatients(
        @RequiredParam(name = "days") NumberParam days) {
    Date since = Date.from(
        Instant.now().minus(days.getValue().intValue(), ChronoUnit.DAYS));
    return patientDao.findCreatedAfter(since);
}
```

URL: `GET /Patient?_query=patientsWithNoGender`

## Compartment Searches

```java
@Search(compartmentName = "Patient")
public List<Observation> searchPatientCompartment(
        @IdParam IdType patientId,
        @OptionalParam(name = Observation.SP_CODE) TokenParam code) {

    // Search observations in patient's compartment
    List<Observation> observations = observationDao.findByPatient(
        patientId.getIdPart());

    if (code != null) {
        observations = observations.stream()
            .filter(o -> matchesCode(o, code))
            .collect(Collectors.toList());
    }

    return observations;
}
```

URL: `GET /Patient/123/Observation?code=12345-6`

## Custom Search Parameters

### Defining Custom Parameters

```java
@Search
public List<Patient> searchByCustomParam(
        @OptionalParam(name = "myCustomParam") StringParam custom) {
    // Handle custom search parameter
}
```

### Advertising in CapabilityStatement

Use an interceptor to add custom search parameters:

```java
@Hook(Pointcut.SERVER_CAPABILITY_STATEMENT_GENERATED)
public void customizeCapabilityStatement(
        IBaseConformance theCapabilityStatement) {

    CapabilityStatement cs = (CapabilityStatement) theCapabilityStatement;

    for (CapabilityStatement.CapabilityStatementRestResourceComponent resource :
            cs.getRestFirstRep().getResource()) {

        if ("Patient".equals(resource.getType())) {
            resource.addSearchParam()
                .setName("myCustomParam")
                .setType(Enumerations.SearchParamType.STRING)
                .setDocumentation("Custom search parameter");
        }
    }
}
```

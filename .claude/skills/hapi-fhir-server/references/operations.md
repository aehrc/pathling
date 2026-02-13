# Extended Operations Reference

## Table of Contents

1. [Operation Basics](#operation-basics)
2. [Instance-Level Operations](#instance-level-operations)
3. [Type-Level Operations](#type-level-operations)
4. [Server-Level Operations](#server-level-operations)
5. [Operation Parameters](#operation-parameters)
6. [Idempotent Operations](#idempotent-operations)
7. [Manual Request/Response Handling](#manual-requestresponse-handling)
8. [Common FHIR Operations](#common-fhir-operations)

## Operation Basics

Extended operations are RPC-style invocations prefixed with `$` that typically take a Parameters resource as input and return a resource.

### Annotation Structure

```java
@Operation(
    name = "$operation-name",      // Operation name with $ prefix
    idempotent = true,             // Allow GET requests
    bundleType = BundleTypeEnum.SEARCHSET,  // Bundle type if returning Bundle
    manualRequest = false,         // Handle raw request
    manualResponse = false         // Handle raw response
)
public IBaseResource operation(
    @IdParam IdType id,                           // For instance operations
    @OperationParam(name = "param") Type param    // Operation parameters
) {
    // Implementation
}
```

## Instance-Level Operations

Operations on a specific resource instance. Include `@IdParam`:

```java
@Operation(name = "$everything", idempotent = true)
public Bundle patientEverything(
        @IdParam IdType patientId,
        @OperationParam(name = "start") DateType start,
        @OperationParam(name = "end") DateType end,
        @OperationParam(name = "_type") List<StringType> types) {

    Bundle bundle = new Bundle();
    bundle.setType(Bundle.BundleType.SEARCHSET);

    // Add patient
    Patient patient = patientDao.findById(patientId.getIdPart());
    bundle.addEntry().setResource(patient);

    // Add related resources
    Date startDate = start != null ? start.getValue() : null;
    Date endDate = end != null ? end.getValue() : null;
    Set<String> typeFilter = types != null
        ? types.stream().map(StringType::getValue).collect(Collectors.toSet())
        : null;

    List<IBaseResource> related = resourceDao.findRelatedResources(
        patientId.getIdPart(), startDate, endDate, typeFilter);

    for (IBaseResource resource : related) {
        bundle.addEntry().setResource(resource);
    }

    return bundle;
}
```

URL: `POST /Patient/123/$everything` or `GET /Patient/123/$everything?start=2024-01-01`

## Type-Level Operations

Operations on a resource type. No `@IdParam`:

```java
@Operation(name = "$match", idempotent = true)
public Bundle patientMatch(
        @OperationParam(name = "resource") Patient inputPatient,
        @OperationParam(name = "onlyCertainMatches") BooleanType onlyCertain) {

    Bundle bundle = new Bundle();
    bundle.setType(Bundle.BundleType.SEARCHSET);

    boolean certainOnly = onlyCertain != null && onlyCertain.booleanValue();

    List<MatchResult> matches = matchingService.findMatches(
        inputPatient, certainOnly);

    for (MatchResult match : matches) {
        Bundle.BundleEntryComponent entry = bundle.addEntry();
        entry.setResource(match.getPatient());
        entry.getSearch()
            .setMode(Bundle.SearchEntryMode.MATCH)
            .setScore(match.getScore());
    }

    return bundle;
}
```

URL: `POST /Patient/$match`

## Server-Level Operations

Operations at the server level. Implement in a Plain Provider:

```java
public class SystemOperationsProvider {

    @Operation(name = "$convert", manualRequest = true, manualResponse = true)
    public void convert(
            HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        // Read input
        String contentType = request.getContentType();
        String input = IOUtils.toString(request.getReader());

        FhirContext ctx = FhirContext.forR4();
        IParser inputParser;
        IParser outputParser;

        // Determine conversion direction
        if (contentType.contains("json")) {
            inputParser = ctx.newJsonParser();
            outputParser = ctx.newXmlParser();
            response.setContentType("application/fhir+xml");
        } else {
            inputParser = ctx.newXmlParser();
            outputParser = ctx.newJsonParser();
            response.setContentType("application/fhir+json");
        }

        // Convert
        IBaseResource resource = inputParser.parseResource(input);
        String output = outputParser.encodeResourceToString(resource);

        response.getWriter().write(output);
    }

    @Operation(name = "$process-message")
    public Bundle processMessage(
            @OperationParam(name = "content") Bundle messageBundle,
            @OperationParam(name = "async") BooleanType async) {

        if (messageBundle.getType() != Bundle.BundleType.MESSAGE) {
            throw new InvalidRequestException("Input must be a message bundle");
        }

        // Process the message
        Bundle response = messageProcessor.process(messageBundle);

        return response;
    }
}
```

Register the provider:

```java
registerProvider(new SystemOperationsProvider());
```

URL: `POST /$convert`, `POST /$process-message`

## Operation Parameters

### Parameter Types

```java
@Operation(name = "$example")
public Parameters exampleOperation(
        // Primitive types
        @OperationParam(name = "string") StringType stringParam,
        @OperationParam(name = "integer") IntegerType intParam,
        @OperationParam(name = "boolean") BooleanType boolParam,
        @OperationParam(name = "date") DateType dateParam,
        @OperationParam(name = "dateTime") DateTimeType dateTimeParam,
        @OperationParam(name = "code") CodeType codeParam,

        // Complex types
        @OperationParam(name = "coding") Coding codingParam,
        @OperationParam(name = "reference") Reference refParam,
        @OperationParam(name = "identifier") Identifier identifierParam,

        // Resources
        @OperationParam(name = "resource") Patient resourceParam,

        // Collections
        @OperationParam(name = "items") List<StringType> listParam,

        // Search parameter types
        @OperationParam(name = "dateRange") DateRangeParam dateRangeParam,
        @OperationParam(name = "token") TokenParam tokenParam) {

    Parameters output = new Parameters();
    output.addParameter().setName("result").setValue(new StringType("success"));
    return output;
}
```

### Required vs Optional Parameters

```java
@Operation(name = "$lookup")
public Parameters lookup(
        // Required - no default handling
        @OperationParam(name = "code", min = 1) CodeType code,
        @OperationParam(name = "system", min = 1) UriType system,

        // Optional
        @OperationParam(name = "version", min = 0) StringType version) {

    if (code == null || system == null) {
        throw new InvalidRequestException("code and system are required");
    }

    // Implementation
}
```

### Multi-valued Parameters

```java
@Operation(name = "$batch-lookup")
public Parameters batchLookup(
        @OperationParam(name = "coding", min = 1, max = OperationParam.MAX_UNLIMITED)
        List<Coding> codings) {

    Parameters output = new Parameters();

    for (Coding coding : codings) {
        Parameters.ParametersParameterComponent param = output.addParameter();
        param.setName("result");

        // Look up each coding
        ConceptDefinitionComponent definition = lookupCoding(coding);
        if (definition != null) {
            param.addPart()
                .setName("display")
                .setValue(new StringType(definition.getDisplay()));
        }
    }

    return output;
}
```

## Idempotent Operations

Setting `idempotent = true` allows HTTP GET requests (only works if all parameters are primitives):

```java
@Operation(name = "$stats", idempotent = true)
public Parameters getStats(
        @OperationParam(name = "resourceType") CodeType resourceType) {

    Parameters params = new Parameters();

    if (resourceType != null) {
        long count = resourceDao.countByType(resourceType.getCode());
        params.addParameter()
            .setName("count")
            .setValue(new IntegerType((int) count));
    } else {
        Map<String, Long> counts = resourceDao.countAll();
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            params.addParameter()
                .setName(entry.getKey())
                .setValue(new IntegerType(entry.getValue().intValue()));
        }
    }

    return params;
}
```

URL: `GET /$stats?resourceType=Patient`

## Manual Request/Response Handling

For complete control over request/response:

```java
@Operation(name = "$custom", manualRequest = true, manualResponse = true)
public void customOperation(
        HttpServletRequest request,
        HttpServletResponse response,
        RequestDetails requestDetails) throws IOException {

    // Read raw request body
    String body = IOUtils.toString(request.getReader());
    String contentType = request.getContentType();

    // Parse based on content type
    IBaseResource input = null;
    if (contentType != null && contentType.contains("json")) {
        input = fhirContext.newJsonParser().parseResource(body);
    } else if (contentType != null && contentType.contains("xml")) {
        input = fhirContext.newXmlParser().parseResource(body);
    }

    // Process
    IBaseResource output = process(input);

    // Write response
    String acceptHeader = request.getHeader("Accept");
    if (acceptHeader != null && acceptHeader.contains("xml")) {
        response.setContentType("application/fhir+xml");
        response.getWriter().write(
            fhirContext.newXmlParser().encodeResourceToString(output));
    } else {
        response.setContentType("application/fhir+json");
        response.getWriter().write(
            fhirContext.newJsonParser().encodeResourceToString(output));
    }
}
```

### Accessing Servlet Details

```java
@Operation(name = "$detailed")
public Parameters detailedOperation(
        @OperationParam(name = "input") StringType input,
        RequestDetails requestDetails,
        ServletRequestDetails servletDetails) {

    HttpServletRequest request = servletDetails.getServletRequest();
    HttpServletResponse response = servletDetails.getServletResponse();

    // Access headers
    String customHeader = request.getHeader("X-Custom-Header");

    // Access user data from interceptors
    String userId = (String) requestDetails.getUserData().get("userId");

    // Set response headers
    response.setHeader("X-Operation-Id", UUID.randomUUID().toString());

    return new Parameters();
}
```

## Common FHIR Operations

### $validate

```java
@Operation(name = "$validate")
public MethodOutcome validate(
        @ResourceParam Patient resource,
        @OperationParam(name = "mode") CodeType mode,
        @OperationParam(name = "profile") UriType profile) {

    ValidationModeEnum validationMode = ValidationModeEnum.CREATE;
    if (mode != null) {
        validationMode = ValidationModeEnum.forCode(mode.getCode());
    }

    String profileUrl = profile != null ? profile.getValue() : null;

    ValidationResult result = validator.validate(resource, profileUrl);

    MethodOutcome outcome = new MethodOutcome();

    if (!result.isSuccessful()) {
        OperationOutcome oo = (OperationOutcome) result.toOperationOutcome();
        throw new UnprocessableEntityException("Validation failed", oo);
    }

    outcome.setOperationOutcome(result.toOperationOutcome());
    return outcome;
}
```

### $meta

```java
@Operation(name = "$meta", idempotent = true)
public Parameters getMeta(@IdParam IdType id) {
    IBaseResource resource = resourceDao.read(id);

    Parameters params = new Parameters();
    params.addParameter()
        .setName("return")
        .setValue(resource.getMeta());

    return params;
}

@Operation(name = "$meta-add")
public Parameters addMeta(
        @IdParam IdType id,
        @OperationParam(name = "meta") Meta metaToAdd) {

    IBaseResource resource = resourceDao.read(id);

    // Add tags
    for (Coding tag : metaToAdd.getTag()) {
        resource.getMeta().addTag(tag);
    }

    // Add security labels
    for (Coding security : metaToAdd.getSecurity()) {
        resource.getMeta().addSecurity(security);
    }

    // Add profiles
    for (CanonicalType profile : metaToAdd.getProfile()) {
        resource.getMeta().addProfile(profile.getValue());
    }

    resourceDao.update(resource);

    Parameters params = new Parameters();
    params.addParameter()
        .setName("return")
        .setValue(resource.getMeta());

    return params;
}
```

### $expand (ValueSet)

```java
@Operation(name = "$expand", idempotent = true)
public ValueSet expand(
        @IdParam(optional = true) IdType id,
        @OperationParam(name = "url") UriType url,
        @OperationParam(name = "filter") StringType filter,
        @OperationParam(name = "offset") IntegerType offset,
        @OperationParam(name = "count") IntegerType count) {

    ValueSet valueSet;

    if (id != null) {
        valueSet = valueSetDao.read(id);
    } else if (url != null) {
        valueSet = valueSetDao.findByUrl(url.getValue());
    } else {
        throw new InvalidRequestException("Either id or url is required");
    }

    String filterText = filter != null ? filter.getValue() : null;
    int offsetVal = offset != null ? offset.getValue() : 0;
    int countVal = count != null ? count.getValue() : 100;

    ValueSet expanded = terminologyService.expand(
        valueSet, filterText, offsetVal, countVal);

    return expanded;
}
```

### $translate (ConceptMap)

```java
@Operation(name = "$translate", idempotent = true)
public Parameters translate(
        @OperationParam(name = "url") UriType conceptMapUrl,
        @OperationParam(name = "source") UriType sourceValueSet,
        @OperationParam(name = "code") CodeType code,
        @OperationParam(name = "system") UriType system,
        @OperationParam(name = "target") UriType targetValueSet) {

    ConceptMap conceptMap = conceptMapDao.findByUrl(conceptMapUrl.getValue());

    List<TranslationResult> translations = terminologyService.translate(
        conceptMap,
        system.getValue(),
        code.getCode(),
        targetValueSet != null ? targetValueSet.getValue() : null
    );

    Parameters params = new Parameters();
    params.addParameter()
        .setName("result")
        .setValue(new BooleanType(!translations.isEmpty()));

    for (TranslationResult translation : translations) {
        Parameters.ParametersParameterComponent match = params.addParameter();
        match.setName("match");
        match.addPart()
            .setName("equivalence")
            .setValue(new CodeType(translation.getEquivalence().toCode()));
        match.addPart()
            .setName("concept")
            .setValue(translation.getCoding());
    }

    return params;
}
```

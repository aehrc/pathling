# Resource Providers Reference

## Table of Contents

1. [IResourceProvider Interface](#iresourceprovider-interface)
2. [Plain Providers](#plain-providers)
3. [Read Operations](#read-operations)
4. [Create Operations](#create-operations)
5. [Update Operations](#update-operations)
6. [Delete Operations](#delete-operations)
7. [Patch Operations](#patch-operations)
8. [History Operations](#history-operations)
9. [Validate Operations](#validate-operations)
10. [Transaction Operations](#transaction-operations)
11. [Accessing Request Context](#accessing-request-context)

## IResourceProvider Interface

Resource providers handle operations for a specific FHIR resource type.

```java
public class PatientResourceProvider implements IResourceProvider {

    @Override
    public Class<Patient> getResourceType() {
        return Patient.class;
    }

    // Operation methods go here
}
```

Register providers in the server:

```java
setResourceProviders(List.of(
    new PatientResourceProvider(),
    new ObservationResourceProvider()
));
```

## Plain Providers

Plain providers handle operations across multiple resource types or server-level operations. They do not implement `IResourceProvider`.

```java
public class SystemOperationsProvider {

    @Transaction
    public Bundle transaction(@TransactionParam Bundle input) {
        // Handle transaction bundle
    }

    @Operation(name = "$closure")
    public ConceptMap closure(
            @OperationParam(name = "name") StringType name,
            @OperationParam(name = "concept") List<Coding> concepts) {
        // Server-level operation
    }
}
```

Register with `registerProvider()`:

```java
registerProvider(new SystemOperationsProvider());
```

## Read Operations

### Basic Read

```java
@Read
public Patient read(@IdParam IdType id) {
    Patient patient = patientDao.findById(id.getIdPart());
    if (patient == null) {
        throw new ResourceNotFoundException(id);
    }
    return patient;
}
```

### Version Read (VRead)

```java
@Read(version = true)
public Patient readOrVread(@IdParam IdType id) {
    if (id.hasVersionIdPart()) {
        // VRead: load specific version
        return patientDao.findByIdAndVersion(
            id.getIdPart(),
            id.getVersionIdPart()
        );
    } else {
        // Read: load current version
        return patientDao.findById(id.getIdPart());
    }
}
```

## Create Operations

### Basic Create

```java
@Create
public MethodOutcome create(@ResourceParam Patient patient) {
    // Validate
    if (patient.getIdentifierFirstRep().isEmpty()) {
        throw new UnprocessableEntityException("Identifier required");
    }

    // Save and get assigned ID
    String newId = patientDao.save(patient);

    MethodOutcome outcome = new MethodOutcome();
    outcome.setId(new IdType("Patient", newId, "1"));
    outcome.setCreated(true);
    outcome.setResource(patient);
    return outcome;
}
```

### Conditional Create

Prevents duplicate creation based on search criteria:

```java
@Create
public MethodOutcome createConditional(
        @ResourceParam Patient patient,
        @ConditionalUrlParam String conditional) {

    if (conditional != null) {
        // Check if resource already exists
        Patient existing = findByConditional(conditional);
        if (existing != null) {
            MethodOutcome outcome = new MethodOutcome();
            outcome.setId(existing.getIdElement());
            outcome.setCreated(false);
            return outcome;
        }
    }

    // Create new resource
    String newId = patientDao.save(patient);
    MethodOutcome outcome = new MethodOutcome();
    outcome.setId(new IdType("Patient", newId, "1"));
    outcome.setCreated(true);
    return outcome;
}
```

Client sends `If-None-Exist` header with search criteria.

## Update Operations

### Basic Update

```java
@Update
public MethodOutcome update(
        @IdParam IdType id,
        @ResourceParam Patient patient) {

    // Ensure ID matches
    patient.setId(id);

    // Save with new version
    String newVersion = patientDao.update(patient);

    MethodOutcome outcome = new MethodOutcome();
    outcome.setId(id.withVersion(newVersion));
    return outcome;
}
```

### Conditional Update

```java
@Update
public MethodOutcome updateConditional(
        @ResourceParam Patient patient,
        @IdParam IdType id,
        @ConditionalUrlParam String conditional) {

    IdType targetId;

    if (conditional != null) {
        // Find by search criteria
        List<Patient> matches = searchByConditional(conditional);

        if (matches.isEmpty()) {
            throw new ResourceNotFoundException(
                "No resource found matching: " + conditional);
        }
        if (matches.size() > 1) {
            throw new PreconditionFailedException(
                "Multiple resources found matching: " + conditional);
        }

        targetId = matches.get(0).getIdElement();
    } else {
        targetId = id;
    }

    patient.setId(targetId);
    String newVersion = patientDao.update(patient);

    MethodOutcome outcome = new MethodOutcome();
    outcome.setId(targetId.withVersion(newVersion));
    return outcome;
}
```

## Delete Operations

### Basic Delete

```java
@Delete
public MethodOutcome delete(@IdParam IdType id) {
    patientDao.delete(id.getIdPart());
    return new MethodOutcome();
}
```

Return type can be `void` or `MethodOutcome`.

### Conditional Delete

```java
@Delete
public MethodOutcome deleteConditional(
        @IdParam IdType id,
        @ConditionalUrlParam String conditional) {

    if (conditional != null) {
        List<Patient> matches = searchByConditional(conditional);
        for (Patient patient : matches) {
            patientDao.delete(patient.getIdElement().getIdPart());
        }

        MethodOutcome outcome = new MethodOutcome();
        // Optionally set count of deleted resources
        return outcome;
    }

    patientDao.delete(id.getIdPart());
    return new MethodOutcome();
}
```

## Patch Operations

### JSON Patch and XML Patch

```java
@Patch
public OperationOutcome patch(
        @IdParam IdType id,
        PatchTypeEnum patchType,
        @ResourceParam String body) {

    Patient existing = patientDao.findById(id.getIdPart());

    if (patchType == PatchTypeEnum.JSON_PATCH) {
        // Apply JSON Patch (RFC 6902)
        existing = applyJsonPatch(existing, body);
    } else if (patchType == PatchTypeEnum.XML_PATCH) {
        // Apply XML Patch
        existing = applyXmlPatch(existing, body);
    } else if (patchType == PatchTypeEnum.FHIR_PATCH_JSON
            || patchType == PatchTypeEnum.FHIR_PATCH_XML) {
        // Apply FHIR Patch (Parameters resource)
        Parameters patchParams = parsePatch(body, patchType);
        existing = applyFhirPatch(existing, patchParams);
    }

    patientDao.update(existing);

    return new OperationOutcome();
}
```

## History Operations

### Instance History

```java
@History
public List<Patient> instanceHistory(
        @IdParam IdType id,
        @Since InstantType since,
        @At DateRangeParam at) {

    return patientDao.getHistory(
        id.getIdPart(),
        since != null ? since.getValue() : null,
        at
    );
}
```

### Type History

Omit `@IdParam` to get history for all resources of a type:

```java
@History
public List<Patient> typeHistory(
        @Since InstantType since,
        @At DateRangeParam at) {
    return patientDao.getAllHistory(since, at);
}
```

### Server History

Implement in a plain provider:

```java
@History
public List<IBaseResource> serverHistory(
        @Since InstantType since,
        @At DateRangeParam at) {
    return resourceDao.getAllResourceHistory(since, at);
}
```

## Validate Operations

```java
@Validate
public MethodOutcome validate(
        @ResourceParam Patient patient,
        @Validate.Mode ValidationModeEnum mode,
        @Validate.Profile String profile) {

    // Perform validation
    List<String> errors = validatePatient(patient, profile);

    if (!errors.isEmpty()) {
        OperationOutcome oo = new OperationOutcome();
        for (String error : errors) {
            oo.addIssue()
                .setSeverity(OperationOutcome.IssueSeverity.ERROR)
                .setDiagnostics(error);
        }
        throw new UnprocessableEntityException(
            "Validation failed", oo);
    }

    MethodOutcome outcome = new MethodOutcome();
    outcome.setOperationOutcome(new OperationOutcome());
    return outcome;
}
```

## Transaction Operations

```java
@Transaction
public Bundle transaction(@TransactionParam Bundle input) {
    Bundle response = new Bundle();
    response.setType(Bundle.BundleType.TRANSACTIONRESPONSE);

    // Start database transaction
    beginTransaction();

    try {
        for (Bundle.BundleEntryComponent entry : input.getEntry()) {
            Bundle.BundleEntryComponent responseEntry =
                processEntry(entry);
            response.addEntry(responseEntry);
        }

        commitTransaction();
    } catch (Exception e) {
        rollbackTransaction();
        throw new InternalErrorException(
            "Transaction failed: " + e.getMessage(), e);
    }

    return response;
}

private Bundle.BundleEntryComponent processEntry(
        Bundle.BundleEntryComponent entry) {

    Bundle.HTTPVerb method = entry.getRequest().getMethod();
    String url = entry.getRequest().getUrl();
    IBaseResource resource = entry.getResource();

    Bundle.BundleEntryComponent responseEntry =
        new Bundle.BundleEntryComponent();

    switch (method) {
        case POST:
            // Create resource
            MethodOutcome createOutcome = create(resource);
            responseEntry.getResponse()
                .setStatus("201 Created")
                .setLocation(createOutcome.getId().getValue());
            break;

        case PUT:
            // Update resource
            MethodOutcome updateOutcome = update(resource);
            responseEntry.getResponse()
                .setStatus("200 OK")
                .setLocation(updateOutcome.getId().getValue());
            break;

        case DELETE:
            // Delete resource
            delete(parseId(url));
            responseEntry.getResponse().setStatus("204 No Content");
            break;

        case GET:
            // Read resource
            IBaseResource result = read(parseId(url));
            responseEntry.setResource(result);
            responseEntry.getResponse().setStatus("200 OK");
            break;
    }

    return responseEntry;
}
```

## Accessing Request Context

### RequestDetails Parameter

Add `RequestDetails` to any method to access request information:

```java
@Read
public Patient read(
        @IdParam IdType id,
        RequestDetails requestDetails) {

    // Access request info
    String requestId = requestDetails.getRequestId();
    String tenantId = requestDetails.getTenantId();
    Map<String, String[]> params = requestDetails.getParameters();

    // Access user data (set by interceptors)
    String userId = (String) requestDetails.getUserData().get("userId");

    return patientDao.findById(id.getIdPart());
}
```

### ServletRequestDetails

For servlet-specific access:

```java
@Read
public Patient read(
        @IdParam IdType id,
        RequestDetails requestDetails,
        ServletRequestDetails servletDetails) {

    HttpServletRequest request = servletDetails.getServletRequest();
    HttpServletResponse response = servletDetails.getServletResponse();

    // Access servlet-specific data
    String header = request.getHeader("X-Custom-Header");

    return patientDao.findById(id.getIdPart());
}
```

### Passing Data Between Interceptors and Providers

Interceptors can store data for providers:

```java
// In interceptor
@Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
public void preHandle(RequestDetails details) {
    details.getUserData().put("startTime", System.currentTimeMillis());
    details.getUserData().put("authenticatedUser", extractUser(details));
}

// In provider
@Read
public Patient read(@IdParam IdType id, RequestDetails details) {
    User user = (User) details.getUserData().get("authenticatedUser");
    // Use authenticated user for access control
}
```

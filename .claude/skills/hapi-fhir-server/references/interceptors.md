# Interceptors Reference

## Table of Contents

1. [Creating Interceptors](#creating-interceptors)
2. [Server Pointcuts](#server-pointcuts)
3. [Return Values and Flow Control](#return-values-and-flow-control)
4. [Built-in Server Interceptors](#built-in-server-interceptors)
5. [Passing Data Between Hooks](#passing-data-between-hooks)
6. [Customising CapabilityStatement](#customising-capabilitystatement)
7. [Exception Handling](#exception-handling)

## Creating Interceptors

### Basic Structure

```java
@Interceptor
public class MyInterceptor {

    @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
    public void preHandle(
            RequestDetails requestDetails,
            HttpServletRequest request,
            HttpServletResponse response) {
        // Logic before request handling
    }

    @Hook(Pointcut.SERVER_OUTGOING_RESPONSE)
    public void postHandle(
            RequestDetails requestDetails,
            IBaseResource resource) {
        // Logic after response generated
    }
}
```

### Registration

```java
@Override
protected void initialize() throws ServletException {
    registerInterceptor(new MyInterceptor());
    registerInterceptor(new LoggingInterceptor());
}
```

### Hook Method Requirements

- Must be `public`
- Can include any parameters specified for the pointcut
- Return type depends on pointcut (often `void` or `boolean`)
- Parameters are injected based on type, not name

## Server Pointcuts

### Request Lifecycle

| Pointcut                                       | When                            | Common Uses                   |
| ---------------------------------------------- | ------------------------------- | ----------------------------- |
| `SERVER_INCOMING_REQUEST_PRE_PROCESSED`        | Earliest, before any processing | Logging, rate limiting        |
| `SERVER_INCOMING_REQUEST_PRE_HANDLER_SELECTED` | Before handler selection        | Request routing               |
| `SERVER_INCOMING_REQUEST_POST_PROCESSED`       | After parsing, before handler   | Request validation            |
| `SERVER_INCOMING_REQUEST_PRE_HANDLED`          | After handler selected          | Authentication, authorisation |
| `SERVER_OUTGOING_RESPONSE`                     | After handler, before response  | Response modification         |
| `SERVER_PROCESSING_COMPLETED_NORMALLY`         | After successful completion     | Cleanup, metrics              |
| `SERVER_PROCESSING_COMPLETED`                  | After any completion            | Always runs, cleanup          |

### Example: Request Timing

```java
@Interceptor
public class TimingInterceptor {

    @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_PROCESSED)
    public void startTiming(RequestDetails details) {
        details.getUserData().put("startTime", System.currentTimeMillis());
    }

    @Hook(Pointcut.SERVER_PROCESSING_COMPLETED)
    public void endTiming(RequestDetails details) {
        Long startTime = (Long) details.getUserData().get("startTime");
        if (startTime != null) {
            long duration = System.currentTimeMillis() - startTime;
            log.info("Request {} took {}ms",
                details.getRequestId(), duration);
        }
    }
}
```

### Exception Handling Pointcuts

| Pointcut                                   | Description                           |
| ------------------------------------------ | ------------------------------------- |
| `SERVER_HANDLE_EXCEPTION`                  | On any exception during processing    |
| `SERVER_PRE_PROCESS_OUTGOING_EXCEPTION`    | Transform exception before response   |
| `SERVER_OUTGOING_FAILURE_OPERATIONOUTCOME` | When returning OperationOutcome error |

### Storage Pointcuts (Manual Trigger in Plain Server)

| Pointcut                              | Description                         |
| ------------------------------------- | ----------------------------------- |
| `STORAGE_PRESTORAGE_RESOURCE_CREATED` | Before resource creation            |
| `STORAGE_PRESTORAGE_RESOURCE_UPDATED` | Before resource update              |
| `STORAGE_PRESTORAGE_RESOURCE_DELETED` | Before resource deletion            |
| `STORAGE_PRESHOW_RESOURCES`           | Before resources returned to client |

For Plain Server, trigger these manually:

```java
@Create
public MethodOutcome create(
        @ResourceParam Patient patient,
        RequestDetails requestDetails) {

    // Trigger storage pointcut
    HookParams params = new HookParams()
        .add(IBaseResource.class, patient)
        .add(RequestDetails.class, requestDetails);

    interceptorBroadcaster.callHooks(
        Pointcut.STORAGE_PRESTORAGE_RESOURCE_CREATED, params);

    // Perform actual creation
    String id = patientDao.save(patient);

    return new MethodOutcome(new IdType("Patient", id));
}
```

## Return Values and Flow Control

### Boolean Returns

```java
@Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
public boolean preHandle(RequestDetails details, HttpServletResponse response)
        throws IOException {

    if (!isAuthorised(details)) {
        response.setStatus(401);
        response.getWriter().write("Unauthorised");
        return false;  // Stop processing, response handled
    }

    return true;  // Continue processing
}
```

- `true`: Continue to next interceptor/handler
- `false`: Stop processing (interceptor handled response)

### Void Returns

For observation-only hooks:

```java
@Hook(Pointcut.SERVER_PROCESSING_COMPLETED)
public void onComplete(RequestDetails details) {
    // Logging, metrics - cannot stop processing
}
```

### Throwing Exceptions

```java
@Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
public void authenticate(RequestDetails details) {
    if (!isAuthenticated(details)) {
        throw new AuthenticationException("Authentication required");
    }
}
```

## Built-in Server Interceptors

### LoggingInterceptor

```java
LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
loggingInterceptor.setLoggerName("fhir.access");
loggingInterceptor.setMessageFormat(
    "${operationType} ${idOrResourceName} - " +
    "User: ${requestHeader.X-User} - " +
    "Time: ${processingTimeMillis}ms"
);
registerInterceptor(loggingInterceptor);
```

Available variables: `${operationType}`, `${operationName}`, `${idOrResourceName}`, `${requestId}`, `${processingTimeMillis}`, `${requestHeader.HEADER_NAME}`, `${remoteAddr}`, `${requestUrl}`, etc.

### ResponseHighlighterInterceptor

Returns syntax-highlighted HTML for browser requests:

```java
registerInterceptor(new ResponseHighlighterInterceptor());
```

### CorsInterceptor

```java
CorsInterceptor corsInterceptor = new CorsInterceptor();
corsInterceptor.setAllowedOrigin("*");
corsInterceptor.setAllowedMethods(Arrays.asList(
    "GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));
corsInterceptor.setAllowedHeaders(Arrays.asList(
    "Content-Type", "Authorization", "Accept"));
corsInterceptor.setExposedHeaders(Arrays.asList(
    "Location", "Content-Location", "ETag"));
registerInterceptor(corsInterceptor);
```

### RequestValidatingInterceptor

```java
FhirContext ctx = getFhirContext();
ValidationSupportChain chain = new ValidationSupportChain(
    new DefaultProfileValidationSupport(ctx),
    new InMemoryTerminologyServerValidationSupport(ctx)
);

FhirInstanceValidator validator = new FhirInstanceValidator(chain);

RequestValidatingInterceptor validatingInterceptor =
    new RequestValidatingInterceptor();
validatingInterceptor.addValidatorModule(validator);
validatingInterceptor.setFailOnSeverity(ResultSeverityEnum.ERROR);
validatingInterceptor.setAddResponseHeaderOnSeverity(
    ResultSeverityEnum.WARNING);
validatingInterceptor.setResponseHeaderName("X-Validation-Warning");

registerInterceptor(validatingInterceptor);
```

### ResponseValidatingInterceptor

Same configuration as RequestValidatingInterceptor, but validates responses:

```java
ResponseValidatingInterceptor responseValidator =
    new ResponseValidatingInterceptor();
responseValidator.addValidatorModule(validator);
responseValidator.setFailOnSeverity(ResultSeverityEnum.ERROR);
registerInterceptor(responseValidator);
```

### ExceptionHandlingInterceptor

```java
ExceptionHandlingInterceptor exceptionInterceptor =
    new ExceptionHandlingInterceptor();
exceptionInterceptor.setReturnStackTracesForExceptionTypes(
    InternalErrorException.class);
registerInterceptor(exceptionInterceptor);
```

### ResponseSizeCapturingInterceptor

```java
ResponseSizeCapturingInterceptor sizeInterceptor =
    new ResponseSizeCapturingInterceptor();
sizeInterceptor.setMaximumBodySize(1024 * 1024);  // 1MB limit
registerInterceptor(sizeInterceptor);
```

### ResponseTerminologyDisplayPopulationInterceptor

Automatically populates missing `Coding.display` values:

```java
ResponseTerminologyDisplayPopulationInterceptor displayInterceptor =
    new ResponseTerminologyDisplayPopulationInterceptor(validationSupport);
registerInterceptor(displayInterceptor);
```

## Passing Data Between Hooks

### Using RequestDetails.getUserData()

```java
@Interceptor
public class ContextInterceptor {

    @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
    public void captureContext(RequestDetails details) {
        // Store data for later hooks
        details.getUserData().put("tenantId", extractTenantId(details));
        details.getUserData().put("user", extractUser(details));
        details.getUserData().put("permissions", loadPermissions(details));
    }

    @Hook(Pointcut.SERVER_OUTGOING_RESPONSE)
    public void auditResponse(RequestDetails details, IBaseResource resource) {
        String user = (String) details.getUserData().get("user");
        // Use stored context for audit logging
        auditService.logAccess(user, resource);
    }
}
```

### Accessing in Resource Providers

```java
@Read
public Patient read(@IdParam IdType id, RequestDetails details) {
    User user = (User) details.getUserData().get("user");
    Set<String> permissions =
        (Set<String>) details.getUserData().get("permissions");

    if (!permissions.contains("read:patient")) {
        throw new ForbiddenOperationException("Access denied");
    }

    return patientDao.findById(id.getIdPart());
}
```

## Customising CapabilityStatement

```java
@Interceptor
public class CapabilityStatementCustomizer {

    @Hook(Pointcut.SERVER_CAPABILITY_STATEMENT_GENERATED)
    public void customize(IBaseConformance theCapabilityStatement) {
        CapabilityStatement cs =
            (CapabilityStatement) theCapabilityStatement;

        // Set software info
        cs.getSoftware()
            .setName("My FHIR Server")
            .setVersion("1.0.0")
            .setReleaseDate(new Date());

        // Set publisher
        cs.setPublisher("My Organisation");

        // Add security information
        CapabilityStatement.CapabilityStatementRestSecurityComponent security =
            cs.getRestFirstRep().getSecurity();
        security.addService()
            .addCoding()
            .setSystem("http://terminology.hl7.org/CodeSystem/restful-security-service")
            .setCode("OAuth")
            .setDisplay("OAuth");

        // Add implementation guide
        cs.addImplementationGuide(
            "http://hl7.org/fhir/us/core/ImplementationGuide/hl7.fhir.us.core");

        // Customise operations
        for (CapabilityStatement.CapabilityStatementRestResourceComponent resource :
                cs.getRestFirstRep().getResource()) {
            if ("Patient".equals(resource.getType())) {
                // Add custom operation
                resource.addOperation()
                    .setName("everything")
                    .setDefinition("http://hl7.org/fhir/OperationDefinition/Patient-everything");
            }
        }
    }
}
```

## Exception Handling

### Custom Exception Handler

```java
@Interceptor
public class CustomExceptionHandler {

    @Hook(Pointcut.SERVER_HANDLE_EXCEPTION)
    public boolean handleException(
            RequestDetails requestDetails,
            BaseServerResponseException exception,
            HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        // Log the error
        log.error("Error processing {}: {}",
            requestDetails.getRequestPath(),
            exception.getMessage(),
            exception);

        // Track metrics
        metricsService.recordError(
            exception.getStatusCode(),
            exception.getClass().getSimpleName());

        // Let HAPI handle the response
        return true;
    }
}
```

### Transforming Exceptions

```java
@Hook(Pointcut.SERVER_PRE_PROCESS_OUTGOING_EXCEPTION)
public BaseServerResponseException transformException(
        RequestDetails requestDetails,
        Throwable throwable) {

    if (throwable instanceof SQLException) {
        // Convert database errors to generic 500
        return new InternalErrorException(
            "Database error occurred");
    }

    if (throwable instanceof BaseServerResponseException) {
        return (BaseServerResponseException) throwable;
    }

    return new InternalErrorException(throwable);
}
```

### Custom OperationOutcome

```java
@Hook(Pointcut.SERVER_OUTGOING_FAILURE_OPERATIONOUTCOME)
public void customizeErrorResponse(
        RequestDetails requestDetails,
        IBaseOperationOutcome operationOutcome) {

    OperationOutcome oo = (OperationOutcome) operationOutcome;

    // Add request ID to all issues
    String requestId = requestDetails.getRequestId();
    for (OperationOutcome.OperationOutcomeIssueComponent issue :
            oo.getIssue()) {
        issue.addExtension()
            .setUrl("http://example.org/fhir/StructureDefinition/request-id")
            .setValue(new StringType(requestId));
    }
}
```

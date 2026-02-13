# Security Reference

## Table of Contents

1. [Authentication](#authentication)
2. [AuthorizationInterceptor](#authorizationinterceptor)
3. [ConsentInterceptor](#consentinterceptor)
4. [SearchNarrowingInterceptor](#searchnarrowinginterceptor)
5. [CORS Configuration](#cors-configuration)
6. [SMART on FHIR Integration](#smart-on-fhir-integration)

## Authentication

### HTTP Basic Authentication

```java
@Interceptor
public class BasicAuthInterceptor {

    @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
    public void authenticate(
            RequestDetails requestDetails,
            HttpServletRequest request) {

        String authHeader = request.getHeader("Authorization");

        if (authHeader == null || !authHeader.startsWith("Basic ")) {
            throw new AuthenticationException("Missing or invalid Authorization header")
                .addAuthenticateHeaderForRealm("FHIR Server");
        }

        String base64Credentials = authHeader.substring("Basic ".length());
        String credentials = new String(
            Base64.getDecoder().decode(base64Credentials),
            StandardCharsets.UTF_8);
        String[] parts = credentials.split(":", 2);

        if (parts.length != 2) {
            throw new AuthenticationException("Invalid credentials format");
        }

        String username = parts[0];
        String password = parts[1];

        User user = userService.authenticate(username, password);
        if (user == null) {
            throw new AuthenticationException("Invalid username or password");
        }

        // Store user for later use
        requestDetails.getUserData().put("user", user);
    }
}
```

### Bearer Token Authentication

```java
@Interceptor
public class BearerTokenInterceptor {

    private final JwtValidator jwtValidator;

    @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
    public void authenticate(
            RequestDetails requestDetails,
            HttpServletRequest request) {

        String authHeader = request.getHeader("Authorization");

        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            throw new AuthenticationException("Bearer token required");
        }

        String token = authHeader.substring("Bearer ".length());

        try {
            Claims claims = jwtValidator.validate(token);

            // Extract user info from claims
            String userId = claims.getSubject();
            Set<String> scopes = parseScopes(claims.get("scope", String.class));

            requestDetails.getUserData().put("userId", userId);
            requestDetails.getUserData().put("scopes", scopes);

        } catch (JwtException e) {
            throw new AuthenticationException("Invalid or expired token");
        }
    }

    private Set<String> parseScopes(String scopeString) {
        if (scopeString == null) {
            return Collections.emptySet();
        }
        return Arrays.stream(scopeString.split(" "))
            .collect(Collectors.toSet());
    }
}
```

## AuthorizationInterceptor

### Basic Implementation

```java
public class MyAuthorizationInterceptor extends AuthorizationInterceptor {

    @Override
    public List<IAuthRule> buildRuleList(RequestDetails requestDetails) {
        User user = (User) requestDetails.getUserData().get("user");

        if (user == null) {
            // Not authenticated - deny all
            return new RuleBuilder()
                .denyAll("Not authenticated")
                .build();
        }

        if (user.isAdmin()) {
            // Admin has full access
            return new RuleBuilder()
                .allowAll()
                .build();
        }

        // Regular user - compartment-based access
        IdType patientId = new IdType("Patient", user.getPatientId());

        return new RuleBuilder()
            // Allow reading metadata
            .allow().metadata()
            .andThen()
            // Allow reading own patient record
            .allow().read().resourcesOfType(Patient.class)
                .inCompartment("Patient", patientId)
            .andThen()
            // Allow reading resources in patient compartment
            .allow().read().allResources()
                .inCompartment("Patient", patientId)
            .andThen()
            // Allow writing to own compartment
            .allow().write().allResources()
                .inCompartment("Patient", patientId)
            .andThen()
            // Deny everything else
            .denyAll("Access denied")
            .build();
    }
}
```

### Rule Types

#### Resource Type Rules

```java
// Allow reading specific resource types
.allow().read().resourcesOfType(Patient.class).withAnyId()

// Allow writing specific resource types
.allow().write().resourcesOfType(Observation.class).withAnyId()

// Allow all operations on specific types
.allow().read().write().resourcesOfType(Patient.class, Observation.class)
```

#### Instance Rules

```java
// Allow access to specific resource instances
.allow().read().instance(new IdType("Patient", "123"))

// Allow access to multiple instances
.allow().read()
    .instances(List.of(
        new IdType("Patient", "123"),
        new IdType("Patient", "456")))
```

#### Compartment Rules

```java
// All resources in patient compartment
.allow().read().allResources()
    .inCompartment("Patient", patientId)

// Specific types in compartment
.allow().read().resourcesOfType(Observation.class)
    .inCompartment("Patient", patientId)

// Multiple compartments
.allow().read().allResources()
    .inCompartment("Patient", new IdType("Patient", "123"))
.andThen()
.allow().read().allResources()
    .inCompartment("Patient", new IdType("Patient", "456"))
```

#### Operation Rules

```java
// Allow specific operations
.allow().operation().named("$everything")
    .onInstance(new IdType("Patient", "123"))

// Allow type-level operations
.allow().operation().named("$validate")
    .onType(Patient.class)

// Allow server-level operations
.allow().operation().named("$meta")
    .onServer()
```

#### Tenant Rules

```java
// Restrict to specific tenants
.allow().read().allResources().forTenantIds("TENANT_A", "TENANT_B")
```

### Combining Rules

```java
return new RuleBuilder()
    // Metadata is always allowed
    .allow().metadata()
    .andThen()

    // Patient can read their own data
    .allow().read().allResources()
        .inCompartment("Patient", patientId)
    .andThen()

    // Patient can create observations
    .allow().create().resourcesOfType(Observation.class)
        .withAnyId()
    .andThen()

    // Practitioners can read all patients
    .allow().read().resourcesOfType(Patient.class)
        .withAnyId()
        .withTester(ctx -> isPractitioner(ctx.getRequestDetails()))
    .andThen()

    // Deny everything else explicitly
    .denyAll("Access denied")
    .build();
```

## ConsentInterceptor

### Implementation

```java
public class MyConsentService implements IConsentService {

    @Override
    public ConsentOutcome startOperation(
            RequestDetails requestDetails,
            IConsentContextServices contextServices) {

        // Fast path for known-safe operations
        if (isMetadataRequest(requestDetails)) {
            return ConsentOutcome.AUTHORIZED;
        }

        // Indicate we need to check individual resources
        return ConsentOutcome.PROCEED;
    }

    @Override
    public ConsentOutcome canSeeResource(
            RequestDetails requestDetails,
            IBaseResource resource,
            IConsentContextServices contextServices) {

        // Check if user can see this resource
        User user = getUser(requestDetails);

        if (resource instanceof Patient) {
            Patient patient = (Patient) resource;
            if (!canAccessPatient(user, patient)) {
                return ConsentOutcome.REJECT;
            }
        }

        // Check consent directives
        if (hasOptedOut(resource, user)) {
            return ConsentOutcome.REJECT;
        }

        return ConsentOutcome.PROCEED;
    }

    @Override
    public ConsentOutcome willSeeResource(
            RequestDetails requestDetails,
            IBaseResource resource,
            IConsentContextServices contextServices) {

        // Optionally redact sensitive data
        User user = getUser(requestDetails);

        if (shouldRedactSensitiveData(user, resource)) {
            redactSensitiveElements(resource);
        }

        return ConsentOutcome.AUTHORIZED;
    }

    @Override
    public void completeOperationSuccess(
            RequestDetails requestDetails,
            IConsentContextServices contextServices) {
        // Create audit trail
        auditService.recordAccess(requestDetails);
    }

    @Override
    public void completeOperationFailure(
            RequestDetails requestDetails,
            BaseServerResponseException exception,
            IConsentContextServices contextServices) {
        // Log failed access attempt
        auditService.recordFailedAccess(requestDetails, exception);
    }
}
```

### Registration

```java
ConsentInterceptor consentInterceptor = new ConsentInterceptor();
consentInterceptor.registerConsentService(new MyConsentService());
registerInterceptor(consentInterceptor);
```

## SearchNarrowingInterceptor

Automatically constrains search results based on user permissions.

### Implementation

```java
public class MySearchNarrowingInterceptor extends SearchNarrowingInterceptor {

    @Override
    protected AuthorizedList buildAuthorizedList(
            RequestDetails requestDetails) {

        String userId = (String) requestDetails.getUserData().get("userId");
        Set<String> scopes = (Set<String>) requestDetails.getUserData().get("scopes");

        if (scopes.contains("admin")) {
            // Admin can see everything
            return new AuthorizedList();
        }

        // Get patient compartments user can access
        List<String> allowedPatients = accessService.getAllowedPatients(userId);

        AuthorizedList authorizedList = new AuthorizedList();
        for (String patientId : allowedPatients) {
            authorizedList.addCompartment("Patient/" + patientId);
        }

        return authorizedList;
    }
}
```

### Configuration Options

```java
MySearchNarrowingInterceptor narrowingInterceptor =
    new MySearchNarrowingInterceptor();

// Also narrow conditional URLs (for create/update/delete)
narrowingInterceptor.setNarrowConditionalUrls(true);

registerInterceptor(narrowingInterceptor);
```

### Combining with AuthorizationInterceptor

```java
// Register in order: narrowing first, then authorization
registerInterceptor(new MySearchNarrowingInterceptor());
registerInterceptor(new MyAuthorizationInterceptor());
```

## CORS Configuration

### Using CorsInterceptor

```java
CorsConfiguration config = new CorsConfiguration();
config.addAllowedOrigin("https://app.example.com");
config.addAllowedOrigin("https://admin.example.com");
config.setAllowedMethods(Arrays.asList(
    "GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));
config.setAllowedHeaders(Arrays.asList(
    "Content-Type",
    "Authorization",
    "Accept",
    "X-FHIR-Starter",
    "Prefer"));
config.setExposedHeaders(Arrays.asList(
    "Location",
    "Content-Location",
    "ETag",
    "X-Request-Id"));
config.setAllowCredentials(true);
config.setMaxAge(3600L);

CorsInterceptor corsInterceptor = new CorsInterceptor(config);
registerInterceptor(corsInterceptor);
```

### Wildcard Origins

```java
CorsConfiguration config = new CorsConfiguration();
config.addAllowedOrigin("*");  // Allow any origin
// Note: credentials cannot be used with wildcard origin
config.setAllowCredentials(false);
```

## SMART on FHIR Integration

HAPI FHIR does not include built-in SMART on FHIR support, but can be integrated with external OAuth2/OIDC providers.

### JWT-Based Authorization

```java
@Interceptor
public class SmartAuthInterceptor {

    private final JwkProvider jwkProvider;
    private final String issuer;
    private final String audience;

    @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
    public void authenticate(
            RequestDetails requestDetails,
            HttpServletRequest request) {

        String authHeader = request.getHeader("Authorization");
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            throw new AuthenticationException("Bearer token required");
        }

        String token = authHeader.substring("Bearer ".length());

        try {
            DecodedJWT jwt = JWT.decode(token);

            // Verify signature
            Jwk jwk = jwkProvider.get(jwt.getKeyId());
            Algorithm algorithm = Algorithm.RSA256(
                (RSAPublicKey) jwk.getPublicKey(), null);
            algorithm.verify(jwt);

            // Verify claims
            if (!issuer.equals(jwt.getIssuer())) {
                throw new AuthenticationException("Invalid issuer");
            }
            if (!jwt.getAudience().contains(audience)) {
                throw new AuthenticationException("Invalid audience");
            }
            if (jwt.getExpiresAt().before(new Date())) {
                throw new AuthenticationException("Token expired");
            }

            // Extract SMART scopes
            String scopeClaim = jwt.getClaim("scope").asString();
            Set<SmartScope> scopes = parseSmartScopes(scopeClaim);

            // Extract patient context
            String patientContext = jwt.getClaim("patient").asString();

            requestDetails.getUserData().put("scopes", scopes);
            requestDetails.getUserData().put("patient", patientContext);

        } catch (Exception e) {
            throw new AuthenticationException("Token validation failed");
        }
    }

    private Set<SmartScope> parseSmartScopes(String scopeString) {
        Set<SmartScope> scopes = new HashSet<>();
        if (scopeString != null) {
            for (String scope : scopeString.split(" ")) {
                SmartScope.parse(scope).ifPresent(scopes::add);
            }
        }
        return scopes;
    }
}
```

### SMART Scope-Based Authorization

```java
public class SmartAuthorizationInterceptor extends AuthorizationInterceptor {

    @Override
    public List<IAuthRule> buildRuleList(RequestDetails requestDetails) {
        Set<SmartScope> scopes =
            (Set<SmartScope>) requestDetails.getUserData().get("scopes");
        String patientContext =
            (String) requestDetails.getUserData().get("patient");

        if (scopes == null || scopes.isEmpty()) {
            return new RuleBuilder().denyAll().build();
        }

        RuleBuilder builder = new RuleBuilder();

        // Always allow metadata
        builder = builder.allow().metadata().andThen();

        for (SmartScope scope : scopes) {
            // patient/Patient.read
            if (scope.getContext() == ScopeContext.PATIENT) {
                IdType patientId = new IdType("Patient", patientContext);

                if (scope.getPermission() == ScopePermission.READ) {
                    builder = builder
                        .allow().read()
                        .resourcesOfType(scope.getResourceType())
                        .inCompartment("Patient", patientId)
                        .andThen();
                }

                if (scope.getPermission() == ScopePermission.WRITE) {
                    builder = builder
                        .allow().write()
                        .resourcesOfType(scope.getResourceType())
                        .inCompartment("Patient", patientId)
                        .andThen();
                }
            }

            // user/Patient.read - broader access
            if (scope.getContext() == ScopeContext.USER) {
                if (scope.getPermission() == ScopePermission.READ) {
                    builder = builder
                        .allow().read()
                        .resourcesOfType(scope.getResourceType())
                        .withAnyId()
                        .andThen();
                }
            }
        }

        return builder.denyAll().build();
    }
}
```

### Well-Known Endpoint

Implement using an interceptor or separate endpoint:

```java
@Interceptor
public class WellKnownInterceptor {

    @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
    public boolean handleWellKnown(
            RequestDetails requestDetails,
            HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        if ("/.well-known/smart-configuration".equals(
                request.getRequestURI())) {

            Map<String, Object> config = new HashMap<>();
            config.put("authorization_endpoint",
                "https://auth.example.com/authorize");
            config.put("token_endpoint",
                "https://auth.example.com/token");
            config.put("capabilities", Arrays.asList(
                "launch-standalone",
                "client-public",
                "client-confidential-symmetric",
                "sso-openid-connect",
                "context-standalone-patient"
            ));
            config.put("scopes_supported", Arrays.asList(
                "openid",
                "profile",
                "patient/*.read",
                "patient/*.write"
            ));

            response.setContentType("application/json");
            response.getWriter().write(
                new ObjectMapper().writeValueAsString(config));

            return false;  // Response handled
        }

        return true;  // Continue processing
    }
}
```

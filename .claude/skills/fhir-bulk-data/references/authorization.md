# SMART Backend Services authorisation

Bulk Data servers should implement OAuth 2.0 authorisation using the SMART Backend Services profile. This enables autonomous backend applications to access FHIR resources without real-time user authorisation.

**Specification:** <https://hl7.org/fhir/smart-app-launch/backend-services.html>

## Authentication flow

SMART Backend Services uses the OAuth 2.0 client credentials flow with JWT assertion for client authentication.

```
Client                                    Authorization Server
  │                                              │
  │  1. Generate JWT assertion                   │
  │                                              │
  │─── POST /token ─────────────────────────────►│
  │    grant_type=client_credentials             │
  │    client_assertion_type=...jwt-bearer       │
  │    client_assertion=[signed JWT]             │
  │    scope=system/*.read                       │
  │                                              │
  │◄── 200 OK ──────────────────────────────────│
  │    access_token=[token]                      │
  │    expires_in=300                            │
  │    scope=system/*.read                       │
```

## Token request parameters

| Parameter               | Value                                                    |
| ----------------------- | -------------------------------------------------------- |
| `grant_type`            | `client_credentials`                                     |
| `client_assertion_type` | `urn:ietf:params:oauth:client-assertion-type:jwt-bearer` |
| `client_assertion`      | Signed JWT (see below)                                   |
| `scope`                 | Requested scopes (e.g., `system/*.read`)                 |

## JWT assertion structure

The client assertion is a signed JWT with these claims:

```json
{
    "iss": "[client_id]",
    "sub": "[client_id]",
    "aud": "[token_endpoint_url]",
    "exp": 1234567890,
    "jti": "[unique-token-id]"
}
```

Requirements:

- Sign with RS384 or ES384 algorithm
- `exp` must be no more than 5 minutes in the future
- `jti` must be unique to prevent replay attacks
- Include `kid` header referencing the public key

## Scopes

Bulk Data uses `system/` scopes to indicate backend service access:

| Scope                     | Access                          |
| ------------------------- | ------------------------------- |
| `system/*.read`           | Read all resource types         |
| `system/Patient.read`     | Read Patient resources only     |
| `system/Observation.read` | Read Observation resources only |

Servers may also support:

- `system/[ResourceType].rs` (SMART v2 format for read + search)
- Granular scopes for specific resource types

## Token response

```json
{
    "access_token": "eyJ...",
    "token_type": "bearer",
    "expires_in": 300,
    "scope": "system/*.read"
}
```

Best practices:

- Tokens should be short-lived (300 seconds recommended maximum)
- The granted scope may differ from the requested scope
- Store tokens securely and refresh before expiry

## Client registration

Before using backend services, clients must register with the server:

1. Generate an asymmetric key pair (RSA or EC)
2. Register the public key with the authorisation server
3. Store the private key securely for signing JWT assertions

Key registration methods:

- JWKS URL: Server fetches keys from client-hosted endpoint
- Direct registration: Client uploads public key during registration

## Discovery

Clients discover authorisation endpoints via the SMART configuration:

```http
GET [fhir-base]/.well-known/smart-configuration
```

Response includes:

```json
{
    "token_endpoint": "https://auth.example.org/token",
    "token_endpoint_auth_methods_supported": ["private_key_jwt"],
    "scopes_supported": ["system/*.read", "system/*.write"]
}
```

## Example: Java client

```java
// Generate JWT assertion
String jwt = Jwts.builder()
    .setIssuer(clientId)
    .setSubject(clientId)
    .setAudience(tokenEndpoint)
    .setExpiration(Date.from(Instant.now().plusSeconds(300)))
    .setId(UUID.randomUUID().toString())
    .signWith(privateKey, SignatureAlgorithm.RS384)
    .compact();

// Request token
HttpRequest request = HttpRequest.newBuilder()
    .uri(URI.create(tokenEndpoint))
    .header("Content-Type", "application/x-www-form-urlencoded")
    .POST(HttpRequest.BodyPublishers.ofString(
        "grant_type=client_credentials" +
        "&client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer" +
        "&client_assertion=" + jwt +
        "&scope=system/*.read"
    ))
    .build();
```

## Example: Python client

```python
import jwt
import requests
import uuid
from datetime import datetime, timedelta

# Generate JWT assertion
now = datetime.utcnow()
claims = {
    "iss": client_id,
    "sub": client_id,
    "aud": token_endpoint,
    "exp": now + timedelta(minutes=5),
    "jti": str(uuid.uuid4())
}
assertion = jwt.encode(claims, private_key, algorithm="RS384")

# Request token
response = requests.post(token_endpoint, data={
    "grant_type": "client_credentials",
    "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
    "client_assertion": assertion,
    "scope": "system/*.read"
})
access_token = response.json()["access_token"]
```

## Security considerations

- Use TLS 1.2 or higher for all connections
- Rotate keys periodically
- Implement token caching to avoid excessive token requests
- Never expose private keys in logs or error messages
- Validate server certificates

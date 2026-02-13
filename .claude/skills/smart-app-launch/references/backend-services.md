# Backend services

Backend services enable server-to-server FHIR access without user interaction. Used for bulk data operations, analytics pipelines, system integrations, and automated monitoring.

## Table of contents

1. [Overview](#overview)
2. [Registration](#registration)
3. [JWT assertion](#jwt-assertion)
4. [Token request](#token-request)
5. [Token response](#token-response)
6. [Example implementation](#example-implementation)

## Overview

Backend services use the OAuth 2.0 client credentials grant with JWT-based client authentication:

1. Register client with JWKS URL or public keys
2. Generate signed JWT assertion
3. POST to token endpoint with assertion
4. Use access token to call FHIR API

Key constraints:

- Only `system/` scopes are available
- Refresh tokens are not issued
- Access tokens should be short-lived (5 minutes recommended)

## Registration

Register your backend service with the authorization server, providing:

| Item             | Description                                |
| ---------------- | ------------------------------------------ |
| `client_id`      | Unique identifier for your service         |
| `jwks_uri`       | URL to your public JWKS (preferred)        |
| `jwks`           | Direct public key submission (discouraged) |
| Permitted scopes | `system/` scopes your service needs        |

### JWKS hosting (preferred)

Host your public keys at a TLS-protected URL:

```json
{
    "keys": [
        {
            "kty": "RSA",
            "kid": "key-2024-01",
            "use": "sig",
            "alg": "RS384",
            "n": "0vx7agoebGcQSuu...",
            "e": "AQAB"
        }
    ]
}
```

This enables key rotation without re-registering with the authorization server.

## JWT assertion

Create a signed JWT to authenticate your service.

### Header

```json
{
    "alg": "RS384",
    "typ": "JWT",
    "kid": "key-2024-01"
}
```

| Field | Required | Description                             |
| ----- | -------- | --------------------------------------- |
| `alg` | Yes      | Signing algorithm: `RS384` or `ES384`   |
| `typ` | Yes      | Fixed value: `JWT`                      |
| `kid` | Yes      | Key ID matching a key in your JWKS      |
| `jku` | Optional | URL to JWKS (must match registered URL) |

### Payload

```json
{
    "iss": "my-backend-service",
    "sub": "my-backend-service",
    "aud": "https://ehr.example.com/auth/token",
    "exp": 1735084800,
    "jti": "f9d7c8b6-1234-5678-9abc-def012345678"
}
```

| Claim | Required | Description                               |
| ----- | -------- | ----------------------------------------- |
| `iss` | Yes      | Client ID (issuer)                        |
| `sub` | Yes      | Client ID (subject) - same as `iss`       |
| `aud` | Yes      | Token endpoint URL (audience)             |
| `exp` | Yes      | Expiration: max 5 minutes from now        |
| `jti` | Yes      | Unique token ID (prevents replay attacks) |

### Signing algorithms

Both RS384 (RSA) and ES384 (ECDSA) must be supported:

| Algorithm | Key type      | Use case                      |
| --------- | ------------- | ----------------------------- |
| RS384     | RSA 2048+ bit | Widely supported, larger keys |
| ES384     | ECDSA P-384   | Smaller keys, faster signing  |

## Token request

POST to the token endpoint:

```
POST /auth/token HTTP/1.1
Host: ehr.example.com
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&
scope=system%2FPatient.rs+system%2FObservation.rs&
client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer&
client_assertion=eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCIsImtpZCI6ImtleS0yMDI0LTAxIn0...
```

| Parameter               | Value                                                    |
| ----------------------- | -------------------------------------------------------- |
| `grant_type`            | `client_credentials`                                     |
| `scope`                 | Space-separated `system/` scopes                         |
| `client_assertion_type` | `urn:ietf:params:oauth:client-assertion-type:jwt-bearer` |
| `client_assertion`      | Signed JWT                                               |

## Token response

```json
{
    "access_token": "eyJhbGciOiJSUzI1NiJ9...",
    "token_type": "bearer",
    "expires_in": 300,
    "scope": "system/Patient.rs system/Observation.rs"
}
```

| Field          | Description                          |
| -------------- | ------------------------------------ |
| `access_token` | Bearer token for API access          |
| `token_type`   | Always `bearer`                      |
| `expires_in`   | Lifetime in seconds (should be ≤300) |
| `scope`        | Granted scopes                       |

No refresh token is issued. Request a new token when the current one expires.

## Example implementation

### Python with PyJWT

```python
import jwt
import uuid
import time
import requests
from cryptography.hazmat.primitives import serialization

def get_backend_token(
    token_url: str,
    client_id: str,
    private_key_path: str,
    key_id: str,
    scopes: list[str]
) -> str:
    """
    Obtain an access token for backend services.

    @param token_url: Authorization server token endpoint.
    @param client_id: Registered client identifier.
    @param private_key_path: Path to PEM-encoded private key.
    @param key_id: Key ID matching the public key in JWKS.
    @param scopes: List of system scopes to request.
    @returns: Access token string.
    """
    # Load private key
    with open(private_key_path, 'rb') as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    # Create JWT assertion
    now = int(time.time())
    claims = {
        'iss': client_id,
        'sub': client_id,
        'aud': token_url,
        'exp': now + 300,  # 5 minutes
        'jti': str(uuid.uuid4())
    }

    assertion = jwt.encode(
        claims,
        private_key,
        algorithm='RS384',
        headers={'kid': key_id, 'typ': 'JWT'}
    )

    # Request token
    response = requests.post(token_url, data={
        'grant_type': 'client_credentials',
        'scope': ' '.join(scopes),
        'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
        'client_assertion': assertion
    })
    response.raise_for_status()

    return response.json()['access_token']


# Usage
token = get_backend_token(
    token_url='https://ehr.example.com/auth/token',
    client_id='my-backend-service',
    private_key_path='/path/to/private-key.pem',
    key_id='key-2024-01',
    scopes=['system/Patient.rs', 'system/Observation.rs']
)

# Use token for FHIR API calls
headers = {'Authorization': f'Bearer {token}'}
response = requests.get('https://ehr.example.com/fhir/Patient', headers=headers)
```

### Java with Nimbus JOSE

```java
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.*;
import java.util.*;

public class BackendServiceAuth {

    public String getAccessToken(
            String tokenUrl,
            String clientId,
            RSAKey privateKey,
            List<String> scopes) throws Exception {

        // Build JWT claims
        Date now = new Date();
        Date exp = new Date(now.getTime() + 300_000); // 5 minutes

        JWTClaimsSet claims = new JWTClaimsSet.Builder()
            .issuer(clientId)
            .subject(clientId)
            .audience(tokenUrl)
            .expirationTime(exp)
            .jwtID(UUID.randomUUID().toString())
            .build();

        // Sign JWT
        JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.RS384)
            .keyID(privateKey.getKeyID())
            .type(JOSEObjectType.JWT)
            .build();

        SignedJWT jwt = new SignedJWT(header, claims);
        jwt.sign(new RSASSASigner(privateKey));
        String assertion = jwt.serialize();

        // Request token (using your preferred HTTP client)
        Map<String, String> params = new HashMap<>();
        params.put("grant_type", "client_credentials");
        params.put("scope", String.join(" ", scopes));
        params.put("client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer");
        params.put("client_assertion", assertion);

        // POST to tokenUrl with params...
        return responseToken;
    }
}
```

## Server-side validation

Authorization servers validate backend service requests by:

1. Verifying JWT signature against registered JWKS
2. Checking `iss` and `sub` match the registered client ID
3. Confirming `aud` matches the token endpoint URL
4. Validating `exp` is within 5 minutes
5. Ensuring `jti` has not been used before (replay prevention)
6. Confirming requested scopes are pre-authorised

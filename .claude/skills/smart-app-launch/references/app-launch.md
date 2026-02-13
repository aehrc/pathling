# App launch flows

## Table of contents

1. [EHR launch](#ehr-launch)
2. [Standalone launch](#standalone-launch)
3. [Authorization request parameters](#authorization-request-parameters)
4. [Authorization response](#authorization-response)
5. [Token request](#token-request)
6. [Token response](#token-response)
7. [Refresh tokens](#refresh-tokens)
8. [OpenID Connect integration](#openid-connect-integration)

## EHR launch

Used when an app is launched from within an active EHR session (e.g., clinician clicks app link in patient chart).

### Launch sequence

1. EHR redirects to app's launch URL with parameters:

```
https://app.example.com/launch?iss=https%3A%2F%2Fehr.example.com%2Ffhir&launch=xyz123
```

| Parameter | Description                               |
| --------- | ----------------------------------------- |
| `iss`     | FHIR server base URL                      |
| `launch`  | Opaque identifier for this launch context |

2. App fetches `.well-known/smart-configuration` from `iss`
3. App redirects to `authorization_endpoint` with `launch` parameter echoed back
4. EHR authenticates user (may be SSO), presents consent screen
5. EHR redirects to app's `redirect_uri` with authorization code
6. App exchanges code for tokens

### EHR launch authorization request

```
GET https://ehr.example.com/auth/authorize?
  response_type=code&
  client_id=my-app&
  redirect_uri=https%3A%2F%2Fapp.example.com%2Fcallback&
  scope=patient%2F*.rs+openid+fhirUser&
  state=abc123xyz&
  aud=https%3A%2F%2Fehr.example.com%2Ffhir&
  code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM&
  code_challenge_method=S256&
  launch=xyz123
```

## Standalone launch

Used when an app is launched independently by a user (e.g., from a mobile home screen or web bookmark).

### Launch sequence

1. User opens app directly
2. App fetches `.well-known/smart-configuration` from configured FHIR server
3. App redirects to `authorization_endpoint` (no `launch` parameter)
4. App may request `launch/patient` scope to ask EHR for patient selection
5. EHR authenticates user, allows patient selection if requested, presents consent
6. EHR redirects to app with authorization code
7. App exchanges code for tokens (which include patient context if granted)

### Standalone launch authorization request

```
GET https://ehr.example.com/auth/authorize?
  response_type=code&
  client_id=my-app&
  redirect_uri=https%3A%2F%2Fapp.example.com%2Fcallback&
  scope=launch%2Fpatient+patient%2F*.rs+openid+fhirUser&
  state=abc123xyz&
  aud=https%3A%2F%2Fehr.example.com%2Ffhir&
  code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM&
  code_challenge_method=S256
```

## Authorization request parameters

| Parameter               | Required | Description                                                  |
| ----------------------- | -------- | ------------------------------------------------------------ |
| `response_type`         | Yes      | Fixed value: `code`                                          |
| `client_id`             | Yes      | App's registered identifier                                  |
| `redirect_uri`          | Yes      | Must match pre-registered URI exactly                        |
| `scope`                 | Yes      | Space-separated list of requested scopes                     |
| `state`                 | Yes      | Minimum 122 bits of entropy for CSRF prevention              |
| `aud`                   | Yes      | FHIR server base URL (audience binding)                      |
| `code_challenge`        | Yes      | PKCE challenge (SHA-256 hash of verifier, base64url encoded) |
| `code_challenge_method` | Yes      | Fixed value: `S256`                                          |
| `launch`                | EHR only | Echo back the launch parameter from EHR                      |

### PKCE implementation

Generate a cryptographically random `code_verifier` (43-128 characters from `[A-Za-z0-9-._~]`):

```python
import secrets
import hashlib
import base64

code_verifier = secrets.token_urlsafe(32)
code_challenge = base64.urlsafe_b64encode(
    hashlib.sha256(code_verifier.encode()).digest()
).decode().rstrip('=')
```

## Authorization response

On success, EHR redirects to `redirect_uri`:

```
https://app.example.com/callback?code=SplxlOBeZQQYbYS6WxSbIA&state=abc123xyz
```

App must validate that `state` matches the value sent in the request.

### Error response

```
https://app.example.com/callback?error=access_denied&error_description=User+denied+access&state=abc123xyz
```

Common error codes: `invalid_request`, `unauthorized_client`, `access_denied`, `unsupported_response_type`, `invalid_scope`, `server_error`.

## Token request

POST to `token_endpoint` with `Content-Type: application/x-www-form-urlencoded`:

### Public client

```
POST /token HTTP/1.1
Host: ehr.example.com
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code&
code=SplxlOBeZQQYbYS6WxSbIA&
redirect_uri=https%3A%2F%2Fapp.example.com%2Fcallback&
client_id=my-app&
code_verifier=dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk
```

### Confidential client (symmetric)

Include `Authorization` header with base64-encoded `client_id:client_secret`:

```
POST /token HTTP/1.1
Host: ehr.example.com
Authorization: Basic bXktYXBwOm15LXNlY3JldA==
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code&
code=SplxlOBeZQQYbYS6WxSbIA&
redirect_uri=https%3A%2F%2Fapp.example.com%2Fcallback&
code_verifier=dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk
```

### Confidential client (asymmetric)

Include JWT assertion:

```
POST /token HTTP/1.1
Host: ehr.example.com
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code&
code=SplxlOBeZQQYbYS6WxSbIA&
redirect_uri=https%3A%2F%2Fapp.example.com%2Fcallback&
code_verifier=dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk&
client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer&
client_assertion=eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCIsImtpZCI6ImtleS0xIn0...
```

## Token response

```json
{
    "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "Bearer",
    "expires_in": 3600,
    "scope": "patient/Observation.rs patient/Patient.rs openid fhirUser",
    "refresh_token": "tGzv3JOkF0XG5Qx2TlKWIA",
    "id_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
    "patient": "123",
    "encounter": "456",
    "need_patient_banner": true,
    "smart_style_url": "https://ehr.example.com/styles/smart-v1.json"
}
```

| Field                 | Required    | Description                                |
| --------------------- | ----------- | ------------------------------------------ |
| `access_token`        | Yes         | Bearer token for API access                |
| `token_type`          | Yes         | Fixed value: `Bearer`                      |
| `expires_in`          | Recommended | Lifetime in seconds                        |
| `scope`               | Yes         | Granted scopes (may differ from requested) |
| `refresh_token`       | Optional    | For obtaining new access tokens            |
| `id_token`            | Optional    | If `openid`/`fhirUser` requested           |
| `patient`             | Optional    | Patient ID from launch context             |
| `encounter`           | Optional    | Encounter ID from launch context           |
| `fhirContext`         | Optional    | Array of additional resource references    |
| `need_patient_banner` | Optional    | Whether app should display patient banner  |
| `smart_style_url`     | Optional    | URL to EHR styling preferences             |

## Refresh tokens

### Online vs offline access

| Scope            | Behaviour                                          |
| ---------------- | -------------------------------------------------- |
| `online_access`  | Refresh token valid only while user session active |
| `offline_access` | Refresh token persists beyond user session         |

### Refresh request

```
POST /token HTTP/1.1
Host: ehr.example.com
Content-Type: application/x-www-form-urlencoded

grant_type=refresh_token&
refresh_token=tGzv3JOkF0XG5Qx2TlKWIA&
scope=patient/Observation.rs
```

The `scope` parameter is optional and must be a subset of originally granted scopes.

### Refresh response

Same structure as initial token response. May include a new refresh token (the previous one should then be discarded).

### Security requirements

- Refresh tokens are bound to the same `client_id`
- Each refresh token should be used only once
- If a refresh token is reused, authorization must be revoked (OAuth 2.1)

## OpenID Connect integration

When `openid` and/or `fhirUser` scopes are requested, the token response includes an `id_token`.

### ID token claims

| Claim      | Description                                               |
| ---------- | --------------------------------------------------------- |
| `iss`      | Issuer URL                                                |
| `sub`      | Subject identifier (opaque user ID)                       |
| `aud`      | Client ID                                                 |
| `exp`      | Expiration timestamp                                      |
| `iat`      | Issued at timestamp                                       |
| `fhirUser` | FHIR resource URL for the user (e.g., `Practitioner/123`) |

### Validating the ID token

1. Extract `iss` claim
2. Fetch `{iss}/.well-known/openid-configuration`
3. Retrieve JWKS from `jwks_uri`
4. Validate signature using public key
5. Verify `aud` matches your `client_id`
6. Check `exp` timestamp
7. Use `fhirUser` to fetch user's FHIR resource

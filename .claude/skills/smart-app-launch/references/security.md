# Security best practices

Security requirements and recommendations for SMART App Launch implementations.

## Table of contents

1. [Transport security](#transport-security)
2. [PKCE requirements](#pkce-requirements)
3. [State parameter](#state-parameter)
4. [Token security](#token-security)
5. [Redirect URI validation](#redirect-uri-validation)
6. [Client security](#client-security)
7. [Common vulnerabilities](#common-vulnerabilities)

## Transport security

### TLS requirements

- All authorization and token requests must use TLS 1.2 or later
- All FHIR API calls with bearer tokens must use HTTPS
- Verify server certificates; do not disable certificate validation

### Header requirements

Token responses must include:

```
Cache-Control: no-store
Pragma: no-cache
```

Apps must not cache token responses in shared storage.

## PKCE requirements

PKCE (Proof Key for Code Exchange) is mandatory for all authorization code flows.

### Implementation

1. Generate cryptographically random `code_verifier` (43-128 characters)
2. Compute `code_challenge` as SHA-256 hash, base64url-encoded
3. Include `code_challenge` and `code_challenge_method=S256` in authorization request
4. Include `code_verifier` in token request

```python
import secrets
import hashlib
import base64

def generate_pkce():
    """
    Generate PKCE code verifier and challenge.

    @returns: Tuple of (code_verifier, code_challenge).
    """
    # Generate 32 random bytes = 43 base64url characters
    code_verifier = secrets.token_urlsafe(32)

    # SHA-256 hash, base64url encode, strip padding
    digest = hashlib.sha256(code_verifier.encode('ascii')).digest()
    code_challenge = base64.urlsafe_b64encode(digest).decode('ascii').rstrip('=')

    return code_verifier, code_challenge
```

### Prohibited methods

The `plain` code challenge method must not be supported or used. Only `S256` is permitted.

## State parameter

The `state` parameter prevents CSRF attacks.

### Requirements

- Minimum 122 bits of entropy (approximately 20+ random characters)
- Must be unpredictable to attackers
- Store locally before authorization request
- Validate exactly matches upon callback

```python
import secrets

def generate_state():
    """
    Generate cryptographically secure state parameter.

    @returns: State string with sufficient entropy.
    """
    # 24 bytes = 192 bits of entropy
    return secrets.token_urlsafe(24)

def validate_state(received: str, expected: str) -> bool:
    """
    Validate state parameter using constant-time comparison.

    @param received: State from callback.
    @param expected: State stored before authorization.
    @returns: True if states match.
    """
    import hmac
    return hmac.compare_digest(received, expected)
```

### Storage

- SPA: Store in session storage (not local storage)
- Native app: Store in memory or secure enclave
- Server-side: Store in session with CSRF token

## Token security

### Access token handling

| Requirement  | Description                                              |
| ------------ | -------------------------------------------------------- |
| Lifetime     | Should not exceed 1 hour; 5 minutes for backend services |
| Storage      | App-specific storage only; never in cookies over HTTP    |
| Transmission | Only over TLS; only in Authorization header              |
| Logging      | Never log full access tokens                             |

### Refresh token handling

| Requirement | Description                                          |
| ----------- | ---------------------------------------------------- |
| Single use  | Each refresh token should be used only once          |
| Revocation  | If reused, all tokens for that grant must be revoked |
| Storage     | Secure storage only (keychain, credential manager)   |
| Binding     | Bound to original client_id                          |

### Token validation (resource servers)

1. Extract token from `Authorization: Bearer {token}` header
2. Validate token signature (if JWT) or introspect
3. Check `exp` timestamp has not passed
4. Verify `aud` matches this resource server
5. Verify scopes permit the requested operation

## Redirect URI validation

### Registration

- All redirect URIs must be pre-registered
- Use exact string matching (no wildcards in path)
- HTTPS required for production (except localhost for development)

### Validation rules

| Rule              | Description                           |
| ----------------- | ------------------------------------- |
| Exact match       | URI must match registered URI exactly |
| No fragments      | Fragment identifiers (#) not allowed  |
| No open redirects | Never redirect to user-provided URLs  |

### Mobile app considerations

- Use claimed HTTPS URIs (Universal Links / App Links) when possible
- Custom schemes (`myapp://`) are less secure but sometimes necessary
- Register specific paths, not just scheme

## Client security

### Public clients

- Cannot securely store secrets
- Rely entirely on PKCE for authorization code security
- May receive shorter-lived tokens
- Should use platform-specific secure storage for tokens

### Confidential clients

- Secrets must never appear in client-side code
- Use environment variables or secret managers
- Implement secret rotation procedures
- Prefer asymmetric authentication over shared secrets

### Backend services

- Private keys must be stored in HSM or secure key storage
- JWTs must have unique `jti` values
- JWT lifetime must not exceed 5 minutes
- Implement key rotation via JWKS

## Common vulnerabilities

### Authorization code interception

**Risk**: Attacker intercepts authorization code before legitimate app.

**Mitigation**: PKCE ensures only the app with the `code_verifier` can exchange the code.

### Token theft via XSS

**Risk**: Cross-site scripting steals tokens from browser storage.

**Mitigations**:

- Store tokens in memory or HttpOnly cookies
- Implement Content Security Policy
- Sanitise all user input
- Use short-lived access tokens

### CSRF attacks

**Risk**: Attacker tricks user into authorising malicious request.

**Mitigation**: State parameter with sufficient entropy, validated on callback.

### Open redirect

**Risk**: Attacker uses app's redirect URI to phish users.

**Mitigation**: Exact redirect URI matching, no user-controlled redirects.

### Token leakage via referrer

**Risk**: Access token leaks in URL fragment via Referrer header.

**Mitigations**:

- Use POST for token delivery where possible
- Implement `Referrer-Policy: no-referrer`
- Avoid tokens in URLs

### Replay attacks

**Risk**: Attacker reuses captured JWT assertions.

**Mitigations**:

- Unique `jti` claim in every JWT
- Server tracks used `jti` values within expiration window
- Short JWT lifetime (≤5 minutes)

### Insufficient scope validation

**Risk**: App requests or uses more permissions than necessary.

**Mitigations**:

- Request minimum necessary scopes
- Validate granted scopes before use
- Handle scope negotiation gracefully

## Security checklist

### App developers

- [ ] PKCE implemented with S256
- [ ] State parameter with 122+ bits entropy
- [ ] State validated on callback using constant-time comparison
- [ ] Tokens stored securely (not in local storage)
- [ ] Refresh tokens used only once
- [ ] Minimum necessary scopes requested
- [ ] All requests over HTTPS
- [ ] Redirect URI exactly matches registration

### Server implementers

- [ ] Only S256 PKCE method supported
- [ ] State parameter required
- [ ] Exact redirect URI matching
- [ ] Access tokens expire within 1 hour
- [ ] Refresh token rotation implemented
- [ ] Token introspection available
- [ ] Token revocation available
- [ ] Cache-Control headers set on token responses
- [ ] `jti` replay prevention for JWT assertions

# Client authentication

SMART App Launch supports three client authentication methods based on the client's capability to protect secrets.

## Table of contents

1. [Public clients](#public-clients)
2. [Confidential clients (symmetric)](#confidential-clients-symmetric)
3. [Confidential clients (asymmetric)](#confidential-clients-asymmetric)
4. [Choosing an authentication method](#choosing-an-authentication-method)

## Public clients

Public clients cannot securely store secrets (e.g., single-page apps, native mobile apps).

### Characteristics

- No client secret
- Relies on PKCE for security
- Registered with `client_id` only
- Tokens may have shorter lifetimes

### Token request

```
POST /token HTTP/1.1
Host: ehr.example.com
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code&
code=SplxlOBeZQQYbYS6WxSbIA&
redirect_uri=https%3A%2F%2Fapp.example.com%2Fcallback&
client_id=my-public-app&
code_verifier=dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk
```

The `code_verifier` proves the client that initiated the authorization request is the same one requesting the token.

### Security considerations

- PKCE is mandatory
- Redirect URI must be pre-registered and validated exactly
- Consider using claimed HTTPS redirect URIs on mobile

## Confidential clients (symmetric)

Confidential clients with backend servers can store a shared secret.

### Characteristics

- Pre-shared client secret
- Secret must be protected server-side
- Not suitable for native/SPA apps

### Token request with Basic authentication

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

The `Authorization` header contains base64-encoded `client_id:client_secret`.

### Token request with POST body

Alternative: include credentials in the request body:

```
POST /token HTTP/1.1
Host: ehr.example.com
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code&
code=SplxlOBeZQQYbYS6WxSbIA&
redirect_uri=https%3A%2F%2Fapp.example.com%2Fcallback&
code_verifier=dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk&
client_id=my-app&
client_secret=my-secret
```

### Security considerations

- Secrets must never be exposed in client-side code
- Rotate secrets periodically
- Use environment variables or secret managers
- Monitor for secret exposure in logs

## Confidential clients (asymmetric)

Uses public key cryptography; client proves identity by signing JWTs.

### Characteristics

- No shared secret
- Client registers public keys or JWKS URL
- More secure than symmetric
- Required for backend services

### Key registration

Option 1 (preferred): Register JWKS URL

```
jwks_uri: https://app.example.com/.well-known/jwks.json
```

Option 2: Register keys directly (discourages key rotation)

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

### JWKS format

RSA key:

```json
{
    "kty": "RSA",
    "kid": "key-2024-01",
    "use": "sig",
    "alg": "RS384",
    "n": "<base64url-encoded modulus>",
    "e": "AQAB"
}
```

ECDSA key:

```json
{
    "kty": "EC",
    "kid": "key-2024-02",
    "use": "sig",
    "alg": "ES384",
    "crv": "P-384",
    "x": "<base64url-encoded x coordinate>",
    "y": "<base64url-encoded y coordinate>"
}
```

### JWT assertion structure

Header:

```json
{
    "alg": "RS384",
    "typ": "JWT",
    "kid": "key-2024-01"
}
```

Payload:

```json
{
    "iss": "my-app",
    "sub": "my-app",
    "aud": "https://ehr.example.com/auth/token",
    "exp": 1735084800,
    "jti": "unique-token-id"
}
```

### Token request

```
POST /token HTTP/1.1
Host: ehr.example.com
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code&
code=SplxlOBeZQQYbYS6WxSbIA&
redirect_uri=https%3A%2F%2Fapp.example.com%2Fcallback&
code_verifier=dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk&
client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer&
client_assertion=eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCIsImtpZCI6ImtleS0yMDI0LTAxIn0...
```

### Key rotation

1. Generate new key pair
2. Add new public key to JWKS (keep old key)
3. Start signing with new key
4. After transition period, remove old key from JWKS

### Security considerations

- Private keys must never leave secure storage
- Use HSM or secure key storage in production
- Implement key rotation schedule
- JWTs must be short-lived (≤5 minutes)
- Each JWT must have unique `jti` for replay prevention

## Choosing an authentication method

| Scenario                 | Recommended method                  |
| ------------------------ | ----------------------------------- |
| Single-page app (SPA)    | Public client                       |
| Native mobile app        | Public client                       |
| Server-rendered web app  | Confidential (asymmetric preferred) |
| Backend service          | Confidential (asymmetric required)  |
| Desktop app with backend | Confidential (asymmetric preferred) |

### Decision factors

| Factor                  | Public   | Symmetric | Asymmetric         |
| ----------------------- | -------- | --------- | ------------------ |
| Secret storage required | No       | Yes       | Private key        |
| Setup complexity        | Low      | Low       | Medium             |
| Key rotation            | N/A      | Manual    | Automated via JWKS |
| Backend services        | No       | No        | Yes                |
| Security level          | Baseline | Medium    | Highest            |

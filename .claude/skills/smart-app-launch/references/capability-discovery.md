# Capability discovery

SMART-enabled FHIR servers publish their capabilities at a well-known endpoint, enabling apps to discover authorization endpoints and supported features automatically.

## Table of contents

1. [Discovery endpoint](#discovery-endpoint)
2. [Required fields](#required-fields)
3. [Conditional fields](#conditional-fields)
4. [Optional fields](#optional-fields)
5. [Capabilities](#capabilities)
6. [Example configurations](#example-configurations)

## Discovery endpoint

Fetch server capabilities from:

```
GET {fhir-base-url}/.well-known/smart-configuration
Accept: application/json
```

Example:

```
GET https://ehr.example.com/fhir/.well-known/smart-configuration
```

Response is always JSON regardless of `Accept` header.

## Required fields

| Field                              | Type   | Description                                         |
| ---------------------------------- | ------ | --------------------------------------------------- |
| `issuer`                           | string | FHIR server base URL (must match token `iss` claim) |
| `authorization_endpoint`           | string | URL for authorization requests                      |
| `token_endpoint`                   | string | URL for token requests                              |
| `capabilities`                     | array  | Supported SMART capabilities                        |
| `code_challenge_methods_supported` | array  | PKCE methods (must include `S256`)                  |
| `grant_types_supported`            | array  | Supported grant types                               |

## Conditional fields

| Field      | Required when                  | Description                 |
| ---------- | ------------------------------ | --------------------------- |
| `jwks_uri` | `sso-openid-connect` supported | URL to server's public keys |

## Optional fields

| Field                                   | Type   | Description                     |
| --------------------------------------- | ------ | ------------------------------- |
| `scopes_supported`                      | array  | Available scopes                |
| `response_types_supported`              | array  | OAuth response types            |
| `introspection_endpoint`                | string | Token introspection URL         |
| `revocation_endpoint`                   | string | Token revocation URL            |
| `management_endpoint`                   | string | User access management URL      |
| `registration_endpoint`                 | string | Dynamic client registration URL |
| `token_endpoint_auth_methods_supported` | array  | Client auth methods             |
| `associated_endpoints`                  | array  | Related FHIR endpoints          |

## Capabilities

### Launch modes

| Capability          | Description                                             |
| ------------------- | ------------------------------------------------------- |
| `launch-ehr`        | Supports EHR launch (app launched from EHR)             |
| `launch-standalone` | Supports standalone launch (app launched independently) |

### Client types

| Capability                       | Description                             |
| -------------------------------- | --------------------------------------- |
| `client-public`                  | Supports public clients (no secret)     |
| `client-confidential-symmetric`  | Supports shared secret authentication   |
| `client-confidential-asymmetric` | Supports private key JWT authentication |

### Permission models

| Capability           | Description                                             |
| -------------------- | ------------------------------------------------------- |
| `permission-patient` | Supports `patient/` scopes                              |
| `permission-user`    | Supports `user/` scopes                                 |
| `permission-offline` | Supports `offline_access` (persistent refresh tokens)   |
| `permission-online`  | Supports `online_access` (session-bound refresh tokens) |
| `permission-v1`      | Supports SMART v1 scope syntax                          |
| `permission-v2`      | Supports SMART v2 scope syntax                          |

### Context capabilities

EHR launch context:

| Capability              | Description                              |
| ----------------------- | ---------------------------------------- |
| `context-ehr-patient`   | Patient context provided at EHR launch   |
| `context-ehr-encounter` | Encounter context provided at EHR launch |

Standalone launch context:

| Capability                     | Description                                 |
| ------------------------------ | ------------------------------------------- |
| `context-standalone-patient`   | Patient selection available in standalone   |
| `context-standalone-encounter` | Encounter selection available in standalone |

### Identity

| Capability           | Description                               |
| -------------------- | ----------------------------------------- |
| `sso-openid-connect` | Supports OpenID Connect for user identity |

### Experimental

| Capability        | Description                    |
| ----------------- | ------------------------------ |
| `smart-app-state` | Supports app state persistence |

## Example configurations

### Patient portal (standalone only)

```json
{
    "issuer": "https://portal.example.com/fhir",
    "authorization_endpoint": "https://portal.example.com/auth/authorize",
    "token_endpoint": "https://portal.example.com/auth/token",
    "grant_types_supported": ["authorization_code"],
    "code_challenge_methods_supported": ["S256"],
    "capabilities": [
        "launch-standalone",
        "client-public",
        "permission-patient",
        "permission-v2",
        "context-standalone-patient",
        "sso-openid-connect"
    ],
    "scopes_supported": [
        "openid",
        "fhirUser",
        "launch/patient",
        "patient/Patient.rs",
        "patient/Observation.rs",
        "patient/Condition.rs"
    ]
}
```

### Full EHR (EHR launch + backend services)

```json
{
    "issuer": "https://ehr.example.com/fhir",
    "authorization_endpoint": "https://ehr.example.com/auth/authorize",
    "token_endpoint": "https://ehr.example.com/auth/token",
    "introspection_endpoint": "https://ehr.example.com/auth/introspect",
    "revocation_endpoint": "https://ehr.example.com/auth/revoke",
    "jwks_uri": "https://ehr.example.com/.well-known/jwks.json",
    "grant_types_supported": ["authorization_code", "client_credentials"],
    "code_challenge_methods_supported": ["S256"],
    "token_endpoint_auth_methods_supported": [
        "client_secret_basic",
        "client_secret_post",
        "private_key_jwt"
    ],
    "capabilities": [
        "launch-ehr",
        "launch-standalone",
        "client-public",
        "client-confidential-symmetric",
        "client-confidential-asymmetric",
        "permission-patient",
        "permission-user",
        "permission-offline",
        "permission-online",
        "permission-v1",
        "permission-v2",
        "context-ehr-patient",
        "context-ehr-encounter",
        "context-standalone-patient",
        "sso-openid-connect"
    ],
    "scopes_supported": [
        "openid",
        "fhirUser",
        "launch",
        "launch/patient",
        "launch/encounter",
        "offline_access",
        "online_access",
        "patient/*.cruds",
        "user/*.cruds",
        "system/*.rs"
    ]
}
```

### Backend services only

```json
{
    "issuer": "https://bulk.example.com/fhir",
    "token_endpoint": "https://bulk.example.com/auth/token",
    "grant_types_supported": ["client_credentials"],
    "code_challenge_methods_supported": ["S256"],
    "token_endpoint_auth_methods_supported": ["private_key_jwt"],
    "capabilities": ["client-confidential-asymmetric", "permission-v2"],
    "scopes_supported": [
        "system/Patient.rs",
        "system/Observation.rs",
        "system/Encounter.rs",
        "system/Condition.rs",
        "system/*.$export"
    ]
}
```

## Discovery workflow

```python
import requests

def discover_smart_config(fhir_base_url: str) -> dict:
    """
    Fetch SMART configuration from a FHIR server.

    @param fhir_base_url: Base URL of the FHIR server.
    @returns: SMART configuration dictionary.
    @throws: requests.HTTPError if discovery fails.
    """
    url = f"{fhir_base_url.rstrip('/')}/.well-known/smart-configuration"
    response = requests.get(url, headers={'Accept': 'application/json'})
    response.raise_for_status()
    return response.json()


def check_capability(config: dict, capability: str) -> bool:
    """
    Check if a SMART server supports a specific capability.

    @param config: SMART configuration dictionary.
    @param capability: Capability string to check.
    @returns: True if capability is supported.
    """
    return capability in config.get('capabilities', [])


# Usage
config = discover_smart_config('https://ehr.example.com/fhir')

if check_capability(config, 'launch-standalone'):
    auth_url = config['authorization_endpoint']
    # Proceed with standalone launch...

if check_capability(config, 'client-confidential-asymmetric'):
    # Can use private key JWT authentication
    pass
```

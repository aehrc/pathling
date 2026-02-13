# Scopes and permissions

## Table of contents

1. [Scope syntax](#scope-syntax)
2. [Context levels](#context-levels)
3. [Permission codes](#permission-codes)
4. [Resource scopes](#resource-scopes)
5. [Launch context scopes](#launch-context-scopes)
6. [OpenID Connect scopes](#openid-connect-scopes)
7. [Access duration scopes](#access-duration-scopes)
8. [Scope constraints](#scope-constraints)
9. [Wildcard scopes](#wildcard-scopes)
10. [Examples](#examples)

## Scope syntax

SMART v2 scope syntax:

```
{context}/{resourceType}.{permissions}[?{constraints}]
```

| Component      | Description                            |
| -------------- | -------------------------------------- |
| `context`      | `patient`, `user`, or `system`         |
| `resourceType` | FHIR resource type or `*` for all      |
| `permissions`  | Combination of `c`, `r`, `u`, `d`, `s` |
| `constraints`  | Optional search parameter filters      |

## Context levels

### patient

Access to data about a specific patient (determined by launch context).

```
patient/Observation.rs    # Read/search observations for launch patient
patient/MedicationRequest.cruds  # Full access to patient's meds
```

### user

Access to all data the authenticated user is permitted to see.

```
user/Patient.rs          # Read/search any patient user can access
user/Encounter.cruds     # Full access to encounters user can access
```

### system

Backend service access without user context. Used only with `client_credentials` grant.

```
system/Patient.rs        # Read/search all patients
system/Observation.rs    # Read/search all observations
```

## Permission codes

| Code | Permission | HTTP methods |
| ---- | ---------- | ------------ |
| `c`  | Create     | POST         |
| `r`  | Read       | GET (by ID)  |
| `u`  | Update     | PUT, PATCH   |
| `d`  | Delete     | DELETE       |
| `s`  | Search     | GET (search) |

Common combinations:

- `.rs` - Read and search (read-only)
- `.crus` - Create, read, update, search (no delete)
- `.cruds` - Full access

## Resource scopes

Request access to specific FHIR resource types:

```
patient/Patient.rs
patient/Observation.rs
patient/Condition.rs
patient/MedicationRequest.cruds
user/Practitioner.rs
system/Organization.rs
```

## Launch context scopes

Request that the server provide launch context:

| Scope                   | Description                                   |
| ----------------------- | --------------------------------------------- |
| `launch`                | EHR launch context (used with EHR launch)     |
| `launch/patient`        | Request patient selection (standalone launch) |
| `launch/encounter`      | Request encounter selection                   |
| `launch/{ResourceType}` | Request specific resource context             |

These scopes do not grant data access; they only request context to be provided.

## OpenID Connect scopes

| Scope      | Description                                    |
| ---------- | ---------------------------------------------- |
| `openid`   | Request ID token with user identity            |
| `fhirUser` | Include FHIR resource URL for user in ID token |
| `profile`  | Standard OIDC profile claims                   |
| `email`    | Standard OIDC email claim                      |

## Access duration scopes

| Scope            | Description                                         |
| ---------------- | --------------------------------------------------- |
| `online_access`  | Refresh token valid only during active user session |
| `offline_access` | Refresh token persists beyond user session          |

If neither is requested, server may or may not provide refresh tokens.

## Scope constraints

SMART v2 allows search parameter constraints to narrow scope:

```
patient/Observation.rs?category=http://terminology.hl7.org/CodeSystem/observation-category|laboratory
```

This limits access to observations with the specified category.

### Constraint syntax

```
?{searchParam}={value}[&{searchParam}={value}...]
```

Values must be URL-encoded. Multiple constraints use `&`.

### Examples

```
# Only vital signs observations
patient/Observation.rs?category=http://terminology.hl7.org/CodeSystem/observation-category|vital-signs

# Only active medication requests
patient/MedicationRequest.rs?status=active

# Appointments for specific practitioner
user/Appointment.rs?actor=Practitioner/123
```

## Wildcard scopes

Use `*` to request access to all resource types:

```
patient/*.rs       # Read/search all patient data
user/*.cruds       # Full access to all user-accessible data
system/*.rs        # Read/search all data (backend service)
```

Wildcard grants access to all current and future FHIR resources the server exposes.

## Examples

### Minimal patient portal

```
launch/patient
patient/Patient.rs
patient/Observation.rs
patient/Condition.rs
openid fhirUser
```

### Clinical decision support

```
launch
patient/Patient.rs
patient/Observation.rs
patient/Condition.rs
patient/MedicationRequest.rs
patient/AllergyIntolerance.rs
```

### EHR-embedded app with full access

```
launch
patient/*.cruds
openid fhirUser
offline_access
```

### Backend analytics service

```
system/Patient.rs
system/Observation.rs
system/Encounter.rs
system/Condition.rs
```

### Lab results only

```
launch/patient
patient/Observation.rs?category=http://terminology.hl7.org/CodeSystem/observation-category|laboratory
patient/DiagnosticReport.rs
openid
```

## Scope negotiation

The server may grant fewer scopes than requested. Always check the `scope` field in the token response:

```python
# Requested
requested_scopes = "patient/*.cruds openid fhirUser offline_access"

# Granted (example - server limited to read-only)
granted_scopes = "patient/*.rs openid fhirUser"
```

Apps must handle scope negotiation gracefully and adjust functionality based on granted scopes.

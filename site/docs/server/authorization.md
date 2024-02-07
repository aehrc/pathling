---
sidebar_position: 6
description: Pathling can perform the role of a resource server within the OpenID Connect framework.
---

# Authorization

Pathling can perform the role of a resource server within the
[OpenID Connect framework](https://openid.net/connect/).

When authorization is enabled through configuration, Pathling will refuse any
requests which are not accompanied by a valid
[bearer token](https://tools.ietf.org/html/rfc6750). The following requirements
must be met:

- Token is a [JSON Web Token](https://tools.ietf.org/html/rfc7519)
- Token contains an
  [audience claim](https://tools.ietf.org/html/rfc7519#section-4.1.3) that
  matches the configured value
- Token contains
  an [issuer claim](https://tools.ietf.org/html/rfc7519#section-4.1.1)
  that matches the configured value
- Issuer provides
  an [OpenID Connect Discovery endpoint](https://openid.net/specs/openid-connect-discovery-1_0.html)
  that provides information about how to validate the token, including a link to
  a
  [JSON Web Key Set](https://tools.ietf.org/html/rfc7517) containing the signing
  key. This endpoint needs to be accessible to the Pathling server.

## Authorities

Pathling supports a set of authorities that control access to resources and
operations. Authorities must be provided within the `authorities` claim within
the JWT bearer token provided with each request.

![Authorities](../../src/images/authorities.svg#light-mode-only "Authorities")
![Authorities](../../src/images/authorities-dark.svg#dark-mode-only "Authorities")

| Authority                        | Description                                                                     |
|----------------------------------|---------------------------------------------------------------------------------|
| `pathling`                       | Provides access to all operations and resources, implies all other authorities. |
| `pathling:read`                  | Provides read access to all resource types.                                     |
| `pathling:read:[resource type]`  | Provides read access to only a specified resource type.                         |
| `pathling:write`                 | Provides write access to all resource types.                                    |
| `pathling:write:[resource type]` | Provides write access to only a specified resource type.                        |
| `pathling:import`                | Provides access to the import operation.                                        |
| `pathling:aggregate`             | Provides access to the aggregate operation.                                     |
| `pathling:search`                | Provides access to the search operation.                                        |
| `pathling:extract`               | Provides access to the extract operation.                                       |
| `pathling:update`                | Provides access to the update operation.                                        |
| `pathling:batch`                 | Provides access to the batch operation.                                         |

In order to enable access to an operation, an operation authority (e.g.
`pathling:search`) must be provided along with a `read` or `write` authority
(e.g. `pathling:read:Patient`).

Where expressions within a request reference multiple different resource types
(e.g. through resource references), authority for read access to all those
resources must be present within the token.

The import and batch operations require `write` authority for all resource types
that are referenced within the request.

## GA4GH passports

Pathling supports an opt-in implementation of
[GA4GH Passport v1](https://github.com/ga4gh-duri/ga4gh-duri.github.io/blob/master/researcher_ids/ga4gh_passport_v1.md), 
which is a way of encoding permission to access patient data within the access
token presented to the server.

You can enable GA4GH passport authentication by enabling the `ga4gh` Spring
profile when running Pathling. Note that this profile is not enabled by default,
and is not enabled within the pre-built Docker image.

The Pathling implementation supports a visa type of `ControlledAccessGrants`,
with a value that is interpreted as a dataset ID.

The issuer and the dataset ID are used to construct a URL of the form
`[issuer]/api/manifest/[dataset ID]`, which is used to retrieve a manifest
document. The manifest contains additional information on how to enforce
permissions for the visa.

The manifest document is expected to take the following form:

```json
{
  "patientIds": [
    "patient-1",
    "patient-2",
    ...
  ]
}
```

The patient IDs are combined with the
`pathling.auth.ga4ghPassports.patientIdSystem` configuration parameter to create
FHIR identifiers that Pathling can match against resources in the database.

When running with GA4GH passports enabled, all queries will be scoped to return
only records that relate to white-listed patients, according to the joining
logic defined within
the [FHIR Patient compartment](https://hl7.org/fhir/R4/compartmentdefinition-patient.html).

The presence of a `ControlledAccessGrants` visa implies the following
authorities within Pathling: `pathling:aggregate`, `pathling:search`,
`pathling:extract` (plus read access to all resources within the Patient
compartment).

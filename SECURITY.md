# Security policy

We take the security of Pathling seriously. This document explains which
versions receive security fixes, how to report a vulnerability, and what to
expect once you have done so.

## Supported versions

Security fixes are applied to the most recent release of each independently
versioned component. Pathling follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html), and we generally
release on the third Tuesday of each month, with earlier releases when an urgent
security update is required.

| Component                                              | Supported versions   |
| ------------------------------------------------------ | -------------------- |
| Core libraries (`encoders`, `fhirpath`, `library-api`) | Latest minor release |
| Python and R libraries                                 | Latest minor release |
| Server                                                 | Latest minor release |

Older releases do not receive backported fixes. If you are affected by a
vulnerability, the recommended remediation is to upgrade to the latest release.

## Reporting a vulnerability

Please report security vulnerabilities privately. Do not open a public issue,
pull request, or discussion for a suspected vulnerability, as this can put other
users at risk before a fix is available.

There are two ways to report:

- **GitHub private vulnerability reporting (preferred).** Use the
  [Report a vulnerability](https://github.com/aehrc/pathling/security/advisories/new)
  form to open a private security advisory. This keeps the report, our
  responses, and any draft fix in one place.
- **Email.** If you cannot use GitHub, send the details to
  [pathling@csiro.au](mailto:pathling@csiro.au).

Please include as much of the following as you can, so we can reproduce and
assess the issue quickly:

- A description of the vulnerability and the impact you believe it has.
- The affected component and version (for example, server 2.0.1).
- Step-by-step instructions to reproduce the issue, including any configuration,
  sample FHIR resources, or requests required.
- Any proof-of-concept code, logs, or screenshots.
- Your assessment of severity, if you have one.

## What to expect

When you report a vulnerability, you can expect the following:

- We will acknowledge your report within five business days.
- We will work with you to confirm the issue and determine its severity and
  scope.
- We will keep you informed of our progress towards a fix.
- Once a fix is released, we will publish a security advisory and credit you for
  the discovery, unless you ask to remain anonymous.

We ask that you give us a reasonable opportunity to release a fix before
disclosing the vulnerability publicly. We are happy to coordinate the timing of
public disclosure with you.

## Scope

This policy covers the code maintained in this repository, including the core
libraries, the Python and R bindings, the server, and the administration UI.

Vulnerabilities in third-party dependencies are best reported to the relevant
upstream project. If a dependency vulnerability affects Pathling and requires a
change on our side (for example, a version bump or a configuration change),
please let us know so that we can address it.

Pathling is experimental software and is provided without warranty under the
[Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
Please review the [README](README.md) for the current set of known limitations.

## Verifying released artifacts

Published Maven artifacts and Helm charts are signed with the following GPG key,
which you can use to verify their integrity:

- **Key ID**: `ED48678D`
- **Fingerprint**: `F814 751C 64B5 F5E7 08A8 C73F C3C6 291F ED48 678D`
- **User ID**: `Pathling Developers <pathling@csiro.au>`

The public key is available on
[keys.openpgp.org](https://keys.openpgp.org/search?q=F814751C64B5F5E708A8C73FC3C6291FED48678D).

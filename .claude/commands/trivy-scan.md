# Trivy security scan

Run Trivy vulnerability scans scoped to the requested module(s), then analyse
the results and provide actionable recommendations.

## Step 1: Determine scan scope from user input

The user passes the scan scope(s) as an argument or instruction. Accept one or
more of the following values:

| Scope              | Directories scanned                                                                                |
| ------------------ | -------------------------------------------------------------------------------------------------- |
| `server`           | `server/`                                                                                          |
| `ui`               | `ui/`                                                                                              |
| `core-libraries`   | `utilities/`, `encoders/`, `terminology/`, `fhirpath/`, `library-api/`, `library-runtime/`, `lib/` |
| `site`             | `site/`                                                                                            |
| `fhirpath-lab-api` | `fhirpath-lab-api/`                                                                                |
| `all`              | All of the above                                                                                   |

If the user does not specify a scope, default to `all`.

If the user provides an invalid scope, inform them of the valid options and
stop.

## Step 2: Run Trivy for each scope

Each scope has its own `.trivyignore` file. Run Trivy from within the scope's
directory so that the local `.trivyignore` is picked up automatically.

Common options for all scans:

```
--severity MEDIUM,HIGH,CRITICAL
--exit-code 0
```

### Core libraries scope

Scans from the repository root with `--skip-dirs` to exclude non-core modules.
The root `.trivyignore` contains suppressions for Spark-provided dependencies.

Working directory: repository root.

```bash
trivy repo . \
  --severity MEDIUM,HIGH,CRITICAL \
  --skip-files "examples/**/*,**/target/**/*,sql-on-fhir/**/*,licenses/**/*" \
  --skip-dirs "server,ui,site,fhirpath-lab-api,benchmark,test-data,deployment,.idea" \
  --exit-code 0
```

### Server scope

Scans the `server` directory. The `server/.trivyignore` contains suppressions
for Spark runtime transitive dependencies and server-specific libraries.

Working directory: `server/`.

```bash
cd server && trivy repo . \
  --severity MEDIUM,HIGH,CRITICAL \
  --skip-files "**/target/**/*" \
  --skip-dirs ".idea" \
  --exit-code 0
```

### UI scope

Scans the `ui` directory. The `ui/.trivyignore` contains suppressions for
client-side JavaScript dependencies.

Working directory: `ui/`.

```bash
cd ui && trivy repo . \
  --severity MEDIUM,HIGH,CRITICAL \
  --skip-dirs ".idea" \
  --exit-code 0
```

### Site scope

Scans the `site` directory. The `site/.trivyignore` contains any
site-specific suppressions.

Working directory: `site/`.

```bash
cd site && trivy repo . \
  --severity MEDIUM,HIGH,CRITICAL \
  --skip-files "**/target/**/*" \
  --skip-dirs ".idea" \
  --exit-code 0
```

### FHIRPath Lab API scope

Scans the `fhirpath-lab-api` directory. The
`fhirpath-lab-api/.trivyignore` contains any API-specific suppressions.

Working directory: `fhirpath-lab-api/`.

```bash
cd fhirpath-lab-api && trivy repo . \
  --severity MEDIUM,HIGH,CRITICAL \
  --skip-dirs ".idea" \
  --exit-code 0
```

Run scans for different scopes in parallel where possible. Use a timeout of
5 minutes per scan. If Trivy is not installed, inform the user and suggest
`brew install trivy`.

## Step 3: Analyse results and provide recommendations

For each vulnerability reported by Trivy, perform a contextual impact
assessment before recommending an action. This means reading the relevant parts
of the Pathling codebase to determine whether and how the vulnerable library is
actually used.

### Per-vulnerability analysis

For each vulnerability:

1. **Identify the vulnerability**: Record the CVE/GHSA ID, affected package,
   installed version, fixed version (if available), and a brief description of
   the attack vector.
2. **Investigate usage in our code**: Search the codebase (within the relevant
   scope) to determine how the vulnerable package is used. Look for:
    - Direct imports or references to the affected package or its vulnerable
      classes/functions.
    - Whether the vulnerable code path is reachable given our usage patterns
      (e.g. do we call the affected API, use the vulnerable configuration, or
      accept untrusted input that reaches the vulnerable code?).
    - Whether the package is a direct dependency, a transitive dependency, or a
      provided/runtime-only dependency that is not bundled in our distribution.
3. **Assess exploitability**: Based on the usage analysis, classify the
   vulnerability as one of:
    - **Exploitable**: The vulnerable code path is reachable in our
      implementation.
    - **Not exploitable**: The vulnerable code path is not reachable, or the
      preconditions for exploitation do not apply (e.g. SSR-only vulnerability
      in a client-side app, or a configuration we do not use).
    - **Not applicable**: The package is a provided dependency not bundled in
      our distribution.
4. **Recommend an action**:
    - **Exploitable with fix available**: Recommend upgrading to the fixed
      version. Identify the specific `pom.xml`, `package.json`, or other
      dependency file that needs updating.

        If the vulnerable package is a **transitive dependency**, always check
        first whether a newer version of the **direct dependency** that brings
        it in ships a fixed transitive. Use `mvn dependency:tree` (or the
        equivalent for the ecosystem) to identify the direct dependency, then
        inspect the upstream POM / package manifest of newer releases to see
        which transitive version they bundle.

        **Prefer upgrading the direct dependency over pinning the transitive
        version.** Rationale: a direct upgrade inherits any other fixes and
        compatibility work the upstream maintainers have done, whereas pinning
        a transitive version risks diverging from what the direct dependency
        was tested against. Only pin the transitive version when no suitable
        direct-dependency upgrade exists (e.g. upstream hasn't released a
        version that bundles a patched transitive), and record the reason in a
        comment adjacent to the override.

    - **Exploitable with no fix available**: Recommend tracking for future
      remediation. Suggest a workaround if one exists.
    - **Not exploitable or not applicable**: Recommend adding to the
      scope-specific `.trivyignore` with a comment explaining the rationale,
      following the existing format in that file (comment line, then CVE/GHSA
      ID).

### Output format

For each scope, present:

1. **Vulnerability count** by severity (CRITICAL, HIGH, MEDIUM).
2. **Detailed findings table** with columns: CVE/GHSA ID, package, severity,
   exploitability assessment, and recommended action.
3. **`.trivyignore` additions**: For vulnerabilities that should be suppressed,
   provide the exact lines to add to the scope's `.trivyignore` file, following
   the existing format (comment explaining rationale, then the CVE/GHSA ID).

If no vulnerabilities are found for a scope, report that the scan passed
cleanly.

End with an overall summary and prioritised list of actions, ordered by
exploitability and severity.

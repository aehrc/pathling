# Trivy security scan

Run Trivy vulnerability scans scoped to the modules modified on the current
branch, then analyse the results and provide actionable recommendations.

## Step 1: Determine modified modules

Run `git diff --name-only main...HEAD` to get the list of files changed on the
current branch. Map each changed file to one of the following scopes based on
its top-level directory:

| Changed directory                                                                                  | Scope            |
| -------------------------------------------------------------------------------------------------- | ---------------- |
| `server/`, `ui/`                                                                                   | server           |
| `utilities/`, `encoders/`, `terminology/`, `fhirpath/`, `library-api/`, `library-runtime/`, `lib/` | core-libraries   |
| `site/`                                                                                            | site             |
| `fhirpath-lab-api/`                                                                                | fhirpath-lab-api |

Files in other directories (e.g. `.github/`, `openspec/`, `benchmark/`,
`test-data/`, `deployment/`) do not trigger any scan scope.

If no scopes are identified, inform the user that no scannable modules were
modified and stop.

## Step 2: Run Trivy for each scope

Run the appropriate `trivy repo` command for each identified scope. All
commands must be run from the repository root so that `.trivyignore` and
relative scan paths resolve correctly.

Common options for all scans:

```
--severity MEDIUM,HIGH,CRITICAL
--ignorefile .trivyignore
--exit-code 0
```

### Server scope

Scans the full repo from the root but skips all non-server modules. The
`**/target/**/*` skip-file excludes Maven build output.

Working directory: repository root.

```bash
trivy repo . \
  --severity MEDIUM,HIGH,CRITICAL \
  --skip-files "**/target/**/*" \
  --skip-dirs "utilities,encoders,terminology,fhirpath,library-api,library-runtime,lib,site,fhirpath-lab-api,benchmark,test-data,deployment" \
  --ignorefile .trivyignore \
  --exit-code 0
```

### Core libraries scope

Scans the full repo from the root but skips non-core modules. The skip-files
exclude directories that are siblings of the core modules but not part of any
scan scope.

Working directory: repository root.

```bash
trivy repo . \
  --severity MEDIUM,HIGH,CRITICAL \
  --skip-files "examples/**/*,**/target/**/*,sql-on-fhir/**/*,licenses/**/*" \
  --skip-dirs "server,ui,site,fhirpath-lab-api,benchmark,test-data,deployment" \
  --ignorefile .trivyignore \
  --exit-code 0
```

### Site scope

Scans only the `site` subdirectory. The `bun.lock` skip-file prevents
lockfile noise. The `--ignorefile` path is relative to the working directory,
not the scan target, so it resolves to the root `.trivyignore`.

Working directory: repository root.

```bash
trivy repo site \
  --severity MEDIUM,HIGH,CRITICAL \
  --skip-files "bun.lock" \
  --ignorefile .trivyignore \
  --exit-code 0
```

### FHIRPath Lab API scope

Scans only the `fhirpath-lab-api` subdirectory.

Working directory: repository root.

```bash
trivy repo fhirpath-lab-api \
  --severity MEDIUM,HIGH,CRITICAL \
  --ignorefile .trivyignore \
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
      dependency file that needs updating. If it is a transitive dependency,
      identify the direct dependency that pulls it in and whether a version
      override or exclusion is appropriate.
    - **Exploitable with no fix available**: Recommend tracking for future
      remediation. Suggest a workaround if one exists.
    - **Not exploitable or not applicable**: Recommend adding to
      `.trivyignore` with a comment explaining the rationale, following the
      existing format in that file (comment line, then CVE/GHSA ID).

### Output format

For each scope, present:

1. **Vulnerability count** by severity (CRITICAL, HIGH, MEDIUM).
2. **Detailed findings table** with columns: CVE/GHSA ID, package, severity,
   exploitability assessment, and recommended action.
3. **`.trivyignore` additions**: For vulnerabilities that should be suppressed,
   provide the exact lines to add, following the existing format (comment
   explaining rationale, then the CVE/GHSA ID).

If no vulnerabilities are found for a scope, report that the scan passed
cleanly.

End with an overall summary and prioritised list of actions, ordered by
exploitability and severity.

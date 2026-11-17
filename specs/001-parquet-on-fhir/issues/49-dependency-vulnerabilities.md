# 49 — Clear the dependency vulnerabilities that the pre-release scan reports

**Spec:** none; a release gate. Raised from the Trivy scan of `release/11.0.0` (owner, 2026-11-10).

**What to build:** The Pre-release workflow on `release/11.0.0` fails at its security scan (`.github/workflows/pre-release.yml`, "Run security scan"), so no SNAPSHOT is published, although every test job passes (Pre-release run 42740213880, 2026-11-10). This is normally done just before a release, but it is tracked here so that 11.0.0 is not held up by it at the end (owner, 2026-11-10).

The scan of 2026-11-10 reported 5 findings, 3 HIGH and 2 MEDIUM, none CRITICAL. Among them:

| Dependency | Version | Finding | Fixed in |
| --- | --- | --- | --- |
| `com.google.guava:guava` | 33.2.1-jre | CVE-2026-102554 (HIGH, denial of service) | 33.7.2 |
| `org.xerial.snappy:snappy-java` | 1.1.10.7 | CVE-2026-108106 (HIGH, denial of service), CVE-2026-93451 (buffer overflow) | 1.1.10.9 |
| `io.opentelemetry:opentelemetry-api` | 1.60.1 | CVE-2026-118259 (MEDIUM) | 1.67.0 |

The list is not the one to work from. The move to Spark 4.1 changes the dependency tree, and with it the report, so the scan is re-run once that move has landed, and the findings of that run are the ones to clear.

- **Re-run the scan** on `release/11.0.0` after #2879 (Move to Spark 4.1 for the 11.0.0 release), and record the findings here.
- **Clear each finding** by upgrading the dependency, directly or through a managed version, where the upgrade is compatible with the Spark version Pathling runs on.
- **Where it is not**, because Spark or another runtime dependency pins the vulnerable version, record the finding in `.trivyignore` with the reason, and say here why it cannot be upgraded and whether Pathling's use reaches the vulnerable code.

**Blocked by:** #2879 (Move to Spark 4.1 for the 11.0.0 release).

**Status:** ready-for-agent

- [ ] The scan is re-run after #2879 lands, and its findings are listed here.
- [ ] Each finding is cleared by an upgrade, or recorded in `.trivyignore` with its reason.
- [ ] The Pre-release workflow's security scan passes on `release/11.0.0`.

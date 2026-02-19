## Why

The Pathling server module has 64.4% overall code coverage (67.3% line, 54.8%
branch) as confirmed by both SonarCloud and JaCoCo reports from the v1.1.1
server release. The core libraries sit at 85.2% line coverage but have weak
spots in `utilities` (14.4%) and `encoders` branch coverage (44.6%). Several
server classes have 0% or very low coverage, including import/export providers,
security components, async job handling, and Spark integration. Improving
coverage will catch regressions, document expected behaviour through tests, and
increase confidence in ongoing refactoring.

## What changes

- Add unit tests for server files with 0-30% coverage, prioritised by uncovered
  line count (highest impact first).
- Add unit tests for server files in the 30-65% range where significant
  uncovered branches remain.
- Improve coverage for the `utilities` core module (currently 14.4% line).
- Use JaCoCo HTML reports locally to iterate on coverage improvements.

## Capabilities

### New capabilities

- `server-test-coverage`: Unit test additions for the Pathling server module,
  targeting files identified by JaCoCo/SonarCloud as having low coverage.

### Modified capabilities

(none)

## Impact

- **Code**: New test classes in the server module's `src/test/java` directory.
- **Build**: No build configuration changes required; existing JaCoCo setup
  generates reports during `mvn verify`.
- **Dependencies**: May require additional test utilities (e.g., Mockito stubs
  for Spring Security, HAPI FHIR test helpers).
- **CI**: No changes to CI workflows; existing JaCoCo and Sonar integration will
  pick up new coverage automatically.

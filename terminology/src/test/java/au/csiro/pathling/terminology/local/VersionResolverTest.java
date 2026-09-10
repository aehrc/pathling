/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.terminology.local;

import static java.util.function.Function.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Version-ordering specification for {@link VersionResolver}. The general (non-SNOMED) cases are
 * ported verbatim from Ontoserver's {@code VersionSortingTest} so that Pathling's local mode picks
 * exactly the same default version as the reference server. The SNOMED default-edition and
 * ambiguity cases exercise Pathling's configured-edition behaviour.
 *
 * @author John Grimes
 */
class VersionResolverTest {

  private static final String URI_SCT = "http://snomed.info/sct";
  private static final String EDITION_INTERNATIONAL = "900000000000207008";
  private static final String EDITION_AUSTRALIAN = "32506021000036107";

  private final VersionResolver resolver = new VersionResolver(null);

  /** Resolves the latest of the given version strings under a general (non-SNOMED) URL. */
  private String latest(final String... versions) {
    return resolver.getLatestOfVersions(Arrays.asList(versions), identity(), "urn:test");
  }

  /** Asserts that {@code expected} is chosen as the latest of {@code versions}. */
  private void assertSort(final String expected, final String... versions) {
    assertEquals(
        expected, latest(versions), () -> "Unexpected latest of: " + Arrays.asList(versions));
  }

  /** Asserts that resolving the latest of {@code versions} is ambiguous. */
  private void assertAmbiguous(final String... versions) {
    assertThrows(AmbiguousVersionException.class, () -> latest(versions));
  }

  // --- Ported from Ontoserver VersionSortingTest ---

  @Test
  void missingVersionIsAmbiguous() {
    assertAmbiguous("1", null);
  }

  @Test
  void heuristicSorting() {
    assertSort("5", "1", "2", "3", "2", "3", "5");

    assertSort("3.3", "3.3", "3.2");
    assertSort("3.3", "3.2", "3.3");
    assertSort("3.3.1", "3.2", "3.3", "3.3.1");
    assertSort("3.3.1", "3.3", "3.3.1", "3.2");
    assertSort("3.3.1", "3.3.0", "3.3.1", "3.2");
    assertSort("3.3.1", "3.3", "3.3.1", "3.2.0.0");
    assertSort("13.2", "3.3.0", "3.3.1", "13.2");

    assertSort("V3.3", "V3.3", "V3.2");
    assertSort("V3.3", "V3.2", "V3.3");
    assertSort("V3.3.1", "V3.2", "V3.3", "V3.3.1");
    assertSort("V3.3.1", "V3.3", "V3.3.1", "V3.2");

    assertSort("V3.2", "V3.2", "A3.3");

    assertSort("V3", "V2", "V1", "V3");

    assertSort("V31", "V3.2", "V31");

    assertSort("2021-05-29", "2021-05-28", "2021-05-29");

    assertAmbiguous("V3.2", "V3.3", "V3.3.");

    assertSort("V3.3-alpha2", "V3.2-alpha1", "V3.3-alpha2", "V3.3");

    assertSort("006", "2", "0000001", "006");
    assertSort("6.1", "2", "0000001", "006", "6.1");

    // YYYYMMDD dates.
    assertSort("20240929", "20231229", "19780101", "20220101", "20240929", "20140929");

    // MMDDYYYY dates transposed to YYYYMMDD for comparison.
    assertSort("09292024", "12292023", "01011978", "01012022", "09292024", "09292014");

    // Trailing-numeric-zero tiebreaker prefers the more explicit form.
    assertSort("6.0", "2", "0000001", "006", "6.0");
  }

  @Test
  void trailingNumericZeroPrefersLongerVersion() {
    assertSort("2.9.0", "2.9.0", "2.9");
    assertSort("2.9.0", "2.9", "2.9.0");
    assertSort("2.9.0.0", "2.9.0", "2.9.0.0");
    assertSort("2.9.0.0", "2.9.0.0", "2.9");
  }

  @Test
  void trueDuplicateIsAmbiguous() {
    assertAmbiguous("2.9.0", "2.9.0");
  }

  @Test
  void mixedSemverAndNonSemverPrefersExplicit() {
    assertSort("1.0.0", "1.0.0", "1.0");
    assertSort("1.0.0", "1.0", "1.0.0");
  }

  @Test
  void allSemverPathResolves() {
    assertSort("1.0.1", "1.0.0", "1.0.1");
    assertSort("2.0.0", "1.10.0", "2.0.0");
  }

  @Test
  void semVerBasic() {
    assertSort("2.0.0", "1.0.0", "2.0.0");
    assertSort("1.1.0", "1.0.0", "1.1.0");
    assertSort("1.0.1", "1.0.0", "1.0.1");
    assertSort("1.10.0", "1.9.0", "1.10.0");
  }

  @Test
  void semVerReleaseBeatsPrerelease() {
    assertSort("1.0.0", "1.0.0", "1.0.0-preview");
    assertSort("1.0.0", "1.0.0-preview", "1.0.0");
    assertSort("1.0.0", "1.0.0", "1.0.0-ballot");
    assertSort("1.0.0", "1.0.0-ballot", "1.0.0");
  }

  @Test
  void semVerPrereleaseOrdering() {
    assertSort("1.0.0-preview", "1.0.0-ballot", "1.0.0-preview");
    assertSort("1.0.0-preview", "1.0.0-preview", "1.0.0-ballot");
  }

  @Test
  void semVerPrereleaseMixed() {
    assertSort("1.0.0", "1.0.0-preview", "1.0.0-ballot", "1.0.0");
    assertSort("1.0.0", "1.0.0", "1.0.0-preview", "1.0.0-ballot");
    assertSort("1.0.0", "1.0.0-ballot", "1.0.0", "1.0.0-preview");
  }

  @Test
  void semVerPrereleaseNumericIdentifiers() {
    assertSort("1.0.0-alpha.2", "1.0.0-alpha.1", "1.0.0-alpha.2");
    assertSort("1.0.0-alpha.11", "1.0.0-alpha.2", "1.0.0-alpha.11");
  }

  @Test
  void semVerPrereleaseNumericBeforeString() {
    assertSort("1.0.0-beta", "1.0.0-1", "1.0.0-beta");
  }

  @Test
  void semVerPrereleaseLongerWins() {
    assertSort("1.0.0-alpha.1", "1.0.0-alpha", "1.0.0-alpha.1");
  }

  @Test
  void semVerBuildMetadataIgnored() {
    assertAmbiguous("1.0.0+build1", "1.0.0+build2");
  }

  @Test
  void semVerHigherMajorBeatsPrerelease() {
    assertSort("2.0.0-preview", "1.0.0", "2.0.0-preview");
  }

  @Test
  void versionsAreEquivalent() {
    // Identical strings are equivalent.
    assertTrue(VersionResolver.versionsAreEquivalent("2.9.0", "2.9.0"));
    assertTrue(VersionResolver.versionsAreEquivalent("2.9", "2.9"));

    // Trailing-zero-padded equivalents.
    assertTrue(VersionResolver.versionsAreEquivalent("2.9.0", "2.9"));
    assertTrue(VersionResolver.versionsAreEquivalent("2.9", "2.9.0"));
    assertTrue(VersionResolver.versionsAreEquivalent("2.9.0.0", "2.9"));
    assertTrue(VersionResolver.versionsAreEquivalent("2.9.0.0", "2.9.0"));
    assertTrue(VersionResolver.versionsAreEquivalent("6.0", "006"));

    // Different non-trailing segments are not equivalent.
    assertFalse(VersionResolver.versionsAreEquivalent("2.9.0", "2.9.1"));
    assertFalse(VersionResolver.versionsAreEquivalent("2.9.0", "3.0.0"));
    assertFalse(VersionResolver.versionsAreEquivalent("2.9", "2.10"));
    assertFalse(VersionResolver.versionsAreEquivalent("2.9.0", "2.9.0.5"));

    // Non-zero or non-numeric trailing differences are not equivalent.
    assertFalse(VersionResolver.versionsAreEquivalent("2.9.0", "2.9.alpha"));

    // Null handling.
    assertTrue(VersionResolver.versionsAreEquivalent(null, null));
    assertFalse(VersionResolver.versionsAreEquivalent("2.9.0", null));
    assertFalse(VersionResolver.versionsAreEquivalent(null, "2.9.0"));
  }

  // --- SNOMED edition and version resolution (Pathling-specific) ---

  private String sctVersion(final String edition, final String date) {
    return "http://snomed.info/sct/" + edition + "/version/" + date;
  }

  @Test
  void snomedLatestWithinSingleEdition() {
    // With one edition present, the latest effectiveTime wins.
    final List<String> versions =
        Arrays.asList(
            sctVersion(EDITION_INTERNATIONAL, "20240101"),
            sctVersion(EDITION_INTERNATIONAL, "20250101"));

    assertEquals(
        sctVersion(EDITION_INTERNATIONAL, "20250101"),
        resolver.getLatestOfVersions(versions, identity(), URI_SCT));
  }

  @Test
  void snomedMultipleEditionsWithoutDefaultIsAmbiguous() {
    // Two editions and no configured default cannot be disambiguated.
    final List<String> versions =
        Arrays.asList(
            sctVersion(EDITION_INTERNATIONAL, "20250101"),
            sctVersion(EDITION_AUSTRALIAN, "20250601"));

    assertThrows(
        AmbiguousVersionException.class,
        () -> resolver.getLatestOfVersions(versions, identity(), URI_SCT));
  }

  @Test
  void snomedMultipleEditionsPrefersConfiguredDefault() {
    // The configured default edition is chosen even when another edition has a later date.
    final VersionResolver australianDefault = new VersionResolver(EDITION_AUSTRALIAN);
    final List<String> versions =
        Arrays.asList(
            sctVersion(EDITION_INTERNATIONAL, "20250101"),
            sctVersion(EDITION_AUSTRALIAN, "20240601"));

    assertEquals(
        sctVersion(EDITION_AUSTRALIAN, "20240601"),
        australianDefault.getLatestOfVersions(versions, identity(), URI_SCT));
  }

  @Test
  void snomedDefaultEditionSelectsLatestWithinThatEdition() {
    // When the default edition has multiple versions, the latest of those is chosen.
    final VersionResolver internationalDefault = new VersionResolver(EDITION_INTERNATIONAL);
    final List<String> versions =
        Arrays.asList(
            sctVersion(EDITION_INTERNATIONAL, "20240101"),
            sctVersion(EDITION_INTERNATIONAL, "20250101"),
            sctVersion(EDITION_AUSTRALIAN, "20250601"));

    assertEquals(
        sctVersion(EDITION_INTERNATIONAL, "20250101"),
        internationalDefault.getLatestOfVersions(versions, identity(), URI_SCT));
  }

  @Test
  void snomedDuplicateVersionIsAmbiguous() {
    // The same edition and effectiveTime twice is genuinely ambiguous.
    final List<String> versions =
        Arrays.asList(
            sctVersion(EDITION_INTERNATIONAL, "20250101"),
            sctVersion(EDITION_INTERNATIONAL, "20250101"));

    assertThrows(
        AmbiguousVersionException.class,
        () -> resolver.getLatestOfVersions(versions, identity(), URI_SCT));
  }

  @Test
  void snomedExperimentalSortsLowerThanProduction() {
    // Experimental (xsct) editions sort lower than production releases of the same edition.
    final String experimental =
        "http://snomed.info/xsct/" + EDITION_INTERNATIONAL + "/version/20250601";
    final String production = sctVersion(EDITION_INTERNATIONAL, "20250101");
    final List<String> versions = Arrays.asList(experimental, production);

    assertEquals(production, resolver.getLatestOfVersions(versions, identity(), URI_SCT));
  }
}

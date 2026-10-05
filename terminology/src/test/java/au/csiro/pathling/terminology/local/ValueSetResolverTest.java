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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.ecl.UnsupportedEclConstructError;
import au.csiro.pathling.test.Rf2Mini;
import au.csiro.pathling.vcl.VclFilter;
import au.csiro.pathling.vcl.VclParseException;
import au.csiro.pathling.vcl.VclRefsetMembership;
import au.csiro.pathling.vcl.VclSystemScoped;
import au.csiro.pathling.vcl.VclWildcard;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Verifies value set URL resolution against a catalogue of stored code system versions: explicit
 * SNOMED implicit forms, VCL URLs, version qualification, default-version selection, and the
 * unknown-content and ambiguity outcomes.
 *
 * @author John Grimes
 */
class ValueSetResolverTest {

  private static final String V1 = "svid-20230601";
  private static final String V2 = "svid-20240601";
  private static final String V_AU = "svid-au-20230601";
  private static final String V_EXPERIMENTAL = "svid-xsct-20250101";

  private static final String INTERNATIONAL = "http://snomed.info/sct/900000000000207008";
  private static final String AUSTRALIAN = "http://snomed.info/sct/32506021000036107";

  private ValueSetResolver singleEdition() {
    final List<CodeSystemEntry> catalogue =
        List.of(
            new CodeSystemEntry(Rf2Mini.SNOMED_URI, Rf2Mini.VERSION_20230601, V1),
            new CodeSystemEntry(Rf2Mini.SNOMED_URI, Rf2Mini.VERSION_20240601, V2));
    return new ValueSetResolver(catalogue, new VersionResolver(null));
  }

  /**
   * A store holding two International versions, one Australian version and an experimental
   * International version that is later than either production one.
   */
  private static ValueSetResolver multiEdition(final String defaultEdition) {
    final List<CodeSystemEntry> catalogue =
        List.of(
            new CodeSystemEntry(Rf2Mini.SNOMED_URI, Rf2Mini.VERSION_20230601, V1),
            new CodeSystemEntry(Rf2Mini.SNOMED_URI, Rf2Mini.VERSION_20240601, V2),
            new CodeSystemEntry(Rf2Mini.SNOMED_URI, AUSTRALIAN + "/version/20230601", V_AU),
            new CodeSystemEntry(
                Rf2Mini.SNOMED_URI,
                "http://snomed.info/xsct/900000000000207008/version/20250101",
                V_EXPERIMENTAL));
    return new ValueSetResolver(catalogue, new VersionResolver(defaultEdition));
  }

  private static String encode(final String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  @Test
  void memoisesResolutionOfRepeatedUrls() {
    // Resolution parses the URL (including any embedded ECL) and resolves versions, so it must be
    // computed once per URL rather than once per row.
    final ValueSetResolver resolver = singleEdition();
    final String url = Rf2Mini.SNOMED_URI + "?fhir_vs=ecl/" + encode("<< " + Rf2Mini.DIABETES);
    assertSame(resolver.resolve(url), resolver.resolve(url));
  }

  @Test
  void resolvesAllConceptsToTheLatestVersion() {
    final Optional<ResolvedValueSet> resolved =
        singleEdition().resolve(Rf2Mini.SNOMED_URI + "?fhir_vs");
    assertTrue(resolved.isPresent());
    assertEquals(V2, resolved.get().getSystemVersionId());
    assertEquals(Rf2Mini.SNOMED_URI, resolved.get().getSystemUrl());
    assertInstanceOf(VclWildcard.class, resolved.get().getExpression());
  }

  @Test
  void resolvesRefsetForm() {
    final Optional<ResolvedValueSet> resolved =
        singleEdition().resolve(Rf2Mini.SNOMED_URI + "?fhir_vs=refset/" + Rf2Mini.SIMPLE_REFSET);
    assertTrue(resolved.isPresent());
    final VclRefsetMembership expression =
        assertInstanceOf(VclRefsetMembership.class, resolved.get().getExpression());
    assertEquals(Rf2Mini.SIMPLE_REFSET, expression.getRefsetCode());
  }

  @Test
  void resolvesIsaForm() {
    final Optional<ResolvedValueSet> resolved =
        singleEdition().resolve(Rf2Mini.SNOMED_URI + "?fhir_vs=isa/" + Rf2Mini.DIABETES);
    assertInstanceOf(VclFilter.class, resolved.orElseThrow().getExpression());
  }

  @Test
  void resolvesEclForm() {
    final Optional<ResolvedValueSet> resolved =
        singleEdition()
            .resolve(Rf2Mini.SNOMED_URI + "?fhir_vs=ecl/" + encode("<< " + Rf2Mini.DIABETES));
    assertInstanceOf(VclFilter.class, resolved.orElseThrow().getExpression());
  }

  @Test
  void resolvesVclUrlToItsScopedSystem() {
    final String vcl = "(" + Rf2Mini.SNOMED_URI + ")concept << " + Rf2Mini.DIABETES;
    final Optional<ResolvedValueSet> resolved =
        singleEdition().resolve("http://fhir.org/VCL?v1=" + encode(vcl));
    assertTrue(resolved.isPresent());
    assertEquals(V2, resolved.get().getSystemVersionId());
    assertInstanceOf(VclSystemScoped.class, resolved.get().getExpression());
  }

  @Test
  void versionQualifiedUrlSelectsExactVersion() {
    final Optional<ResolvedValueSet> resolved =
        singleEdition()
            .resolve(Rf2Mini.VERSION_20230601 + "?fhir_vs=refset/" + Rf2Mini.SIMPLE_REFSET);
    assertEquals(V1, resolved.orElseThrow().getSystemVersionId());
  }

  @Test
  void unknownSystemUrlIsUnknownContent() {
    assertTrue(singleEdition().resolve("http://loinc.org/vs").isEmpty());
  }

  @Test
  void requestedVersionNotInStoreIsUnknownContent() {
    final String missing = "http://snomed.info/sct/900000000000207008/version/19990101";
    assertTrue(singleEdition().resolve(missing + "?fhir_vs").isEmpty());
  }

  // --- Edition URIs that name no version. ---

  @Test
  void editionUrlWithoutVersionSelectsTheLatestVersionOfThatEdition() {
    // THO allows a server to default to the most recent version of a named edition. The store holds
    // several editions and no default edition is configured, so this must not be ambiguous.
    final Optional<ResolvedValueSet> resolved =
        multiEdition(null).resolve(INTERNATIONAL + "?fhir_vs");
    assertEquals(V2, resolved.orElseThrow().getSystemVersionId());
    assertEquals(Rf2Mini.SNOMED_URI, resolved.get().getSystemUrl());
  }

  @Test
  void editionUrlWithoutVersionOverridesTheDefaultEdition() {
    // The named edition is used even when a different default edition is configured.
    assertEquals(
        V_AU,
        multiEdition("900000000000207008")
            .resolve(AUSTRALIAN + "?fhir_vs=isa/" + Rf2Mini.DIABETES)
            .orElseThrow()
            .getSystemVersionId());
  }

  @Test
  void editionUrlWithoutVersionForAnAbsentEditionIsUnknownContent() {
    assertTrue(multiEdition(null).resolve("http://snomed.info/sct/1999007?fhir_vs").isEmpty());
  }

  @Test
  void editionUrlWithoutVersionDoesNotMatchAnEditionWhoseIdentifierExtendsIt() {
    // An edition whose identifier merely starts with the requested one is a different edition.
    final List<CodeSystemEntry> catalogue =
        List.of(
            new CodeSystemEntry(
                Rf2Mini.SNOMED_URI, INTERNATIONAL + "1/version/20990101", "svid-longer"));
    final ValueSetResolver resolver = new ValueSetResolver(catalogue, new VersionResolver(null));
    assertTrue(resolver.resolve(INTERNATIONAL + "?fhir_vs").isEmpty());
  }

  @Test
  void codingVersionNamingOnlyAProductionEditionIgnoresExperimentalVersions() {
    // This is the Coding.version path. The experimental version is later than any production
    // version, but an sct edition URI names published content only.
    assertEquals(
        Optional.of(V2),
        multiEdition(null).resolveCodeSystemVersion(Rf2Mini.SNOMED_URI, INTERNATIONAL));
  }

  @Test
  void experimentalEditionUrlSelectsOnlyExperimentalVersions() {
    assertEquals(
        V_EXPERIMENTAL,
        multiEdition(null)
            .resolve("http://snomed.info/xsct/900000000000207008?fhir_vs")
            .orElseThrow()
            .getSystemVersionId());
  }

  @Test
  void experimentalEditionUrlWithNoExperimentalVersionIsUnknownContent() {
    // Production versions of the edition do not satisfy a request for unpublished content.
    assertTrue(
        singleEdition().resolve("http://snomed.info/xsct/900000000000207008?fhir_vs").isEmpty());
  }

  @Test
  void vclSystemVersionNamingOnlyAnEditionSelectsItsLatestVersion() {
    final String vcl =
        "(" + Rf2Mini.SNOMED_URI + "|" + AUSTRALIAN + ")concept << " + Rf2Mini.DIABETES;
    assertEquals(
        V_AU,
        multiEdition(null)
            .resolve("http://fhir.org/VCL?v1=" + encode(vcl))
            .orElseThrow()
            .getSystemVersionId());
  }

  @Test
  void ambiguousEditionRaisesAnError() {
    final List<CodeSystemEntry> twoEditions =
        List.of(
            new CodeSystemEntry(
                Rf2Mini.SNOMED_URI,
                "http://snomed.info/sct/900000000000207008/version/20230601",
                V1),
            new CodeSystemEntry(
                Rf2Mini.SNOMED_URI,
                "http://snomed.info/sct/32506021000036107/version/20230601",
                V2));
    final ValueSetResolver resolver = new ValueSetResolver(twoEditions, new VersionResolver(null));
    assertThrows(
        AmbiguousVersionException.class, () -> resolver.resolve(Rf2Mini.SNOMED_URI + "?fhir_vs"));
  }

  @Test
  void malformedVclReportsAParseError() {
    assertThrows(
        VclParseException.class,
        () -> singleEdition().resolve("http://fhir.org/VCL?v1=" + encode("(unclosed")));
  }

  @Test
  void unsupportedEclRaisesANamedError() {
    final UnsupportedEclConstructError error =
        assertThrows(
            UnsupportedEclConstructError.class,
            () ->
                singleEdition()
                    .resolve(
                        Rf2Mini.SNOMED_URI
                            + "?fhir_vs=ecl/"
                            + encode("<< " + Rf2Mini.DIABETES + " {{ + HISTORY-MIN }}")));
    assertTrue(error.getMessage().toLowerCase().contains("history"));
  }
}

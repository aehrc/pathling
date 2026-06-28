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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link CanonicalReference} covering the url/version split, the canonical-form
 * predicate, and candidate selection (exact version, then prefer-active-then-greatest-version).
 *
 * @author John Grimes
 */
class CanonicalReferenceTest {

  // ---------------------------------------------------------------------------
  // Parsing.
  // ---------------------------------------------------------------------------

  @Test
  void parsesUrlWithoutVersion() {
    final CanonicalReference reference = CanonicalReference.parse("https://example.org/V");

    assertThat(reference.getUrl()).isEqualTo("https://example.org/V");
    assertThat(reference.getVersion()).isNull();
    assertThat(reference.hasVersion()).isFalse();
  }

  @Test
  void parsesUrlWithVersion() {
    final CanonicalReference reference = CanonicalReference.parse("https://example.org/V|2");

    assertThat(reference.getUrl()).isEqualTo("https://example.org/V");
    assertThat(reference.getVersion()).isEqualTo("2");
    assertThat(reference.hasVersion()).isTrue();
  }

  @Test
  void treatsAnEmptyVersionSuffixAsAbsent() {
    final CanonicalReference reference = CanonicalReference.parse("https://example.org/V|");

    assertThat(reference.getUrl()).isEqualTo("https://example.org/V");
    assertThat(reference.getVersion()).isNull();
  }

  @Test
  void rejectsABlankUrlSegment() {
    assertThatThrownBy(() -> CanonicalReference.parse("|2"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("url");
  }

  // ---------------------------------------------------------------------------
  // Canonical-form detection.
  // ---------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://example.org/V",
        "http://example.org/V",
        "https://example.org/V|2",
        "urn:uuid:53fefa32-fcbb-4ff8-8a92-55ee120877b7"
      })
  void recognisesAbsoluteCanonicalUrls(final String value) {
    assertThat(CanonicalReference.isCanonical(value)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ViewDefinition/abc",
        "Library/abc",
        "abc",
        "patient-view",
        "https://example.org/V#fragment",
        "https://example.org/V|1|2",
        ""
      })
  void rejectsNonCanonicalValues(final String value) {
    assertThat(CanonicalReference.isCanonical(value)).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Candidate selection.
  // ---------------------------------------------------------------------------

  @Test
  void selectsTheSoleCandidate() {
    final Library only = library("1.0", PublicationStatus.RETIRED);
    final CanonicalReference reference = CanonicalReference.parse("https://example.org/V");

    assertThat(reference.select(List.of(only), isActive(), versionOf())).isSameAs(only);
  }

  @Test
  void withAVersionReturnsAMatchingCandidateWithoutPreferringStatus() {
    // The caller has already filtered to the exact version, so any candidate is acceptable; the
    // status preference does not apply when a version was explicitly requested.
    final Library first = library("2", PublicationStatus.RETIRED);
    final Library second = library("2", PublicationStatus.ACTIVE);
    final CanonicalReference reference = CanonicalReference.parse("https://example.org/V|2");

    assertThat(reference.select(List.of(first, second), isActive(), versionOf())).isSameAs(first);
  }

  @Test
  void prefersActiveOverRetiredWhenNoVersionSupplied() {
    final Library retired = library("3.0", PublicationStatus.RETIRED);
    final Library active = library("1.0", PublicationStatus.ACTIVE);
    final CanonicalReference reference = CanonicalReference.parse("https://example.org/V");

    assertThat(reference.select(List.of(retired, active), isActive(), versionOf()))
        .isSameAs(active);
  }

  @Test
  void prefersTheGreatestVersionAmongActiveCandidates() {
    final Library v1 = library("1.0", PublicationStatus.ACTIVE);
    final Library v2 = library("2.0", PublicationStatus.ACTIVE);
    final CanonicalReference reference = CanonicalReference.parse("https://example.org/V");

    assertThat(reference.select(List.of(v1, v2), isActive(), versionOf())).isSameAs(v2);
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private static Predicate<Library> isActive() {
    return library -> library.getStatus() == PublicationStatus.ACTIVE;
  }

  private static Function<Library, String> versionOf() {
    return Library::getVersion;
  }

  private static Library library(final String version, final PublicationStatus status) {
    final Library library = new Library();
    library.setVersion(version);
    library.setStatus(status);
    return library;
  }
}

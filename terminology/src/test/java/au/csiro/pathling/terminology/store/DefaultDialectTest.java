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

package au.csiro.pathling.terminology.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies how the default dialect of a release is chosen: from the import option where one is
 * given, otherwise from the release itself where it leaves no room for doubt, and otherwise not at
 * all. A release that holds several language reference sets and is not the International edition is
 * refused rather than guessed at, and the refusal names every candidate so the operator can choose.
 *
 * @author John Grimes
 */
class DefaultDialectTest {

  private static final String GB_ENGLISH = "900000000000508004";
  private static final String US_ENGLISH = "900000000000509007";
  private static final String INTERNATIONAL_EDITION = "900000000000207008";
  private static final String NATIONAL_EDITION = "32506021000036107";
  private static final String NHS_CLINICAL = "999001261000000100";
  private static final String NHS_PHARMACY = "999000691000001104";

  private static final String GB_ENGLISH_NAME = "Great Britain English language reference set";
  private static final String US_ENGLISH_NAME =
      "United States of America English language reference set";

  /** Builds a candidate map in an order deliberately unlike the order it should be reported in. */
  private static Map<String, String> candidates(final String... identifiersAndNames) {
    final Map<String, String> candidates = new LinkedHashMap<>();
    for (int index = 0; index < identifiersAndNames.length; index += 2) {
      candidates.put(identifiersAndNames[index], identifiersAndNames[index + 1]);
    }
    return candidates;
  }

  // --- The option decides. ---

  @Test
  void takesTheReferenceSetTheOptionNamesByTag() {
    assertEquals(
        GB_ENGLISH,
        DefaultDialect.choose(
            "en-GB",
            INTERNATIONAL_EDITION,
            candidates(US_ENGLISH, US_ENGLISH_NAME, GB_ENGLISH, GB_ENGLISH_NAME)));
  }

  @Test
  void takesTheReferenceSetTheOptionNamesByExtensionTag() {
    assertEquals(
        GB_ENGLISH,
        DefaultDialect.choose(
            "en-x-sctlang-90000000-00005080-04",
            INTERNATIONAL_EDITION,
            candidates(US_ENGLISH, US_ENGLISH_NAME, GB_ENGLISH, GB_ENGLISH_NAME)));
  }

  @Test
  void takesTheReferenceSetTheOptionNamesByIdentifier() {
    // A reference set outside the built-in alias table is named by its identifier, since the import
    // receives no service configuration and so consults no configured aliases.
    assertEquals(
        NHS_CLINICAL,
        DefaultDialect.choose(
            NHS_CLINICAL,
            NATIONAL_EDITION,
            candidates(NHS_CLINICAL, "NHS realm (clinical part)", GB_ENGLISH, GB_ENGLISH_NAME)));
  }

  @Test
  void refusesAnOptionNamingAReferenceSetTheReleaseDoesNotHold() {
    final Map<String, String> held = candidates(US_ENGLISH, US_ENGLISH_NAME);
    final TerminologyImportException failure =
        assertThrows(
            TerminologyImportException.class,
            () -> DefaultDialect.choose("en-GB", INTERNATIONAL_EDITION, held));
    // The message names the dialect as it was asked for, and the reference set it resolved to, so
    // the operator can see which of the two was wrong.
    assertTrue(failure.getMessage().contains("en-GB"), failure.getMessage());
    assertTrue(failure.getMessage().contains(GB_ENGLISH), failure.getMessage());
    // It also lists what the release does hold, which is what the operator has to choose from.
    assertTrue(failure.getMessage().contains(US_ENGLISH + "  " + US_ENGLISH_NAME));
  }

  @ParameterizedTest
  @ValueSource(strings = {"en", "klingon", "12345", "en-x-sctlang-nonsense"})
  void refusesAnOptionThatNamesNoReferenceSetAtAll(final String requested) {
    final Map<String, String> held = candidates(US_ENGLISH, US_ENGLISH_NAME);
    final TerminologyImportException failure =
        assertThrows(
            TerminologyImportException.class,
            () -> DefaultDialect.choose(requested, INTERNATIONAL_EDITION, held));
    assertTrue(failure.getMessage().contains(requested), failure.getMessage());
  }

  // --- The release decides. ---

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" "})
  void takesTheSoleReferenceSetOfTheRelease(@Nullable final String requested) {
    // One reference set leaves no room for doubt, whatever edition the release is.
    assertEquals(
        NHS_CLINICAL,
        DefaultDialect.choose(
            requested, NATIONAL_EDITION, candidates(NHS_CLINICAL, "NHS realm (clinical part)")));
  }

  @Test
  void takesUsEnglishForTheInternationalEdition() {
    assertEquals(
        US_ENGLISH,
        DefaultDialect.choose(
            null,
            INTERNATIONAL_EDITION,
            candidates(GB_ENGLISH, GB_ENGLISH_NAME, US_ENGLISH, US_ENGLISH_NAME)));
  }

  @Test
  void returnsNothingWhenTheReleaseHoldsNoLanguageReferenceSet() {
    // Nothing is preferred, so no default can be chosen and none is required: the display falls to
    // the fully specified name and then the concept code, as it does today.
    assertNull(DefaultDialect.choose(null, INTERNATIONAL_EDITION, Map.of()));
    assertNull(DefaultDialect.choose(null, null, Map.of()));
  }

  // --- Neither decides. ---

  @Test
  void refusesAReleaseHoldingSeveralReferenceSetsThatIsNotTheInternationalEdition() {
    final Map<String, String> held =
        candidates(
            NHS_CLINICAL,
            "NHS realm language reference set (clinical part)",
            GB_ENGLISH,
            "GB English",
            NHS_PHARMACY,
            "National Health Service realm language reference set (pharmacy part)");
    final TerminologyImportException failure =
        assertThrows(
            TerminologyImportException.class,
            () -> DefaultDialect.choose(null, NATIONAL_EDITION, held));
    // Candidates are listed in ascending identifier order so the message is itself reproducible,
    // whatever order the release's rows were read in.
    assertEquals(
        """
        The release holds 3 language reference sets and none of them is a clear default. \
        Name one with the defaultDialect import option:
          900000000000508004  GB English
          999000691000001104  National Health Service realm language reference set (pharmacy part)
          999001261000000100  NHS realm language reference set (clinical part)\
        """,
        failure.getMessage());
  }

  @Test
  void refusesAnInternationalReleaseThatDoesNotHoldUsEnglish() {
    // The International default is a specific reference set, not merely a rule, so a partial
    // package
    // that does not carry it is as ambiguous as any other release holding several.
    final Map<String, String> held =
        candidates(GB_ENGLISH, "GB English", NHS_CLINICAL, "NHS realm");
    final TerminologyImportException failure =
        assertThrows(
            TerminologyImportException.class,
            () -> DefaultDialect.choose(null, INTERNATIONAL_EDITION, held));
    assertTrue(failure.getMessage().contains("2 language reference sets"), failure.getMessage());
  }

  @Test
  void ordersCandidatesNumericallyRatherThanAsText() {
    // Identifiers of unequal length must order by value: a twelve-digit identifier is lower than an
    // eighteen-digit one even though it sorts after it as text.
    final Map<String, String> held =
        candidates(GB_ENGLISH, "GB English", "271000210107", "NZ English");
    final TerminologyImportException failure =
        assertThrows(
            TerminologyImportException.class,
            () -> DefaultDialect.choose(null, NATIONAL_EDITION, held));
    assertEquals(
        """
        The release holds 2 language reference sets and none of them is a clear default. \
        Name one with the defaultDialect import option:
          271000210107  NZ English
          900000000000508004  GB English\
        """,
        failure.getMessage());
  }

  @Test
  void namesACandidateTheReleaseGivesNoNameFor() {
    // A package carrying language reference set members but not the concepts that name them still
    // has to produce a usable message.
    final Map<String, String> unnamed = new LinkedHashMap<>();
    unnamed.put(GB_ENGLISH, null);
    unnamed.put(US_ENGLISH, US_ENGLISH_NAME);
    final TerminologyImportException failure =
        assertThrows(
            TerminologyImportException.class,
            () -> DefaultDialect.choose(null, NATIONAL_EDITION, unnamed));
    assertTrue(
        failure.getMessage().contains(GB_ENGLISH + "  (not named in this release)"),
        failure.getMessage());
  }
}

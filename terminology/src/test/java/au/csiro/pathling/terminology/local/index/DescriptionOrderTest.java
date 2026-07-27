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

package au.csiro.pathling.terminology.local.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies the order a concept's descriptions are held in. Every key comes from the content, so two
 * stores built from the same release hold each concept's descriptions in the same order, whatever
 * order their rows happened to be written in.
 *
 * @author John Grimes
 */
class DescriptionOrderTest {

  private static final Comparator<Description> ORDER = DescriptionOrder.byLanguageTypeAndTerm();

  @Nonnull
  private static Description description(
      @Nullable final String language,
      @Nullable final String typeCode,
      @Nonnull final String term) {
    return new Description(term, language, typeCode, "http://snomed.info/sct", null);
  }

  /** Sorts a list with the comparator under test and returns the terms in the resulting order. */
  @Nonnull
  private static List<String> sortedTerms(@Nonnull final List<Description> descriptions) {
    final List<Description> sorted = new ArrayList<>(descriptions);
    sorted.sort(ORDER);
    return sorted.stream().map(Description::getTerm).toList();
  }

  @Test
  void ordersByLanguageFirst() {
    // Grouping by language keeps a concept's languages together rather than interleaving them,
    // which
    // is how a person reading a designation list would expect to find them.
    assertEquals(
        List.of("Zebra in German", "Aardvark in English"),
        sortedTerms(
            List.of(
                description("en", "900000000000013009", "Aardvark in English"),
                description("de", "900000000000013009", "Zebra in German"))));
  }

  @Test
  void ordersByTypeWithinALanguage() {
    assertEquals(
        List.of("A fully specified name (finding)", "A synonym"),
        sortedTerms(
            List.of(
                description("en", "900000000000013009", "A synonym"),
                description("en", "900000000000003001", "A fully specified name (finding)"))));
  }

  @Test
  void ordersByTermWithinATypeAndLanguage() {
    assertEquals(
        List.of("Alpha", "Beta", "Gamma"),
        sortedTerms(
            List.of(
                description("en", "900000000000013009", "Gamma"),
                description("en", "900000000000013009", "Alpha"),
                description("en", "900000000000013009", "Beta"))));
  }

  @Test
  void sortsNullsLastWithinEachKey() {
    // A null language or type is unknown rather than empty, so it goes to the end of its group.
    assertEquals(
        List.of("Known language", "Unknown language"),
        sortedTerms(
            List.of(
                description(null, "900000000000013009", "Unknown language"),
                description("en", "900000000000013009", "Known language"))));
    assertEquals(
        List.of("Known type", "Unknown type"),
        sortedTerms(
            List.of(
                description("en", null, "Unknown type"),
                description("en", "900000000000013009", "Known type"))));
  }

  @Test
  void treatsDescriptionsEqualOnAllThreeKeysAsEqual() {
    // The comparator has to be total for the sort to be well defined, and two descriptions
    // differing
    // only in their acceptability are equal for the purpose of ordering.
    final Description left = description("en", "900000000000013009", "The same term");
    final Description right = description("en", "900000000000013009", "The same term");
    assertEquals(0, ORDER.compare(left, right));
    assertEquals(0, ORDER.compare(right, left));
  }

  @Test
  void isSymmetricAndTransitive() {
    // A comparator that is not consistent can make the sort throw, so the three keys are checked
    // against each other in both directions.
    final Description german = description("de", "900000000000003001", "Zebra");
    final Description englishFsn = description("en", "900000000000003001", "Aardvark");
    final Description englishSynonym = description("en", "900000000000013009", "Aardvark");
    assertTrue(ORDER.compare(german, englishFsn) < 0);
    assertTrue(ORDER.compare(englishFsn, german) > 0);
    assertTrue(ORDER.compare(englishFsn, englishSynonym) < 0);
    assertTrue(ORDER.compare(german, englishSynonym) < 0);
  }
}

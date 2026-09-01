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
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies that a requested language tag resolves to the SNOMED CT language reference set that
 * serves it: the built-in dialects of the International edition, the private-use extension form a
 * designation language is reported in, and a deployment's own aliases, which may also override a
 * built-in entry. A tag naming no reference set resolves to nothing rather than failing.
 *
 * @author John Grimes
 */
class DialectResolverTest {

  private static final DialectResolver BUILT_IN = new DialectResolver(null);

  @Nonnull
  private static Optional<String> resolve(@Nullable final String tag) {
    return BUILT_IN.resolve(tag);
  }

  @ParameterizedTest
  @CsvSource({
    // The eight built-in dialects, each verified against a terminology server in research.md.
    "en-GB,900000000000508004",
    "en-US,900000000000509007",
    "en-AU,32570271000036106",
    "es,448879004",
    "fr,722131000",
    "de,722130004",
    "ja,722129009",
    "zh,722128001"
  })
  void resolvesEveryBuiltInDialect(final String tag, final String refsetId) {
    assertEquals(Optional.of(refsetId), resolve(tag));
  }

  @ParameterizedTest
  @ValueSource(strings = {"EN-GB", "en-gb", "En-Gb"})
  void matchesATagWithoutRegardToCase(final String tag) {
    assertEquals(Optional.of("900000000000508004"), resolve(tag));
  }

  @Test
  void resolvesThePrivateUseExtensionForm() {
    // This is the form we emit as the language of a preferredForLanguage designation, so a language
    // reported on the way out can be requested on the way in.
    assertEquals(Optional.of("900000000000508004"), resolve("en-x-sctlang-90000000-00005080-04"));
    assertEquals(Optional.of("900000000000509007"), resolve("en-x-sctlang-90000000-00005090-07"));
  }

  @Test
  void resolvesThePrivateUseExtensionFormWithoutRegardToCase() {
    assertEquals(Optional.of("900000000000508004"), resolve("EN-X-SCTLANG-90000000-00005080-04"));
  }

  @Test
  void resolvesAnExtensionFormForAReferenceSetOutsideTheBuiltInTable() {
    // The extension form carries its own identifier, so it needs no alias table entry at all.
    assertEquals(Optional.of("271000210107"), resolve("en-x-sctlang-27100021-0107"));
  }

  @Test
  void resolvesAConfiguredAlias() {
    final DialectResolver resolver = new DialectResolver(Map.of("en-NZ", "271000210107"));
    assertEquals(Optional.of("271000210107"), resolver.resolve("en-NZ"));
    // Case is not significant in a configured key either.
    assertEquals(Optional.of("271000210107"), resolver.resolve("en-nz"));
  }

  @Test
  void prefersAConfiguredAliasOverABuiltInOne() {
    // A deployment can correct a built-in entry, so the configured mapping replaces it rather than
    // merging with it.
    final DialectResolver resolver = new DialectResolver(Map.of("en-GB", "999001261000000100"));
    assertEquals(Optional.of("999001261000000100"), resolver.resolve("en-GB"));
    // Every other built-in entry is untouched.
    assertEquals(Optional.of("900000000000509007"), resolver.resolve("en-US"));
  }

  @Test
  void leavesTheBuiltInTableAloneWhenAConfiguredAliasIsAdded() {
    final DialectResolver resolver = new DialectResolver(Map.of("en-NZ", "271000210107"));
    assertEquals(Optional.of("900000000000508004"), resolver.resolve("en-GB"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // A bare primary subtag names no single reference set, so it expresses no preference.
        "en",
        // A region that no built-in entry and no alias covers.
        "de-CH",
        "en-NZ",
        // A bare reference set identifier is not a language tag; the import option accepts one, a
        // query-time language request does not.
        "900000000000508004",
        // Malformed extension forms.
        "en-x-sctlang-not-digits",
        "en-x-sctlang-",
        "en-x-sctlang",
        "en-x-sctlang-1234",
        "en-x-sctlang-9000000000000000000000",
        "x-sctlang-90000000-00005080-04"
      })
  void resolvesAnUnrecognisedTagToNothing(final String tag) {
    assertTrue(resolve(tag).isEmpty(), "Expected " + tag + " to resolve to nothing");
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t"})
  void resolvesAnAbsentTagToNothing(@Nullable final String tag) {
    assertTrue(resolve(tag).isEmpty());
  }

  @Test
  void ignoresABlankOrMalformedConfiguredAlias() {
    // Configuration validation rejects these, but a resolver built from an unvalidated map must not
    // fail a lookup over them.
    final DialectResolver resolver =
        new DialectResolver(Map.of(" ", "900000000000508004", "en-XX", "not-digits"));
    assertTrue(resolver.resolve("en-XX").isEmpty());
    assertTrue(resolver.resolve(" ").isEmpty());
    assertEquals(Optional.of("900000000000508004"), resolver.resolve("en-GB"));
  }
}

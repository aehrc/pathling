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

import jakarta.annotation.Nullable;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies that a weighted language preference list is read as RFC 9110 describes: tags are tried
 * in descending order of weight, ties keep the order they were written, a zero weight excludes a
 * tag, and the wildcard expresses no preference. Nothing here ever raises an error, because a
 * lookup failing over a quirk in a header would be out of all proportion.
 *
 * @author John Grimes
 */
class LanguagePreferenceTest {

  @Test
  void readsASingleTag() {
    assertEquals(List.of("en-GB"), LanguagePreference.parse("en-GB"));
  }

  @Test
  void ordersTagsByDescendingWeight() {
    assertEquals(List.of("fr", "en"), LanguagePreference.parse("fr;q=0.9,en;q=0.5"));
    assertEquals(List.of("fr", "en"), LanguagePreference.parse("en;q=0.5,fr;q=0.9"));
  }

  @Test
  void treatsAnAbsentWeightAsTheHighest() {
    // An absent q parameter means 1, so an unweighted tag outranks any weighted one.
    assertEquals(List.of("en-GB", "fr"), LanguagePreference.parse("fr;q=0.9,en-GB"));
  }

  @Test
  void keepsTagsOfEqualWeightInTheOrderTheyWereWritten() {
    assertEquals(List.of("en-GB", "en-US"), LanguagePreference.parse("en-GB,en-US"));
    assertEquals(List.of("en-US", "en-GB"), LanguagePreference.parse("en-US,en-GB"));
    assertEquals(List.of("en-GB", "en-US"), LanguagePreference.parse("en-GB;q=0.4,en-US;q=0.4"));
  }

  @Test
  void dropsATagGivenZeroWeight() {
    assertEquals(List.of("en-GB"), LanguagePreference.parse("en-US;q=0,en-GB"));
    assertEquals(List.of(), LanguagePreference.parse("en-US;q=0"));
    assertEquals(List.of(), LanguagePreference.parse("en-US;q=0.0"));
  }

  @Test
  void dropsTheWildcard() {
    // A wildcard accepts anything, which is indistinguishable in effect from expressing no
    // preference, since the stored display is the default dialect's term.
    assertEquals(List.of(), LanguagePreference.parse("*"));
    assertEquals(List.of("en-GB"), LanguagePreference.parse("en-GB,*;q=0.1"));
    assertEquals(List.of("en-GB"), LanguagePreference.parse("*;q=0.9,en-GB;q=0.1"));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t", ",", ",,", ";q=0.5"})
  void readsAnAbsentOrEmptyValueAsNoPreference(@Nullable final String headerValue) {
    assertEquals(List.of(), LanguagePreference.parse(headerValue));
  }

  @Test
  void dropsAnEntryWhoseWeightCannotBeRead() {
    assertEquals(List.of(), LanguagePreference.parse("en-GB;q=notanumber"));
    assertEquals(List.of("en-US"), LanguagePreference.parse("en-GB;q=notanumber,en-US"));
    // A weight outside the permitted range is no more readable than a weight that is not a number.
    assertEquals(List.of(), LanguagePreference.parse("en-GB;q=2.0"));
    assertEquals(List.of(), LanguagePreference.parse("en-GB;q=-1"));
  }

  @Test
  void skipsEmptyEntries() {
    assertEquals(List.of("en-GB", "en-US"), LanguagePreference.parse("en-GB;q=0.9,,en-US;q=0.5"));
    assertEquals(List.of("en-GB"), LanguagePreference.parse(",en-GB,"));
    // The empty entry changes nothing, so both tags survive and are ordered by weight: the
    // unweighted en-US carries an implicit weight of 1 and therefore comes first.
    assertEquals(List.of("en-US", "en-GB"), LanguagePreference.parse("en-GB;q=0.9,,en-US"));
  }

  @Test
  void readsAnEntryOfNothingButSeparators() {
    assertEquals(List.of(), LanguagePreference.parse(";;;"));
    assertEquals(List.of("en-GB"), LanguagePreference.parse(";;;,en-GB"));
  }

  @Test
  void ignoresSurroundingWhitespace() {
    assertEquals(
        List.of("fr", "en-GB"), LanguagePreference.parse("  en-GB ; q=0.5 ,  fr ; q=0.9 "));
  }

  @Test
  void ignoresParametersOtherThanTheWeight() {
    assertEquals(List.of("en-GB"), LanguagePreference.parse("en-GB;charset=utf-8"));
    assertEquals(List.of("fr", "en-GB"), LanguagePreference.parse("en-GB;q=0.1;x=1,fr;q=0.9"));
  }
}

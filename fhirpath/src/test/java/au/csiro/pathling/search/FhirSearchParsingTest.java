/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link FhirSearch#fromQueryString(String)} parsing.
 */
class FhirSearchParsingTest {

  // ========== Basic Parsing ==========

  @Test
  void fromQueryString_singleParameter() {
    final FhirSearch search = FhirSearch.fromQueryString("gender=male");

    assertEquals(1, search.getCriteria().size());

    final SearchCriterion criterion = search.getCriteria().getFirst();
    assertEquals("gender", criterion.getParameterCode());
    assertNull(criterion.getModifier());
    assertEquals(List.of("male"), criterion.getValues());
  }

  @Test
  void fromQueryString_multipleParameters() {
    final FhirSearch search = FhirSearch.fromQueryString("gender=male&birthdate=1990-01-01");

    assertEquals(2, search.getCriteria().size());

    assertEquals("gender", search.getCriteria().getFirst().getParameterCode());
    assertEquals(List.of("male"), search.getCriteria().getFirst().getValues());

    assertEquals("birthdate", search.getCriteria().get(1).getParameterCode());
    assertEquals(List.of("1990-01-01"), search.getCriteria().get(1).getValues());
  }

  @Test
  void fromQueryString_commaSeparatedValues() {
    final FhirSearch search = FhirSearch.fromQueryString("gender=male,female");

    assertEquals(1, search.getCriteria().size());
    assertEquals(List.of("male", "female"), search.getCriteria().getFirst().getValues());
  }

  @Test
  void fromQueryString_withModifier() {
    final FhirSearch search = FhirSearch.fromQueryString("family:exact=Smith");

    assertEquals(1, search.getCriteria().size());

    final SearchCriterion criterion = search.getCriteria().getFirst();
    assertEquals("family", criterion.getParameterCode());
    assertEquals("exact", criterion.getModifier());
    assertEquals(List.of("Smith"), criterion.getValues());
  }

  @Test
  void fromQueryString_notModifier() {
    final FhirSearch search = FhirSearch.fromQueryString("gender:not=male");

    final SearchCriterion criterion = search.getCriteria().getFirst();
    assertEquals("gender", criterion.getParameterCode());
    assertEquals("not", criterion.getModifier());
    assertEquals(List.of("male"), criterion.getValues());
  }

  // ========== URL Encoding ==========

  @Test
  void fromQueryString_urlEncodedPipe() {
    // code=http://loinc.org|123 (URL encoded)
    final String encoded = "code=" + URLEncoder.encode("http://loinc.org|123", StandardCharsets.UTF_8);
    final FhirSearch search = FhirSearch.fromQueryString(encoded);

    assertEquals(1, search.getCriteria().size());
    assertEquals(List.of("http://loinc.org|123"), search.getCriteria().getFirst().getValues());
  }

  @Test
  void fromQueryString_urlEncodedCommaBecomesOr() {
    // code=a,b (URL encoded comma)
    final String encoded = "code=" + URLEncoder.encode("a,b", StandardCharsets.UTF_8);
    final FhirSearch search = FhirSearch.fromQueryString(encoded);

    assertEquals(1, search.getCriteria().size());
    // After URL decoding, the comma becomes a delimiter for OR values
    assertEquals(List.of("a", "b"), search.getCriteria().getFirst().getValues());
  }

  @Test
  void fromQueryString_urlEncodedAmpersandInValue() {
    // name=A&B (URL encoded ampersand)
    final String encoded = "name=" + URLEncoder.encode("A&B", StandardCharsets.UTF_8);
    final FhirSearch search = FhirSearch.fromQueryString(encoded);

    assertEquals(1, search.getCriteria().size());
    assertEquals(List.of("A&B"), search.getCriteria().getFirst().getValues());
  }

  @Test
  void fromQueryString_fullUrlEncodedExample() {
    // code=http://loinc.org|8867-4 with proper encoding
    final String value = "http://loinc.org|8867-4";
    final String encoded = "code=" + URLEncoder.encode(value, StandardCharsets.UTF_8);
    final FhirSearch search = FhirSearch.fromQueryString(encoded);

    assertEquals("http://loinc.org|8867-4", search.getCriteria().getFirst().getValues().getFirst());
  }

  // ========== FHIR Escaping ==========

  static Stream<Arguments> fhirEscapingTestCases() {
    return Stream.of(
        // \, - backslash-comma is a literal comma, not a delimiter
        Arguments.of("escaped comma", "code=a\\,b", List.of("a,b")),
        // \\ - double backslash is a literal backslash
        Arguments.of("escaped backslash", "code=a\\\\b", List.of("a\\b")),
        // \\, - backslash-backslash followed by comma: literal backslash, then delimiter
        Arguments.of("escaped backslash before comma", "code=a\\\\,b", List.of("a\\", "b")),
        // Complex: a\,b\\c,d -> ["a,b\c", "d"]
        Arguments.of("multiple escape sequences", "code=a\\,b\\\\c,d", List.of("a,b\\c", "d"))
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("fhirEscapingTestCases")
  void fromQueryString_fhirEscaping(final String name, final String queryString,
      final List<String> expectedValues) {
    final FhirSearch search = FhirSearch.fromQueryString(queryString);

    assertEquals(1, search.getCriteria().size());
    assertEquals(expectedValues, search.getCriteria().getFirst().getValues());
  }

  // ========== Edge Cases ==========

  @Test
  void fromQueryString_emptyString() {
    final FhirSearch search = FhirSearch.fromQueryString("");

    assertTrue(search.getCriteria().isEmpty());
  }

  @Test
  void fromQueryString_emptyValue() {
    final FhirSearch search = FhirSearch.fromQueryString("gender=");

    assertEquals(1, search.getCriteria().size());
    assertEquals(List.of(""), search.getCriteria().getFirst().getValues());
  }

  @Test
  void fromQueryString_parameterWithoutEquals() {
    final FhirSearch search = FhirSearch.fromQueryString("_summary");

    assertEquals(1, search.getCriteria().size());
    assertEquals("_summary", search.getCriteria().getFirst().getParameterCode());
    assertEquals(List.of(""), search.getCriteria().getFirst().getValues());
  }

  @Test
  void fromQueryString_repeatedParameter() {
    // Repeated parameters create separate criteria (AND logic)
    final FhirSearch search = FhirSearch.fromQueryString("date=ge2022-01-01&date=lt2023-01-01");

    assertEquals(2, search.getCriteria().size());

    assertEquals("date", search.getCriteria().getFirst().getParameterCode());
    assertEquals(List.of("ge2022-01-01"), search.getCriteria().getFirst().getValues());

    assertEquals("date", search.getCriteria().get(1).getParameterCode());
    assertEquals(List.of("lt2023-01-01"), search.getCriteria().get(1).getValues());
  }

  @Test
  void fromQueryString_trailingAmpersand() {
    final FhirSearch search = FhirSearch.fromQueryString("gender=male&");

    assertEquals(1, search.getCriteria().size());
    assertEquals("gender", search.getCriteria().getFirst().getParameterCode());
  }

  @Test
  void fromQueryString_leadingAmpersand() {
    final FhirSearch search = FhirSearch.fromQueryString("&gender=male");

    assertEquals(1, search.getCriteria().size());
    assertEquals("gender", search.getCriteria().getFirst().getParameterCode());
  }

  @Test
  void fromQueryString_multipleConsecutiveAmpersands() {
    final FhirSearch search = FhirSearch.fromQueryString("gender=male&&birthdate=1990");

    assertEquals(2, search.getCriteria().size());
  }

  @Test
  void fromQueryString_withPrefix() {
    final FhirSearch search = FhirSearch.fromQueryString("birthdate=ge1990-01-01");

    assertEquals(1, search.getCriteria().size());
    // Prefix is part of the value, not parsed specially at this level
    assertEquals(List.of("ge1990-01-01"), search.getCriteria().getFirst().getValues());
  }

  @Test
  void fromQueryString_complexExample() {
    // Real-world example with multiple features
    final String queryString = "code=" + URLEncoder.encode("http://loinc.org|8867-4", StandardCharsets.UTF_8)
        + "&value-quantity=lt60,gt100"
        + "&date=ge2022-01-01";

    final FhirSearch search = FhirSearch.fromQueryString(queryString);

    assertEquals(3, search.getCriteria().size());

    assertEquals("code", search.getCriteria().getFirst().getParameterCode());
    assertEquals(List.of("http://loinc.org|8867-4"), search.getCriteria().getFirst().getValues());

    assertEquals("value-quantity", search.getCriteria().get(1).getParameterCode());
    assertEquals(List.of("lt60", "gt100"), search.getCriteria().get(1).getValues());

    assertEquals("date", search.getCriteria().get(2).getParameterCode());
    assertEquals(List.of("ge2022-01-01"), search.getCriteria().get(2).getValues());
  }

  // ========== splitOnComma helper tests ==========

  @Test
  void splitOnComma_simpleValue() {
    assertEquals(List.of("abc"), FhirSearch.splitOnComma("abc"));
  }

  @Test
  void splitOnComma_multipleValues() {
    assertEquals(List.of("a", "b", "c"), FhirSearch.splitOnComma("a,b,c"));
  }

  @Test
  void splitOnComma_escapedComma() {
    assertEquals(List.of("a,b"), FhirSearch.splitOnComma("a\\,b"));
  }

  @Test
  void splitOnComma_escapedBackslash() {
    assertEquals(List.of("a\\b"), FhirSearch.splitOnComma("a\\\\b"));
  }

  @Test
  void splitOnComma_trailingBackslash() {
    assertEquals(List.of("abc\\"), FhirSearch.splitOnComma("abc\\"));
  }

  @Test
  void splitOnComma_emptySegments() {
    assertEquals(List.of("", "a", ""), FhirSearch.splitOnComma(",a,"));
  }
}

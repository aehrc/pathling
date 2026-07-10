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

package au.csiro.pathling.vcl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URLDecoder;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Golden tests for the VCL parser: each construct of the VCL v1 grammar is parsed and its resulting
 * {@link VclExpression} model asserted, and malformed input is checked to raise a {@link
 * VclParseException} with a position.
 *
 * @author John Grimes
 */
class VclParserTest {

  // --- Codes and wildcard ---

  @Test
  void parsesBareCode() {
    assertEquals(new VclCode("73211009"), Vcl.parse("73211009"));
  }

  @Test
  void parsesQuotedCode() {
    // A code with a period must be quoted, otherwise it parses as property navigation.
    assertEquals(new VclCode("B.123"), Vcl.parse("\"B.123\""));
  }

  @Test
  void parsesWildcard() {
    assertEquals(new VclWildcard(), Vcl.parse("*"));
  }

  // --- Set operators and grouping ---

  @Test
  void parsesConjunction() {
    assertEquals(
        new VclConjunction(List.of(new VclCode("A"), new VclCode("B"), new VclCode("C"))),
        Vcl.parse("A,B,C"));
  }

  @Test
  void parsesDisjunction() {
    assertEquals(new VclDisjunction(List.of(new VclCode("A"), new VclCode("B"))), Vcl.parse("A;B"));
  }

  @Test
  void parsesExclusion() {
    // A hyphen is a valid character within an unquoted code (e.g. "4548-4"), so the exclusion
    // operator must be separated from its operands by whitespace to be recognised.
    assertEquals(new VclExclusion(new VclCode("A"), new VclCode("B")), Vcl.parse("A - B"));
  }

  @Test
  void parsesHyphenatedCodeAsSingleCode() {
    // Confirms the counterpart: without surrounding whitespace a hyphen is part of the code.
    assertEquals(new VclCode("A-B"), Vcl.parse("A-B"));
  }

  @Test
  void parsesGroupedDisjunctionInConjunction() {
    assertEquals(
        new VclConjunction(
            List.of(
                new VclDisjunction(List.of(new VclCode("A"), new VclCode("B"))), new VclCode("C"))),
        Vcl.parse("(A;B),C"));
  }

  @Test
  void parsesConjunctionInDisjunction() {
    assertEquals(
        new VclDisjunction(
            List.of(
                new VclCode("A"), new VclConjunction(List.of(new VclCode("B"), new VclCode("C"))))),
        Vcl.parse("A;(B,C)"));
  }

  @Test
  void parsesGroupedExclusion() {
    assertEquals(
        new VclExclusion(
            new VclDisjunction(List.of(new VclCode("A"), new VclCode("B"))), new VclCode("C")),
        Vcl.parse("(A;B)-C"));
  }

  // --- System URI prefixes ---

  @Test
  void parsesSystemPrefixedCode() {
    assertEquals(
        new VclSystemScoped(new VclSystemUri("http://loinc.org", null), new VclCode("4548-4")),
        Vcl.parse("(http://loinc.org)4548-4"));
  }

  @Test
  void parsesSystemPrefixWithVersion() {
    assertEquals(
        new VclSystemScoped(new VclSystemUri("http://loinc.org", "2.74"), new VclCode("4548-4")),
        Vcl.parse("(http://loinc.org|2.74)4548-4"));
  }

  @Test
  void parsesSystemPrefixOnGroup() {
    assertEquals(
        new VclSystemScoped(
            new VclSystemUri("http://loinc.org", null),
            new VclDisjunction(List.of(new VclCode("41995-2"), new VclCode("4548-4")))),
        Vcl.parse("(http://loinc.org)(41995-2;4548-4)"));
  }

  @Test
  void parsesSnomedVersionPinnedSystemUri() {
    final String version = "http://snomed.info/sct/32506021000036107/version/20230831";
    assertEquals(
        new VclSystemScoped(
            new VclSystemUri("http://snomed.info/sct", version), new VclCode("73211009")),
        Vcl.parse("(http://snomed.info/sct|" + version + ")73211009"));
  }

  // --- Filter operators ---

  @Test
  void parsesEqualsFilter() {
    assertEquals(
        new VclFilter("parent", VclFilterOperator.EQUALS, new VclCodeValue("73211009")),
        Vcl.parse("parent = 73211009"));
  }

  @Test
  void parsesIsAFilter() {
    assertEquals(
        new VclFilter("concept", VclFilterOperator.IS_A, new VclCodeValue("73211009")),
        Vcl.parse("concept << 73211009"));
  }

  @Test
  void parsesIsNotAFilter() {
    assertEquals(
        new VclFilter("concept", VclFilterOperator.IS_NOT_A, new VclCodeValue("46635009")),
        Vcl.parse("concept ~<< 46635009"));
  }

  @Test
  void parsesDescendentOfFilter() {
    assertEquals(
        new VclFilter("concept", VclFilterOperator.DESCENDENT_OF, new VclCodeValue("73211009")),
        Vcl.parse("concept < 73211009"));
  }

  @Test
  void parsesChildOfFilter() {
    assertEquals(
        new VclFilter("concept", VclFilterOperator.CHILD_OF, new VclCodeValue("404684003")),
        Vcl.parse("concept <! 404684003"));
  }

  @Test
  void parsesDescendentLeafFilter() {
    assertEquals(
        new VclFilter("concept", VclFilterOperator.DESCENDENT_LEAF, new VclCodeValue("64572001")),
        Vcl.parse("concept !!< 64572001"));
  }

  @Test
  void parsesGeneralizesFilter() {
    assertEquals(
        new VclFilter("concept", VclFilterOperator.GENERALIZES, new VclCodeValue("44054006")),
        Vcl.parse("concept >> 44054006"));
  }

  @Test
  void parsesRegexFilter() {
    assertEquals(
        new VclFilter("code", VclFilterOperator.REGEX, new VclStringValue("A.*")),
        Vcl.parse("code / \"A.*\""));
  }

  @Test
  void parsesExistsFilter() {
    assertEquals(
        new VclFilter("ingredient", VclFilterOperator.EXISTS, new VclCodeValue("true")),
        Vcl.parse("ingredient ? true"));
  }

  @Test
  void parsesInFilterWithCodeList() {
    assertEquals(
        new VclFilter(
            "concept", VclFilterOperator.IN, new VclCodeListValue(List.of("A", "B", "C"))),
        Vcl.parse("concept ^ {A,B,C}"));
  }

  @Test
  void parsesNotInFilterWithCodeList() {
    assertEquals(
        new VclFilter(
            "concept", VclFilterOperator.NOT_IN, new VclCodeListValue(List.of("A", "B", "C"))),
        Vcl.parse("concept ~^ {A,B,C}"));
  }

  @Test
  void parsesNestedFilterList() {
    // consists_of ^ { has_ingredient ^ { has_tradename = 2201670 } }
    final VclFilter innermost =
        new VclFilter("has_tradename", VclFilterOperator.EQUALS, new VclCodeValue("2201670"));
    final VclFilter middle =
        new VclFilter(
            "has_ingredient", VclFilterOperator.IN, new VclFilterListValue(List.of(innermost)));
    final VclFilter outer =
        new VclFilter("consists_of", VclFilterOperator.IN, new VclFilterListValue(List.of(middle)));
    assertEquals(
        outer, Vcl.parse("consists_of ^ { has_ingredient ^ { has_tradename = 2201670 } }"));
  }

  @Test
  void parsesNavigationInsideFilterList() {
    // A filter-list element may itself be a reverse navigation (the DOT form of the filter rule),
    // not only a property filter.
    assertEquals(
        new VclFilter(
            "concept",
            VclFilterOperator.IN,
            new VclFilterListValue(List.of(new VclNavigation(new VclCodeValue("x"), "y")))),
        Vcl.parse("concept ^ { x.y }"));
  }

  // --- Value set inclusion ---

  @Test
  void parsesTopLevelValueSetInclusion() {
    assertEquals(
        new VclIncludeValueSet("http://hl7.org/fhir/ValueSet/payeetype", false),
        Vcl.parse("^http://hl7.org/fhir/ValueSet/payeetype"));
  }

  @Test
  void parsesTopLevelCodeSystemInclusion() {
    assertEquals(
        new VclIncludeValueSet("http://loinc.org", true), Vcl.parse("^(http://loinc.org)"));
  }

  @Test
  void parsesInclusionCombinedWithCode() {
    assertEquals(
        new VclDisjunction(
            List.of(
                new VclCode("10007-3"),
                new VclIncludeValueSet("http://loinc.org/vs/LP257682-7", false))),
        Vcl.parse("10007-3 ; ^http://loinc.org/vs/LP257682-7"));
  }

  // --- Property navigation ---

  @Test
  void parsesCodeNavigation() {
    assertEquals(new VclNavigation(new VclCodeValue("B"), "codeprop"), Vcl.parse("B.codeprop"));
  }

  @Test
  void parsesWildcardNavigation() {
    assertEquals(new VclNavigation(new VclWildcardValue(), "parent"), Vcl.parse("*.parent"));
  }

  @Test
  void parsesFilterListNavigation() {
    assertEquals(
        new VclNavigation(
            new VclFilterListValue(
                List.of(
                    new VclFilter(
                        "concept", VclFilterOperator.DESCENDENT_OF, new VclCodeValue("B")))),
            "parent"),
        Vcl.parse("{concept < B}.parent"));
  }

  @Test
  void parsesCodeListNavigation() {
    assertEquals(
        new VclNavigation(new VclCodeListValue(List.of("A", "B")), "parent"),
        Vcl.parse("{A,B}.parent"));
  }

  // --- Quoting and escaping ---

  @Test
  void unescapesQuotedValue() {
    // VCL: code / "a\"b" -> the regex value is a"b.
    assertEquals(
        new VclFilter("code", VclFilterOperator.REGEX, new VclStringValue("a\"b")),
        Vcl.parse("code / \"a\\\"b\""));
  }

  @Test
  void unescapesBackslash() {
    // VCL: code / "a\\b" -> the regex value is a\b.
    assertEquals(
        new VclFilter("code", VclFilterOperator.REGEX, new VclStringValue("a\\b")),
        Vcl.parse("code / \"a\\\\b\""));
  }

  // --- Boolean property ---

  @Test
  void parsesBooleanPropertyFilter() {
    assertEquals(
        new VclSystemScoped(
            new VclSystemUri("http://snomed.info/sct", null),
            new VclFilter("inactive", VclFilterOperator.EQUALS, new VclCodeValue("true"))),
        Vcl.parse("(http://snomed.info/sct)inactive = true"));
  }

  // --- Percent-encoded URL form (decoded upstream, then parsed) ---

  @Test
  void parsesPercentDecodedExpression() {
    final String encoded = "(http://snomed.info/sct)concept%20%3C%3C%2073211009";
    final String decoded = URLDecoder.decode(encoded, UTF_8);
    assertEquals(
        new VclSystemScoped(
            new VclSystemUri("http://snomed.info/sct", null),
            new VclFilter("concept", VclFilterOperator.IS_A, new VclCodeValue("73211009"))),
        Vcl.parse(decoded));
  }

  // --- Error positions ---

  @Test
  void rejectsEmptyExpression() {
    assertThrows(VclParseException.class, () -> Vcl.parse(""));
  }

  @Test
  void rejectsTrailingComma() {
    final VclParseException e = assertThrows(VclParseException.class, () -> Vcl.parse("A,"));
    assertTrue(e.getPosition() > 0, "position should be reported");
  }

  @Test
  void rejectsUnclosedGroup() {
    final VclParseException e = assertThrows(VclParseException.class, () -> Vcl.parse("(A"));
    assertTrue(e.getPosition() > 0, "position should be reported");
  }

  @Test
  void rejectsMissingFilterValue() {
    final VclParseException e =
        assertThrows(VclParseException.class, () -> Vcl.parse("concept << "));
    assertTrue(e.getPosition() > 0, "position should be reported");
  }

  @Test
  void rejectsNewline() {
    // Newlines are not permitted anywhere in a VCL expression.
    assertThrows(VclParseException.class, () -> Vcl.parse("A\nB"));
  }
}

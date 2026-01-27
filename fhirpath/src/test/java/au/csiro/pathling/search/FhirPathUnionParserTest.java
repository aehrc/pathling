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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link FhirPathUnionParser}. */
class FhirPathUnionParserTest {

  // ========== splitUnion tests ==========

  @Test
  void splitUnion_simpleExpression() {
    final List<String> result = FhirPathUnionParser.splitUnion("Patient.name");

    assertEquals(List.of("Patient.name"), result);
  }

  static Stream<Arguments> splitUnionTestCases() {
    return Stream.of(
        Arguments.of(
            "multi-resource",
            "Patient.name | Practitioner.name",
            List.of("Patient.name", "Practitioner.name")),
        Arguments.of(
            "trims whitespace",
            "  Patient.name  |  Practitioner.name  ",
            List.of("Patient.name", "Practitioner.name")),
        Arguments.of(
            "empty parts",
            "Patient.name | | Practitioner.name",
            List.of("Patient.name", "Practitioner.name")),
        Arguments.of(
            "preserves parentheses",
            "A.field | (B | C).first() | D.field",
            List.of("A.field", "(B | C).first()", "D.field")),
        Arguments.of(
            "nested parentheses",
            "A.x | (B.where((C | D).exists())).first() | E.y",
            List.of("A.x", "(B.where((C | D).exists())).first()", "E.y")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("splitUnionTestCases")
  void splitUnion(final String name, final String input, final List<String> expected) {
    final List<String> result = FhirPathUnionParser.splitUnion(input);
    assertEquals(expected, result);
  }

  // ========== isQualified tests ==========

  @Test
  void isQualified_qualifiedExpression() {
    assertTrue(FhirPathUnionParser.isQualified("Patient.name"));
    assertTrue(FhirPathUnionParser.isQualified("Observation.code"));
  }

  @Test
  void isQualified_unqualifiedExpression() {
    assertFalse(FhirPathUnionParser.isQualified("name.given"));
    assertFalse(FhirPathUnionParser.isQualified("(start | end).first()"));
  }

  @Test
  void isQualified_parenthesizedQualified() {
    assertTrue(FhirPathUnionParser.isQualified("(Patient.name)"));
    assertTrue(FhirPathUnionParser.isQualified("(Observation.effective.ofType(dateTime))"));
  }

  // ========== extractResourceType tests ==========

  @Test
  void extractResourceType_simpleExpression() {
    assertEquals(ResourceType.PATIENT, FhirPathUnionParser.extractResourceType("Patient.name"));
    assertEquals(
        ResourceType.OBSERVATION, FhirPathUnionParser.extractResourceType("Observation.code"));
  }

  @Test
  void extractResourceType_parenthesizedExpression() {
    assertEquals(
        ResourceType.PATIENT, FhirPathUnionParser.extractResourceType("(Patient.name.given)"));
  }

  // ========== parse tests ==========

  @Test
  void parse_simpleQualifiedExpression() {
    final Map<ResourceType, List<String>> result =
        FhirPathUnionParser.parse("Patient.name", List.of(ResourceType.PATIENT));

    assertEquals(Map.of(ResourceType.PATIENT, List.of("Patient.name")), result);
  }

  @Test
  void parse_multiResourceUnion() {
    final Map<ResourceType, List<String>> result =
        FhirPathUnionParser.parse(
            "Patient.name.given | Practitioner.name.given",
            List.of(ResourceType.PATIENT, ResourceType.PRACTITIONER));

    assertEquals(
        Map.of(
            ResourceType.PATIENT, List.of("Patient.name.given"),
            ResourceType.PRACTITIONER, List.of("Practitioner.name.given")),
        result);
  }

  @Test
  void parse_polymorphicSameResource() {
    final Map<ResourceType, List<String>> result =
        FhirPathUnionParser.parse(
            "Observation.effective.ofType(dateTime) | Observation.effective.ofType(Period)",
            List.of(ResourceType.OBSERVATION));

    assertEquals(
        Map.of(
            ResourceType.OBSERVATION,
            List.of(
                "Observation.effective.ofType(dateTime)", "Observation.effective.ofType(Period)")),
        result);
  }

  @Test
  void parse_unqualifiedIncludedForAllBases() {
    // Example with mixed qualified/unqualified expression
    final Map<ResourceType, List<String>> result =
        FhirPathUnionParser.parse(
            "Procedure.occurrence.ofType(Period) | occurrence.ofType(dateTime)",
            List.of(ResourceType.PROCEDURE));

    assertEquals(
        Map.of(
            ResourceType.PROCEDURE,
            List.of("Procedure.occurrence.ofType(Period)", "occurrence.ofType(dateTime)")),
        result);
  }

  @Test
  void parse_dateParameterWithUnqualified() {
    // Simulates the 'date' parameter with unqualified expression for Appointment
    final Map<ResourceType, List<String>> result =
        FhirPathUnionParser.parse(
            "AllergyIntolerance.recordedDate | (start | requestedPeriod.start).first() |"
                + " AuditEvent.recorded",
            List.of(
                ResourceType.ALLERGYINTOLERANCE,
                ResourceType.APPOINTMENT,
                ResourceType.AUDITEVENT));

    assertEquals(
        Map.of(
            ResourceType.ALLERGYINTOLERANCE,
                List.of(
                    "AllergyIntolerance.recordedDate", "(start | requestedPeriod.start).first()"),
            ResourceType.APPOINTMENT, List.of("(start | requestedPeriod.start).first()"),
            ResourceType.AUDITEVENT,
                List.of("AuditEvent.recorded", "(start | requestedPeriod.start).first()")),
        result);
  }

  @Test
  void parse_onlyUnqualified() {
    // Resource with only unqualified expressions
    final Map<ResourceType, List<String>> result =
        FhirPathUnionParser.parse(
            "(start | requestedPeriod.start).first()", List.of(ResourceType.APPOINTMENT));

    assertEquals(
        Map.of(ResourceType.APPOINTMENT, List.of("(start | requestedPeriod.start).first()")),
        result);
  }

  @Test
  void parse_baseNotInQualified() {
    // Base resource has no qualified expression, gets only unqualified
    final Map<ResourceType, List<String>> result =
        FhirPathUnionParser.parse(
            "Patient.name | unqualified.field | Practitioner.name",
            List.of(ResourceType.PATIENT, ResourceType.OBSERVATION, ResourceType.PRACTITIONER));

    assertEquals(
        Map.of(
            ResourceType.PATIENT, List.of("Patient.name", "unqualified.field"),
            ResourceType.OBSERVATION, List.of("unqualified.field"),
            ResourceType.PRACTITIONER, List.of("Practitioner.name", "unqualified.field")),
        result);
  }

  @Test
  void parse_emptyBases() {
    final Map<ResourceType, List<String>> result =
        FhirPathUnionParser.parse("Patient.name", List.of());

    assertTrue(result.isEmpty());
  }
}

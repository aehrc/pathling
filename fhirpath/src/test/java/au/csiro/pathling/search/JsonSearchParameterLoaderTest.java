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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ca.uhn.fhir.context.FhirContext;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link JsonSearchParameterLoader}. */
class JsonSearchParameterLoaderTest {

  private JsonSearchParameterLoader loader;

  @BeforeEach
  void setUp() {
    loader = new JsonSearchParameterLoader(FhirContext.forR4());
  }

  @Test
  void load_simpleParameter() {
    final String json =
        """
        {
          "resourceType": "Bundle",
          "entry": [
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "gender",
                "type": "token",
                "base": ["Patient"],
                "expression": "Patient.gender"
              }
            }
          ]
        }
        """;

    final Map<ResourceType, Map<String, SearchParameterDefinition>> result =
        loader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

    assertNotNull(result.get(ResourceType.PATIENT));
    final SearchParameterDefinition def = result.get(ResourceType.PATIENT).get("gender");
    assertNotNull(def);
    assertEquals("gender", def.code());
    assertEquals(SearchParameterType.TOKEN, def.type());
    assertEquals(List.of("Patient.gender"), def.expressions());
  }

  @Test
  void load_multiResourceParameter() {
    final String json =
        """
        {
          "resourceType": "Bundle",
          "entry": [
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "given",
                "type": "string",
                "base": ["Patient", "Practitioner"],
                "expression": "Patient.name.given | Practitioner.name.given"
              }
            }
          ]
        }
        """;

    final Map<ResourceType, Map<String, SearchParameterDefinition>> result =
        loader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

    // Patient should have Patient.name.given
    final SearchParameterDefinition patientDef = result.get(ResourceType.PATIENT).get("given");
    assertNotNull(patientDef);
    assertEquals(List.of("Patient.name.given"), patientDef.expressions());

    // Practitioner should have Practitioner.name.given
    final SearchParameterDefinition practitionerDef =
        result.get(ResourceType.PRACTITIONER).get("given");
    assertNotNull(practitionerDef);
    assertEquals(List.of("Practitioner.name.given"), practitionerDef.expressions());
  }

  @Test
  void load_polymorphicParameter() {
    final String json =
        """
        {
          "resourceType": "Bundle",
          "entry": [
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "date",
                "type": "date",
                "base": ["Observation"],
                "expression": "Observation.effective.ofType(dateTime) | Observation.effective.ofType(Period)"
              }
            }
          ]
        }
        """;

    final Map<ResourceType, Map<String, SearchParameterDefinition>> result =
        loader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

    final SearchParameterDefinition def = result.get(ResourceType.OBSERVATION).get("date");
    assertNotNull(def);
    assertEquals(SearchParameterType.DATE, def.type());
    assertEquals(
        List.of("Observation.effective.ofType(dateTime)", "Observation.effective.ofType(Period)"),
        def.expressions());
  }

  @Test
  void load_unqualifiedExpression() {
    final String json =
        """
        {
          "resourceType": "Bundle",
          "entry": [
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "date",
                "type": "date",
                "base": ["AllergyIntolerance", "Appointment"],
                "expression": "AllergyIntolerance.recordedDate | (start | requestedPeriod.start).first()"
              }
            }
          ]
        }
        """;

    final Map<ResourceType, Map<String, SearchParameterDefinition>> result =
        loader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

    // AllergyIntolerance gets both qualified and unqualified
    final SearchParameterDefinition allergyDef =
        result.get(ResourceType.ALLERGYINTOLERANCE).get("date");
    assertNotNull(allergyDef);
    assertEquals(
        List.of("AllergyIntolerance.recordedDate", "(start | requestedPeriod.start).first()"),
        allergyDef.expressions());

    // Appointment gets only unqualified (no qualified expression for it)
    final SearchParameterDefinition appointmentDef =
        result.get(ResourceType.APPOINTMENT).get("date");
    assertNotNull(appointmentDef);
    assertEquals(List.of("(start | requestedPeriod.start).first()"), appointmentDef.expressions());
  }

  @Test
  void load_skipsParametersWithoutExpression() {
    final String json =
        """
        {
          "resourceType": "Bundle",
          "entry": [
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "no-expression",
                "type": "token",
                "base": ["Patient"]
              }
            }
          ]
        }
        """;

    final Map<ResourceType, Map<String, SearchParameterDefinition>> result =
        loader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

    assertTrue(
        result.isEmpty()
            || result.get(ResourceType.PATIENT) == null
            || result.get(ResourceType.PATIENT).get("no-expression") == null);
  }

  @Test
  void load_allParameterTypes() {
    final String json =
        """
        {
          "resourceType": "Bundle",
          "entry": [
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "string-param",
                "type": "string",
                "base": ["Patient"],
                "expression": "Patient.name.family"
              }
            },
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "token-param",
                "type": "token",
                "base": ["Patient"],
                "expression": "Patient.gender"
              }
            },
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "date-param",
                "type": "date",
                "base": ["Patient"],
                "expression": "Patient.birthDate"
              }
            },
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "number-param",
                "type": "number",
                "base": ["RiskAssessment"],
                "expression": "RiskAssessment.prediction.probability.ofType(decimal)"
              }
            },
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "quantity-param",
                "type": "quantity",
                "base": ["Observation"],
                "expression": "Observation.value.ofType(Quantity)"
              }
            }
          ]
        }
        """;

    final Map<ResourceType, Map<String, SearchParameterDefinition>> result =
        loader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

    assertEquals(
        SearchParameterType.STRING, result.get(ResourceType.PATIENT).get("string-param").type());
    assertEquals(
        SearchParameterType.TOKEN, result.get(ResourceType.PATIENT).get("token-param").type());
    assertEquals(
        SearchParameterType.DATE, result.get(ResourceType.PATIENT).get("date-param").type());
    assertEquals(
        SearchParameterType.NUMBER,
        result.get(ResourceType.RISKASSESSMENT).get("number-param").type());
    assertEquals(
        SearchParameterType.QUANTITY,
        result.get(ResourceType.OBSERVATION).get("quantity-param").type());
  }

  @Test
  void load_emptyBundle() {
    final String json =
        """
        {
          "resourceType": "Bundle",
          "entry": []
        }
        """;

    final Map<ResourceType, Map<String, SearchParameterDefinition>> result =
        loader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

    assertTrue(result.isEmpty());
  }

  @Test
  void load_multipleParametersForSameResource() {
    final String json =
        """
        {
          "resourceType": "Bundle",
          "entry": [
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "gender",
                "type": "token",
                "base": ["Patient"],
                "expression": "Patient.gender"
              }
            },
            {
              "resource": {
                "resourceType": "SearchParameter",
                "code": "birthdate",
                "type": "date",
                "base": ["Patient"],
                "expression": "Patient.birthDate"
              }
            }
          ]
        }
        """;

    final Map<ResourceType, Map<String, SearchParameterDefinition>> result =
        loader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

    final Map<String, SearchParameterDefinition> patientParams = result.get(ResourceType.PATIENT);
    assertNotNull(patientParams);
    assertEquals(2, patientParams.size());
    assertNotNull(patientParams.get("gender"));
    assertNotNull(patientParams.get("birthdate"));
  }
}

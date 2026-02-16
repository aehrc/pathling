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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ca.uhn.fhir.context.FhirContext;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Enumerations.SearchParamType;
import org.hl7.fhir.r4.model.SearchParameter;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SearchParameterRegistry} factory methods.
 *
 * <p>These are "link tests" that verify the factory methods correctly delegate to the loader.
 * Comprehensive parsing tests are in {@link JsonSearchParameterLoaderTest}.
 */
class SearchParameterRegistryTest {

  private static final FhirContext FHIR_CONTEXT = FhirContext.forR4();

  // ========== fromInputStream tests ==========

  @Test
  void fromInputStream_createsWorkingRegistry() {
    // Basic smoke test - detailed parsing is tested in JsonSearchParameterLoaderTest
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

    final SearchParameterRegistry registry =
        SearchParameterRegistry.fromInputStream(
            FHIR_CONTEXT, new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

    final Optional<SearchParameterDefinition> result =
        registry.getParameter(ResourceType.PATIENT, "gender");

    assertTrue(result.isPresent());
    assertEquals("gender", result.get().code());
    assertEquals(SearchParameterType.TOKEN, result.get().type());
    assertEquals(List.of("Patient.gender"), result.get().expressions());
  }

  @Test
  void fromInputStream_returnsEmptyForUnknownParameter() {
    final String json =
        """
        {
          "resourceType": "Bundle",
          "entry": []
        }
        """;

    final SearchParameterRegistry registry =
        SearchParameterRegistry.fromInputStream(
            FHIR_CONTEXT, new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

    assertTrue(registry.getParameter(ResourceType.PATIENT, "unknown").isEmpty());
  }

  // ========== fromSearchParameters tests ==========

  @Test
  void fromSearchParameters_simpleParameter() {
    final SearchParameter sp = new SearchParameter();
    sp.setCode("active");
    sp.setType(SearchParamType.TOKEN);
    sp.addBase("Patient");
    sp.setExpression("Patient.active");

    final SearchParameterRegistry registry =
        SearchParameterRegistry.fromSearchParameters(List.of(sp));

    final Optional<SearchParameterDefinition> result =
        registry.getParameter(ResourceType.PATIENT, "active");

    assertTrue(result.isPresent());
    assertEquals("active", result.get().code());
    assertEquals(SearchParameterType.TOKEN, result.get().type());
    assertEquals(List.of("Patient.active"), result.get().expressions());
  }

  @Test
  void fromSearchParameters_multipleParameters() {
    final SearchParameter genderParam = new SearchParameter();
    genderParam.setCode("gender");
    genderParam.setType(SearchParamType.TOKEN);
    genderParam.addBase("Patient");
    genderParam.setExpression("Patient.gender");

    final SearchParameter birthdateParam = new SearchParameter();
    birthdateParam.setCode("birthdate");
    birthdateParam.setType(SearchParamType.DATE);
    birthdateParam.addBase("Patient");
    birthdateParam.setExpression("Patient.birthDate");

    final SearchParameterRegistry registry =
        SearchParameterRegistry.fromSearchParameters(List.of(genderParam, birthdateParam));

    assertTrue(registry.getParameter(ResourceType.PATIENT, "gender").isPresent());
    assertTrue(registry.getParameter(ResourceType.PATIENT, "birthdate").isPresent());
  }

  @Test
  void fromSearchParameters_multiResourceParameter() {
    final SearchParameter sp = new SearchParameter();
    sp.setCode("name");
    sp.setType(SearchParamType.STRING);
    sp.addBase("Patient");
    sp.addBase("Practitioner");
    sp.setExpression("Patient.name | Practitioner.name");

    final SearchParameterRegistry registry =
        SearchParameterRegistry.fromSearchParameters(List.of(sp));

    // Each resource gets its own expression
    final Optional<SearchParameterDefinition> patientDef =
        registry.getParameter(ResourceType.PATIENT, "name");
    assertTrue(patientDef.isPresent());
    assertEquals(List.of("Patient.name"), patientDef.get().expressions());

    final Optional<SearchParameterDefinition> practitionerDef =
        registry.getParameter(ResourceType.PRACTITIONER, "name");
    assertTrue(practitionerDef.isPresent());
    assertEquals(List.of("Practitioner.name"), practitionerDef.get().expressions());
  }

  @Test
  void fromSearchParameters_emptyList() {
    final SearchParameterRegistry registry =
        SearchParameterRegistry.fromSearchParameters(List.of());

    assertTrue(registry.getParameter(ResourceType.PATIENT, "anything").isEmpty());
  }

  @Test
  void fromSearchParameters_skipsParameterWithoutExpression() {
    final SearchParameter sp = new SearchParameter();
    sp.setCode("no-expr");
    sp.setType(SearchParamType.TOKEN);
    sp.addBase("Patient");
    // No expression set

    final SearchParameterRegistry registry =
        SearchParameterRegistry.fromSearchParameters(List.of(sp));

    assertTrue(registry.getParameter(ResourceType.PATIENT, "no-expr").isEmpty());
  }

  // ========== getParameters(ResourceType) tests ==========

  @Test
  void getParameters_returnsAllParametersForResourceType() {
    // Given: a registry with multiple parameters for Patient.
    final SearchParameter genderParam = new SearchParameter();
    genderParam.setCode("gender");
    genderParam.setType(SearchParamType.TOKEN);
    genderParam.addBase("Patient");
    genderParam.setExpression("Patient.gender");

    final SearchParameter birthdateParam = new SearchParameter();
    birthdateParam.setCode("birthdate");
    birthdateParam.setType(SearchParamType.DATE);
    birthdateParam.addBase("Patient");
    birthdateParam.setExpression("Patient.birthDate");

    final SearchParameterRegistry registry =
        SearchParameterRegistry.fromSearchParameters(List.of(genderParam, birthdateParam));

    // When: getting all parameters for Patient.
    final Map<String, SearchParameterDefinition> params =
        registry.getParameters(ResourceType.PATIENT);

    // Then: both parameters are returned.
    assertEquals(2, params.size());
    assertTrue(params.containsKey("gender"));
    assertTrue(params.containsKey("birthdate"));
    assertEquals(SearchParameterType.TOKEN, params.get("gender").type());
    assertEquals(SearchParameterType.DATE, params.get("birthdate").type());
  }

  @Test
  void getParameters_returnsEmptyMapForUnknownResourceType() {
    // Given: a registry with parameters only for Patient.
    final SearchParameter sp = new SearchParameter();
    sp.setCode("gender");
    sp.setType(SearchParamType.TOKEN);
    sp.addBase("Patient");
    sp.setExpression("Patient.gender");

    final SearchParameterRegistry registry =
        SearchParameterRegistry.fromSearchParameters(List.of(sp));

    // When: getting parameters for a resource type with no entries.
    final Map<String, SearchParameterDefinition> params =
        registry.getParameters(ResourceType.OBSERVATION);

    // Then: an empty map is returned.
    assertTrue(params.isEmpty());
  }

  @Test
  void getParameters_returnsEmptyMapForEmptyRegistry() {
    // Given: an empty registry.
    final SearchParameterRegistry registry =
        SearchParameterRegistry.fromSearchParameters(List.of());

    // When: getting parameters for any resource type.
    final Map<String, SearchParameterDefinition> params =
        registry.getParameters(ResourceType.PATIENT);

    // Then: an empty map is returned.
    assertTrue(params.isEmpty());
  }

  @Test
  void getParameters_returnsImmutableMap() {
    // Given: a registry with a parameter.
    final SearchParameter sp = new SearchParameter();
    sp.setCode("gender");
    sp.setType(SearchParamType.TOKEN);
    sp.addBase("Patient");
    sp.setExpression("Patient.gender");

    final SearchParameterRegistry registry =
        SearchParameterRegistry.fromSearchParameters(List.of(sp));

    // When: getting parameters and attempting to modify the result.
    final Map<String, SearchParameterDefinition> params =
        registry.getParameters(ResourceType.PATIENT);

    // Then: the map is unmodifiable.
    try {
      params.put("newParam", new SearchParameterDefinition("new", SearchParameterType.TOKEN, "x"));
      // If we get here, the map is mutable — fail.
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (final UnsupportedOperationException e) {
      // Expected.
    }
  }
}

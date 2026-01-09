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

import static au.csiro.pathling.search.SearchParameterType.DATE;
import static au.csiro.pathling.search.SearchParameterType.NUMBER;
import static au.csiro.pathling.search.SearchParameterType.STRING;
import static au.csiro.pathling.search.SearchParameterType.TOKEN;

import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Registry for FHIR search parameter definitions. Currently uses hardcoded parameters, but will be
 * extended to load from the FHIR search parameters JSON registry.
 */
public class SearchParameterRegistry {

  private static final Map<ResourceType, Map<String, SearchParameterDefinition>> PARAMETERS =
      Map.of(
          ResourceType.PATIENT, Map.of(
              "gender", new SearchParameterDefinition("gender", TOKEN, "Patient.gender"),
              "address-use", new SearchParameterDefinition("address-use", TOKEN,
                  "Patient.address.use"),
              "family", new SearchParameterDefinition("family", STRING, "Patient.name.family"),
              "birthdate", new SearchParameterDefinition("birthdate", DATE, "Patient.birthDate"),
              "identifier", new SearchParameterDefinition("identifier", TOKEN, "Patient.identifier"),
              "telecom", new SearchParameterDefinition("telecom", TOKEN, "Patient.telecom"),
              "active", new SearchParameterDefinition("active", TOKEN, "Patient.active")
          ),
          ResourceType.OBSERVATION, Map.of(
              "code", new SearchParameterDefinition("code", TOKEN, "Observation.code")
          ),
          ResourceType.RISKASSESSMENT, Map.of(
              "probability", new SearchParameterDefinition("probability", NUMBER,
                  "RiskAssessment.prediction.probability.ofType(decimal)")
          ),
          ResourceType.COVERAGE, Map.of(
              "period", new SearchParameterDefinition("period", DATE, "Coverage.period")
          ),
          ResourceType.CONDITION, Map.of(
              "recorded-date", new SearchParameterDefinition("recorded-date", DATE,
                  "Condition.recordedDate")
          ),
          ResourceType.AUDITEVENT, Map.of(
              "date", new SearchParameterDefinition("date", DATE, "AuditEvent.recorded")
          )
      );

  /**
   * Gets the search parameter definition for a given resource type and parameter code.
   *
   * @param resourceType the resource type
   * @param code the parameter code
   * @return the parameter definition, or empty if not found
   */
  @Nonnull
  public Optional<SearchParameterDefinition> getParameter(
      @Nonnull final ResourceType resourceType,
      @Nonnull final String code) {
    return Optional.ofNullable(PARAMETERS.get(resourceType))
        .map(params -> params.get(code));
  }
}

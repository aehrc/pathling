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
import static au.csiro.pathling.search.SearchParameterType.QUANTITY;
import static au.csiro.pathling.search.SearchParameterType.REFERENCE;
import static au.csiro.pathling.search.SearchParameterType.STRING;
import static au.csiro.pathling.search.SearchParameterType.TOKEN;
import static au.csiro.pathling.search.SearchParameterType.URI;

import java.util.List;
import java.util.Map;

/**
 * A test registry with hardcoded search parameter definitions for unit testing.
 *
 * <p>This registry provides a minimal set of parameters for testing without requiring JSON loading.
 * Use this in unit tests where deterministic, fast parameter lookup is needed.
 */
public class TestSearchParameterRegistry extends SearchParameterRegistry {

  private static final Map<String, Map<String, SearchParameterDefinition>> TEST_PARAMETERS =
      Map.of(
          "Patient",
              Map.ofEntries(
                  Map.entry(
                      "gender", new SearchParameterDefinition("gender", TOKEN, "Patient.gender")),
                  Map.entry(
                      "address-use",
                      new SearchParameterDefinition("address-use", TOKEN, "Patient.address.use")),
                  Map.entry(
                      "family",
                      new SearchParameterDefinition("family", STRING, "Patient.name.family")),
                  Map.entry(
                      "birthdate",
                      new SearchParameterDefinition("birthdate", DATE, "Patient.birthDate")),
                  Map.entry(
                      "identifier",
                      new SearchParameterDefinition("identifier", TOKEN, "Patient.identifier")),
                  Map.entry(
                      "telecom",
                      new SearchParameterDefinition("telecom", TOKEN, "Patient.telecom")),
                  Map.entry(
                      "active", new SearchParameterDefinition("active", TOKEN, "Patient.active")),
                  Map.entry(
                      "general-practitioner",
                      new SearchParameterDefinition(
                          "general-practitioner", REFERENCE, "Patient.generalPractitioner"))),
          "Observation",
              Map.of(
                  "code", new SearchParameterDefinition("code", TOKEN, "Observation.code"),
                  "date",
                      new SearchParameterDefinition(
                          "date",
                          DATE,
                          List.of(
                              "Observation.effective.ofType(dateTime)",
                              "Observation.effective.ofType(Period)",
                              "Observation.effective.ofType(instant)")),
                  "value-quantity",
                      new SearchParameterDefinition(
                          "value-quantity", QUANTITY, "Observation.value.ofType(Quantity)"),
                  "subject",
                      new SearchParameterDefinition("subject", REFERENCE, "Observation.subject")),
          "RiskAssessment",
              Map.of(
                  "probability",
                  new SearchParameterDefinition(
                      "probability",
                      NUMBER,
                      "RiskAssessment.prediction.probability.ofType(decimal)")),
          "Coverage",
              Map.of("period", new SearchParameterDefinition("period", DATE, "Coverage.period")),
          "Condition",
              Map.of(
                  "recorded-date",
                  new SearchParameterDefinition("recorded-date", DATE, "Condition.recordedDate")),
          "AuditEvent",
              Map.of("date", new SearchParameterDefinition("date", DATE, "AuditEvent.recorded")),
          "CarePlan",
              Map.of(
                  "instantiates-uri",
                  new SearchParameterDefinition(
                      "instantiates-uri", URI, "CarePlan.instantiatesUri")),
          "CapabilityStatement",
              Map.of("url", new SearchParameterDefinition("url", URI, "CapabilityStatement.url")));

  /** Creates a test registry with hardcoded parameters. */
  public TestSearchParameterRegistry() {
    super(TEST_PARAMETERS);
  }
}

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

package au.csiro.pathling.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.DatasetDataSource;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ContactPoint;
import org.hl7.fhir.r4.model.ContactPoint.ContactPointSystem;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Encounter.EncounterStatus;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Enumerations.SearchParamType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationStatus;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.SearchParameter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that a token search tolerates a schema that does not carry a field of the element it
 * matches: the system or code of a Coding, the codings of a CodeableConcept, the system of an
 * Identifier, or the value of a ContactPoint (FR-031, FR-054).
 *
 * <p>A pruned schema carries only the elements the data populates. An absent field matches as a
 * null one does.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class PrunedTokenSearchTest {

  private static final String ID_SYSTEM = "http://example.org/ids";

  private static final String CLASS_SYSTEM = "http://example.org/classes";

  private static final String CODE_SYSTEM = "http://example.org/codes";

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  @TempDir static Path tempDir;

  private final SearchParameterRegistry registry =
      SearchParameterRegistry.fromSearchParameters(
          List.of(
              tokenParameter("Patient", "identifier"),
              tokenParameter("Patient", "telecom"),
              tokenParameter("Encounter", "class"),
              tokenParameter("Observation", "code")));

  private Map<String, Dataset<Row>> datasets;

  @BeforeAll
  void setUp() {
    final PrunedSchemaReader patients =
        PrunedSchemaReader.write(
            encode("Patient", List.of(patient("1", "123", "555"), patient("2", "456", "777"))),
            tempDir.resolve("patients").toString());
    final PrunedSchemaReader observations =
        PrunedSchemaReader.write(
            encode("Observation", List.of(observation("1", "A"), observation("2", "B"))),
            tempDir.resolve("observations").toString());
    final PrunedSchemaReader encounters =
        PrunedSchemaReader.write(
            encode("Encounter", List.of(encounter("1", "AMB"), encounter("2", "IMP"))),
            tempDir.resolve("encounters").toString());
    datasets =
        Map.of(
            "identifierWithoutSystem", patients.readWithout("identifier.system"),
            "telecomWithoutValue", patients.readWithout("telecom.value"),
            "classWithoutSystem", encounters.readWithout("class.system"),
            "classWithoutCode", encounters.readWithout("class.code"),
            "codingWithoutSystem", observations.readWithout("code.coding.system"),
            "conceptWithoutCoding", observations.readWithout("code.coding"));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
        // An Identifier without a system.
        arguments("identifierWithoutSystem", "Patient", "identifier", "456", List.of("2")),
        arguments("identifierWithoutSystem", "Patient", "identifier", "|123", List.of("1")),
        arguments(
            "identifierWithoutSystem", "Patient", "identifier", ID_SYSTEM + "|123", List.of()),
        // A ContactPoint without a value.
        arguments("telecomWithoutValue", "Patient", "telecom", "555", List.of()),
        // A Coding without a system, and one without a code.
        arguments("classWithoutSystem", "Encounter", "class", "AMB", List.of("1")),
        arguments("classWithoutSystem", "Encounter", "class", CLASS_SYSTEM + "|AMB", List.of()),
        arguments("classWithoutCode", "Encounter", "class", CLASS_SYSTEM + "|", List.of("1", "2")),
        arguments("classWithoutCode", "Encounter", "class", "AMB", List.of()),
        // A CodeableConcept whose codings have no system, and one with no codings.
        arguments("codingWithoutSystem", "Observation", "code", "A", List.of("1")),
        arguments("codingWithoutSystem", "Observation", "code", CODE_SYSTEM + "|A", List.of()),
        arguments("conceptWithoutCoding", "Observation", "code", "A", List.of()));
  }

  @ParameterizedTest(name = "{2}={3} over {0}")
  @MethodSource("cases")
  void tokenSearchToleratesAnAbsentField(
      @Nonnull final String dataset,
      @Nonnull final String resourceType,
      @Nonnull final String parameter,
      @Nonnull final String searchValue,
      @Nonnull final List<String> expectedIds) {
    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(
            encoders.getContext(),
            new DatasetDataSource(Map.of(resourceType, datasets.get(dataset))),
            registry);
    final Dataset<Row> results =
        executor.execute(
            ResourceType.fromCode(resourceType),
            FhirSearch.builder().criterion(parameter, searchValue).build());
    assertThat(results.select("id").collectAsList().stream().map(row -> row.getString(0)))
        .containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  @Nonnull
  private Dataset<Row> encode(
      @Nonnull final String resourceType, @Nonnull final List<? extends IBaseResource> resources) {
    return spark
        .createDataset(List.<IBaseResource>copyOf(resources), encoders.of(resourceType))
        .toDF();
  }

  /** A token search parameter on an element of a resource, named for the element. */
  @Nonnull
  private static SearchParameter tokenParameter(
      @Nonnull final String resourceType, @Nonnull final String element) {
    final SearchParameter parameter = new SearchParameter();
    parameter.setCode(element);
    parameter.setType(SearchParamType.TOKEN);
    parameter.addBase(resourceType);
    parameter.setExpression(resourceType + "." + element);
    return parameter;
  }

  @Nonnull
  private static Patient patient(
      @Nonnull final String id, @Nonnull final String identifier, @Nonnull final String phone) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addIdentifier(new Identifier().setSystem(ID_SYSTEM).setValue(identifier));
    patient.addTelecom(new ContactPoint().setSystem(ContactPointSystem.PHONE).setValue(phone));
    return patient;
  }

  @Nonnull
  private static Encounter encounter(@Nonnull final String id, @Nonnull final String code) {
    final Encounter encounter = new Encounter();
    encounter.setId(id);
    encounter.setStatus(EncounterStatus.FINISHED);
    encounter.setClass_(new Coding(CLASS_SYSTEM, code, null));
    return encounter;
  }

  @Nonnull
  private static Observation observation(@Nonnull final String id, @Nonnull final String code) {
    final Observation observation = new Observation();
    observation.setId(id);
    observation.setStatus(ObservationStatus.FINAL);
    observation.setCode(new CodeableConcept().addCoding(new Coding(CODE_SYSTEM, code, null)));
    return observation;
  }
}

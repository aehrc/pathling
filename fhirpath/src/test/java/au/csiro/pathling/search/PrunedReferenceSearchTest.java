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
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Enumerations.SearchParamType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationStatus;
import org.hl7.fhir.r4.model.Reference;
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
 * Tests that a reference search tolerates a schema whose Reference structure does not carry the
 * reference string, as a pruned table does when no stored reference has one (FR-054). An absent
 * reference string matches as a null one does, so it matches no search value, and it matches every
 * negated one.
 *
 * <p>The schema that carries the reference string is the positive control.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class PrunedReferenceSearchTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  @TempDir static Path tempDir;

  private final SearchParameterRegistry registry =
      SearchParameterRegistry.fromSearchParameters(
          List.of(
              referenceParameter("subject", "Observation.subject"),
              referenceParameter("performer", "Observation.performer")));

  private Map<String, Dataset<Row>> datasets;

  @BeforeAll
  void setUp() {
    final PrunedSchemaReader observations =
        PrunedSchemaReader.write(
            LayoutDatasets.fromResources(
                spark,
                encoders,
                TestLayout.active(),
                "Observation",
                List.of(
                    observation("1", "Patient/p1", "Practitioner/pr1"),
                    observation("2", "Patient/p2", "Practitioner/pr2"))),
            tempDir.resolve("observations").toString(),
            encoders.of("Observation").schema());
    datasets =
        Map.of(
            "full", observations.read(),
            "subjectWithoutReference", observations.readWithout("subject.reference"),
            "performerWithoutReference", observations.readWithout("performer.reference"));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
        // The positive control: the reference string is present.
        arguments("full", "subject", "Patient/p1", List.of("1")),
        arguments("full", "subject", "p2", List.of("2")),
        arguments("full", "subject:not", "Patient/p1", List.of("2")),
        arguments("full", "subject:Patient", "p1", List.of("1")),
        arguments("full", "performer", "Practitioner/pr2", List.of("2")),
        // A singular Reference without its reference string.
        arguments("subjectWithoutReference", "subject", "Patient/p1", List.of()),
        arguments("subjectWithoutReference", "subject", "p2", List.of()),
        arguments("subjectWithoutReference", "subject", "http://example.org/Patient/p1", List.of()),
        arguments("subjectWithoutReference", "subject:Patient", "p1", List.of()),
        arguments("subjectWithoutReference", "subject:not", "Patient/p1", List.of("1", "2")),
        // A repeating Reference without its reference string.
        arguments("performerWithoutReference", "performer", "Practitioner/pr2", List.of()),
        arguments("performerWithoutReference", "performer", "pr1", List.of()));
  }

  @ParameterizedTest(name = "{1}={2} over {0}")
  @MethodSource("cases")
  void referenceSearchToleratesAnAbsentReferenceString(
      @Nonnull final String dataset,
      @Nonnull final String parameter,
      @Nonnull final String searchValue,
      @Nonnull final List<String> expectedIds) {
    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(
            encoders.getContext(),
            new DatasetDataSource(Map.of("Observation", datasets.get(dataset))),
            registry);
    final Dataset<Row> results =
        executor.execute(
            ResourceType.OBSERVATION,
            FhirSearch.builder().criterion(parameter, searchValue).build());
    assertThat(results.select("id").collectAsList().stream().map(row -> row.getString(0)))
        .containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  /** A reference search parameter on Observation, with the given expression. */
  @Nonnull
  private static SearchParameter referenceParameter(
      @Nonnull final String code, @Nonnull final String expression) {
    final SearchParameter parameter = new SearchParameter();
    parameter.setCode(code);
    parameter.setType(SearchParamType.REFERENCE);
    parameter.addBase("Observation");
    parameter.setExpression(expression);
    return parameter;
  }

  @Nonnull
  private static Observation observation(
      @Nonnull final String id, @Nonnull final String subject, @Nonnull final String performer) {
    final Observation observation = new Observation();
    observation.setId(id);
    observation.setStatus(ObservationStatus.FINAL);
    observation.setCode(new CodeableConcept().setText("x"));
    observation.setSubject(
        new Reference(subject).setIdentifier(new Identifier().setValue("id-" + id)));
    observation.addPerformer(new Reference(performer).setDisplay("performer " + id));
    return observation;
  }
}

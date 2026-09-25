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

package au.csiro.pathling.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.path.Paths.Traversal;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.DatasetDataSource;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Questionnaire;
import org.hl7.fhir.r4.model.Questionnaire.QuestionnaireItemType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests the type of a view's output column over an element absent from the input schema (FR-028 to
 * FR-030), written ahead of T110 to T112.
 *
 * <p>Each view is run over the full schema, as the control that passed before T110, and over a
 * schema from which the named elements are removed, which passes since T110. The removed elements
 * are never populated in the source, so the pruned run must also return the control's rows.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class ProjectedColumnTypeTest {

  private static final DataType STRING = DataTypes.StringType;

  private static final DataType BOOLEAN = DataTypes.BooleanType;

  private static final DataType INTEGER = DataTypes.IntegerType;

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Autowired Gson gson;

  @TempDir static Path tempDir;

  private Map<String, PrunedSchemaReader> readers;

  @BeforeAll
  void setUp() {
    final Patient patient = new Patient();
    patient.setId("p1");
    patient.setGender(AdministrativeGender.FEMALE);
    patient.addName().addGiven("Ann");

    final Observation observation = new Observation();
    observation.setId("o1");
    observation.setValue(new StringType("x"));

    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("q1");
    questionnaire.setStatus(PublicationStatus.ACTIVE);
    questionnaire.addItem().setLinkId("1").setType(QuestionnaireItemType.DISPLAY);

    readers =
        Map.of(
            "Patient", write("Patient", patient),
            "Observation", write("Observation", observation),
            "Questionnaire", write("Questionnaire", questionnaire));
  }

  // T106: a view column declaring a FHIR type has that type over an absent element (FR-028).

  @Nonnull
  Stream<Arguments> declaredTypes() {
    return Stream.of(
        arguments(
            "Patient",
            List.of("active", "multipleBirthInteger", "birthDate"),
            """
            {
              "column": [
                { "name": "active", "path": "active", "type": "boolean" },
                { "name": "births", "path": "multipleBirth.ofType(integer)", "type": "integer" },
                { "name": "birthDate", "path": "birthDate", "type": "date" }
              ]
            }
            """,
            Map.of("active", BOOLEAN, "births", INTEGER, "birthDate", STRING)),
        arguments(
            "Patient",
            List.of("name.family", "name.prefix"),
            """
            {
              "forEach": "name",
              "column": [
                { "name": "family", "path": "family", "type": "string" },
                { "name": "prefix", "path": "prefix", "type": "string", "collection": true }
              ]
            }
            """,
            Map.of("family", STRING, "prefix", DataTypes.createArrayType(STRING))),
        arguments(
            "Observation",
            List.of("valueQuantity"),
            """
            {
              "column": [
                { "name": "unit", "path": "value.ofType(Quantity).code", "type": "code" }
              ]
            }
            """,
            Map.of("unit", STRING)));
  }

  @ParameterizedTest(name = "{0} {2} over the full schema")
  @MethodSource("declaredTypes")
  void declaredTypeOverFullSchema(
      @Nonnull final String resourceType,
      @Nonnull final List<String> prunedPaths,
      @Nonnull final String selection,
      @Nonnull final Map<String, DataType> expectedTypes) {
    assertColumnTypes(
        run(resourceType, readers.get(resourceType).read(), selection), expectedTypes);
  }

  @ParameterizedTest(name = "{0} {2} with {1} absent from the schema")
  @MethodSource("declaredTypes")
  void declaredTypeIsAppliedOverAbsentElement(
      @Nonnull final String resourceType,
      @Nonnull final List<String> prunedPaths,
      @Nonnull final String selection,
      @Nonnull final Map<String, DataType> expectedTypes) {
    assertPrunedRunMatchesControl(resourceType, prunedPaths, selection, expectedTypes);
  }

  @Test
  void declaredDecimalTypeStaysTextOnOutput() {
    // A declared decimal is exempt from the cast to the declared type (decision 76). A decimal is
    // output as its literal text, so the column is a string on either schema, while getSqlType()
    // still reports DECIMAL(32,6).
    final String selection =
        """
        {
          "column": [
            { "name": "amount", "path": "value.ofType(Quantity).value", "type": "decimal" }
          ]
        }
        """;
    final PrunedSchemaReader reader = readers.get("Observation");
    assertColumnTypes(run("Observation", reader.read(), selection), Map.of("amount", STRING));
    assertColumnTypes(
        run("Observation", reader.readWithout("valueQuantity"), selection),
        Map.of("amount", STRING));
  }

  // T107: a column declaring no type over an absent primitive carries the definitions' type
  // (FR-030), and a column with no type information fails with a helpful message (FR-029).

  @Nonnull
  Stream<Arguments> definitionTypes() {
    return Stream.of(
        arguments(
            "Patient",
            List.of("active", "multipleBirthInteger", "birthDate"),
            """
            {
              "column": [
                { "name": "active", "path": "active" },
                { "name": "births", "path": "multipleBirth.ofType(integer)" },
                { "name": "birthDate", "path": "birthDate" }
              ]
            }
            """,
            Map.of("active", BOOLEAN, "births", INTEGER, "birthDate", STRING)),
        arguments(
            "Patient",
            List.of("name.family", "name.prefix"),
            """
            {
              "forEach": "name",
              "column": [
                { "name": "family", "path": "family" },
                { "name": "prefix", "path": "prefix", "collection": true }
              ]
            }
            """,
            Map.of("family", STRING, "prefix", DataTypes.createArrayType(STRING))),
        arguments(
            "Observation",
            List.of("valueQuantity"),
            """
            {
              "column": [
                { "name": "amount", "path": "value.ofType(Quantity).value" },
                { "name": "unit", "path": "value.ofType(Quantity).code" }
              ]
            }
            """,
            // A decimal with no declared type is output as its literal text wherever it is
            // present, so an absent one must be too.
            Map.of("amount", STRING, "unit", STRING)),
        // The recursive selection path derives an expected element type from its columns, which
        // is where the failure for a column with no type information is raised.
        arguments(
            "Questionnaire",
            List.of("item.text"),
            """
            {
              "repeat": ["item"],
              "column": [
                { "name": "linkId", "path": "linkId" },
                { "name": "text", "path": "text" }
              ]
            }
            """,
            Map.of("linkId", STRING, "text", STRING)));
  }

  @ParameterizedTest(name = "{0} {2} over the full schema")
  @MethodSource("definitionTypes")
  void definitionTypeOverFullSchema(
      @Nonnull final String resourceType,
      @Nonnull final List<String> prunedPaths,
      @Nonnull final String selection,
      @Nonnull final Map<String, DataType> expectedTypes) {
    assertColumnTypes(
        run(resourceType, readers.get(resourceType).read(), selection), expectedTypes);
  }

  @ParameterizedTest(name = "{0} {2} with {1} absent from the schema")
  @MethodSource("definitionTypes")
  void undeclaredColumnOverAbsentPrimitiveCarriesDefinitionType(
      @Nonnull final String resourceType,
      @Nonnull final List<String> prunedPaths,
      @Nonnull final String selection,
      @Nonnull final Map<String, DataType> expectedTypes) {
    assertPrunedRunMatchesControl(resourceType, prunedPaths, selection, expectedTypes);
  }

  @Test
  void columnWithNoTypeInformationFailsNamingColumnPathAndRemedy() {
    // A column that genuinely carries no type information: an empty collection with no FHIR type,
    // and no type declared on the column. The path differs from the name, so that naming one is not
    // mistaken for naming the other.
    final ProjectedColumn column =
        new ProjectedColumn(
            EmptyCollection.getInstance(),
            new RequestedColumn(
                new Traversal("untypedPath"),
                "untypedColumn",
                false,
                Optional.empty(),
                Optional.empty()));

    assertThatThrownBy(column::getSqlType)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("untypedColumn")
        .hasMessageContaining("untypedPath")
        // The remedy is to declare a type on the column.
        .hasMessageContaining("\"type\"");
  }

  // Helpers.

  private void assertPrunedRunMatchesControl(
      @Nonnull final String resourceType,
      @Nonnull final List<String> prunedPaths,
      @Nonnull final String selection,
      @Nonnull final Map<String, DataType> expectedTypes) {
    final PrunedSchemaReader reader = readers.get(resourceType);
    final List<Row> control = run(resourceType, reader.read(), selection).collectAsList();
    final Dataset<Row> pruned =
        run(resourceType, reader.readWithout(prunedPaths.toArray(String[]::new)), selection);
    assertColumnTypes(pruned, expectedTypes);
    assertThat(pruned.collectAsList()).containsExactlyInAnyOrderElementsOf(control);
  }

  private static void assertColumnTypes(
      @Nonnull final Dataset<Row> result, @Nonnull final Map<String, DataType> expectedTypes) {
    final Map<String, DataType> actualTypes =
        Arrays.stream(result.schema().fields())
            .filter(field -> expectedTypes.containsKey(field.name()))
            .collect(Collectors.toMap(StructField::name, StructField::dataType));
    assertThat(actualTypes).isEqualTo(expectedTypes);
  }

  @Nonnull
  private Dataset<Row> run(
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String selection) {
    final String json =
        """
        {
          "resource": "%s",
          "select": [
            { "column": [ { "name": "id", "path": "id" } ] },
            %s
          ]
        }
        """
            .formatted(resourceType, selection);
    final FhirViewExecutor executor =
        new FhirViewExecutor(
            fhirEncoders.getContext(), new DatasetDataSource(Map.of(resourceType, dataset)));
    return executor.buildQuery(gson.fromJson(json, FhirView.class));
  }

  @Nonnull
  private PrunedSchemaReader write(
      @Nonnull final String resourceType, @Nonnull final IBaseResource resource) {
    final Dataset<Row> encoded =
        spark.createDataset(List.of(resource), fhirEncoders.of(resourceType)).toDF();
    return PrunedSchemaReader.write(encoded, tempDir.resolve(resourceType).toString());
  }
}

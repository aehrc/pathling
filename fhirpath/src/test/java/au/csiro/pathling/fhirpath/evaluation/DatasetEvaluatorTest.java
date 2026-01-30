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

package au.csiro.pathling.fhirpath.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.Assertions;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Integration tests for {@link DatasetEvaluator}. */
@SpringBootUnitTest
class DatasetEvaluatorTest {

  private static final Parser PARSER = new Parser();

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  private ObjectDataSource patientDataSource;
  private Dataset<Row> patientDataset;

  @BeforeEach
  void setUp() {
    final Patient patient1 = new Patient();
    patient1.setId("Patient/1");
    patient1.setGender(AdministrativeGender.FEMALE);
    patient1.setActive(true);
    patient1.addName().setFamily("Smith").addGiven("Jane");

    final Patient patient2 = new Patient();
    patient2.setId("Patient/2");
    patient2.setGender(AdministrativeGender.MALE);
    patient2.setActive(false);
    patient2.addName().setFamily("Doe").addGiven("John");

    final Patient patient3 = new Patient();
    patient3.setId("Patient/3");
    // No gender, no active status, no name

    patientDataSource =
        new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3));
    patientDataset = patientDataSource.read("Patient");
  }

  @Nested
  class EvaluationTests {

    @Test
    void evaluate_simpleFieldPath_returnsCollectionDataset() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath fhirPath = PARSER.parse("Patient.name");

      final CollectionDataset result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      assertNotNull(result.getDataset());
      assertNotNull(result.getValue());
      assertTrue(result.getValue().getFhirType().isPresent());
      assertEquals("HumanName", result.getValue().getFhirType().get().toCode());
    }

    @Test
    void evaluate_withDefaultInputContext_usesSubjectResource() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath fhirPath = PARSER.parse("gender");

      final CollectionDataset result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      assertTrue(result.getValue().getFhirType().isPresent());
      assertEquals("code", result.getValue().getFhirType().get().toCode());
    }

    @Test
    void evaluate_withCustomInputContext_usesProvidedContext() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath namePath = PARSER.parse("name");
      final Collection nameCollection = evaluator.evaluateToCollection(namePath);
      final FhirPath familyPath = PARSER.parse("family");

      final CollectionDataset result = evaluator.evaluate(familyPath, nameCollection);

      assertNotNull(result);
      Assertions.assertThat(result).hasClass(StringCollection.class);
    }

    @Test
    void evaluate_nestedPath_returnsCorrectType() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath fhirPath = PARSER.parse("Patient.name.family");

      final CollectionDataset result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      Assertions.assertThat(result).hasClass(StringCollection.class);
      assertTrue(result.getValue().getFhirType().isPresent());
      assertEquals("string", result.getValue().getFhirType().get().toCode());
    }

    @Test
    void evaluate_booleanExpression_returnsBooleanCollection() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath fhirPath = PARSER.parse("Patient.active");

      final CollectionDataset result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      Assertions.assertThat(result).hasClass(BooleanCollection.class);
      assertTrue(result.getValue().getFhirType().isPresent());
      assertEquals("boolean", result.getValue().getFhirType().get().toCode());
    }

    @Test
    void evaluate_comparisonExpression_returnsBooleanCollection() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath fhirPath = PARSER.parse("Patient.gender = 'female'");

      final CollectionDataset result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      Assertions.assertThat(result).hasClass(BooleanCollection.class);
    }
  }

  @Nested
  class EvaluateToCollectionTests {

    @Test
    void evaluateToCollection_returnsCollectionOnly() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath fhirPath = PARSER.parse("Patient.name");

      final Collection result = evaluator.evaluateToCollection(fhirPath);

      assertNotNull(result);
      assertTrue(result.getFhirType().isPresent());
      assertEquals("HumanName", result.getFhirType().get().toCode());
    }

    @Test
    void evaluateToCollection_withInputContext_returnsCollection() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath namePath = PARSER.parse("name");
      final Collection nameCollection = evaluator.evaluateToCollection(namePath);
      final FhirPath familyPath = PARSER.parse("family");

      final Collection result = evaluator.evaluateToCollection(familyPath, nameCollection);

      assertNotNull(result);
      assertTrue(result.getFhirType().isPresent());
      assertEquals("string", result.getFhirType().get().toCode());
    }
  }

  @Nested
  class MetadataTests {

    @Test
    void getSubjectResourceCode_returnsCorrectCode() {
      final DatasetEvaluator evaluator = createPatientEvaluator();

      assertEquals("Patient", evaluator.getSubjectResourceCode());
    }

    @Test
    void getSubjectResourceCode_withObservation_returnsObservation() {
      final Observation observation = new Observation();
      observation.setId("Observation/1");
      final ObjectDataSource observationDataSource =
          new ObjectDataSource(spark, encoders, List.of(observation));
      final Dataset<Row> observationDataset = observationDataSource.read("Observation");

      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create(ResourceType.OBSERVATION, encoders.getContext())
              .withDataset(observationDataset)
              .build();

      assertEquals("Observation", evaluator.getSubjectResourceCode());
    }

    @Test
    void getDefaultInputContext_returnsSubjectResource() {
      final DatasetEvaluator evaluator = createPatientEvaluator();

      final ResourceCollection inputContext = evaluator.getDefaultInputContext();

      assertNotNull(inputContext);
      assertTrue(inputContext.getFhirType().isPresent());
      assertEquals("Patient", inputContext.getFhirType().get().toCode());
    }
  }

  @Nested
  class CollectionDatasetTests {

    @Test
    void toCanonical_returnsCanonicalCollectionDataset() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath fhirPath = PARSER.parse("Patient.name.family");

      final CollectionDataset result = evaluator.evaluate(fhirPath);
      final CollectionDataset canonical = result.toCanonical();

      assertNotNull(canonical);
      assertNotNull(canonical.getDataset());
      assertNotNull(canonical.getValue());
    }

    @Test
    void toIdValueDataset_returnsSimplifiedDataset() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath fhirPath = PARSER.parse("Patient.active");

      final CollectionDataset result = evaluator.evaluate(fhirPath);
      final Dataset<Row> idValueDataset = result.toCanonical().toIdValueDataset();

      assertNotNull(idValueDataset);
      // Should have "id" and "value" columns
      assertEquals(2, idValueDataset.columns().length);
      assertEquals("id", idValueDataset.columns()[0]);
      assertEquals("value", idValueDataset.columns()[1]);
    }

    @Test
    void getValueColumn_returnsColumn() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath fhirPath = PARSER.parse("Patient.active");

      final CollectionDataset result = evaluator.evaluate(fhirPath);

      assertNotNull(result.getValueColumn());
    }
  }

  @Nested
  class CrossResourceStrategyTests {

    @Test
    void defaultStrategy_throwsOnForeignResourceReference() {
      final DatasetEvaluator evaluator = createPatientEvaluator();
      final FhirPath fhirPath = PARSER.parse("Observation");

      assertThrows(UnsupportedOperationException.class, () -> evaluator.evaluate(fhirPath));
    }

    @Test
    void emptyStrategy_allowsForeignResourceReference() {
      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, encoders.getContext())
              .withDataset(patientDataset)
              .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
              .build();

      final FhirPath fhirPath = PARSER.parse("Observation");
      final CollectionDataset result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      assertTrue(result.getValue().getFhirType().isPresent());
      assertEquals("Observation", result.getValue().getFhirType().get().toCode());
    }
  }

  @Nested
  class DataSourceIntegrationTests {

    @Test
    void builder_withDataSource_readsDataset() {
      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, encoders.getContext())
              .withDataSource(patientDataSource)
              .build();

      final FhirPath fhirPath = PARSER.parse("Patient.active");
      final CollectionDataset result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      // Verify evaluation works with data from the data source
      Assertions.assertThat(result)
          .hasClass(BooleanCollection.class)
          .toCanonicalResult()
          .hasRowsUnordered(
              RowFactory.create("1", true),
              RowFactory.create("2", false),
              RowFactory.create("3", null));
    }
  }

  @Nonnull
  private DatasetEvaluator createPatientEvaluator() {
    return DatasetEvaluatorBuilder.create(ResourceType.PATIENT, encoders.getContext())
        .withDataset(patientDataset)
        .build();
  }
}

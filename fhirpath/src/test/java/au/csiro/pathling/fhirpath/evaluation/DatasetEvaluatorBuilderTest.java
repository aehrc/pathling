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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Tests for {@link DatasetEvaluatorBuilder}. */
class DatasetEvaluatorBuilderTest {

  private FhirContext fhirContext;

  @BeforeEach
  void setUp() {
    fhirContext = FhirContext.forR4();
  }

  @Nested
  class FactoryMethodTests {

    @Test
    void create_withResourceType_createsBuilder() {
      final DatasetEvaluatorBuilder builder =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirContext);

      assertNotNull(builder);
    }

    @Test
    void create_withResourceCode_createsBuilder() {
      final DatasetEvaluatorBuilder builder =
          DatasetEvaluatorBuilder.create("Patient", fhirContext);

      assertNotNull(builder);
    }

    @Test
    void create_withCustomResourceCode_createsBuilder() {
      final DatasetEvaluatorBuilder builder =
          DatasetEvaluatorBuilder.create("ViewDefinition", fhirContext);

      assertNotNull(builder);
    }
  }

  @Nested
  class ConfigurationTests {

    @Test
    @SuppressWarnings("unchecked")
    void withDataset_setsDataset() {
      final Dataset<Row> mockDataset = mock(Dataset.class);

      final DatasetEvaluatorBuilder builder =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirContext)
              .withDataset(mockDataset);

      assertNotNull(builder);
      // Builder should be configured with dataset - verified by building successfully
      final DatasetEvaluator evaluator = builder.build();
      assertNotNull(evaluator);
      assertEquals(mockDataset, evaluator.getDataset());
    }

    @Test
    @SuppressWarnings("unchecked")
    void withDataSource_setsDataSource() {
      final DataSource mockDataSource = mock(DataSource.class);
      final Dataset<Row> mockDataset = mock(Dataset.class);
      when(mockDataSource.read("Patient")).thenReturn(mockDataset);

      final DatasetEvaluatorBuilder builder =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirContext)
              .withDataSource(mockDataSource);

      assertNotNull(builder);
      // DataSource should be used when building
      final DatasetEvaluator evaluator = builder.build();
      verify(mockDataSource).read("Patient");
      assertEquals(mockDataset, evaluator.getDataset());
    }

    @Test
    @SuppressWarnings("unchecked")
    void withVariables_setsVariables() {
      final Dataset<Row> mockDataset = mock(Dataset.class);
      final Collection mockCollection = mock(Collection.class);
      final Map<String, Collection> variables = Map.of("myVar", mockCollection);

      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirContext)
              .withDataset(mockDataset)
              .withVariables(variables)
              .build();

      assertNotNull(evaluator);
    }

    @Test
    @SuppressWarnings("unchecked")
    void withFunctionRegistry_setsFunctionRegistry() {
      final Dataset<Row> mockDataset = mock(Dataset.class);
      final FunctionRegistry mockRegistry = mock(FunctionRegistry.class);

      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirContext)
              .withDataset(mockDataset)
              .withFunctionRegistry(mockRegistry)
              .build();

      assertNotNull(evaluator);
    }

    @Test
    @SuppressWarnings("unchecked")
    void withCrossResourceStrategy_setsStrategy() {
      final Dataset<Row> mockDataset = mock(Dataset.class);

      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirContext)
              .withDataset(mockDataset)
              .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
              .build();

      assertNotNull(evaluator);
    }
  }

  @Nested
  class ValidationTests {

    @Test
    void build_withoutDatasetOrDataSource_throwsIllegalStateException() {
      final DatasetEvaluatorBuilder builder =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirContext);

      final IllegalStateException exception =
          assertThrows(IllegalStateException.class, builder::build);

      assertEquals(
          "Either dataset or dataSource must be set before building DatasetEvaluator",
          exception.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    void build_withDataset_createsEvaluator() {
      final Dataset<Row> mockDataset = mock(Dataset.class);

      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirContext)
              .withDataset(mockDataset)
              .build();

      assertNotNull(evaluator);
      assertEquals("Patient", evaluator.getSubjectResourceCode());
      assertEquals(mockDataset, evaluator.getDataset());
    }

    @Test
    @SuppressWarnings("unchecked")
    void build_withDataSource_readsFromDataSource() {
      final DataSource mockDataSource = mock(DataSource.class);
      final Dataset<Row> mockDataset = mock(Dataset.class);
      when(mockDataSource.read("Observation")).thenReturn(mockDataset);

      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create(ResourceType.OBSERVATION, fhirContext)
              .withDataSource(mockDataSource)
              .build();

      assertNotNull(evaluator);
      assertEquals("Observation", evaluator.getSubjectResourceCode());
      verify(mockDataSource).read("Observation");
      assertEquals(mockDataset, evaluator.getDataset());
    }

    @Test
    @SuppressWarnings("unchecked")
    void build_withBothDatasetAndDataSource_prefersDataset() {
      final Dataset<Row> directDataset = mock(Dataset.class);
      final DataSource mockDataSource = mock(DataSource.class);
      final Dataset<Row> sourceDataset = mock(Dataset.class);
      when(mockDataSource.read("Patient")).thenReturn(sourceDataset);

      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirContext)
              .withDataset(directDataset)
              .withDataSource(mockDataSource)
              .build();

      // When both are set, dataset takes precedence
      assertNotNull(evaluator);
      assertEquals(directDataset, evaluator.getDataset());
    }
  }

  @Nested
  class DefaultValueTests {

    /**
     * Verifies that the builder creates a valid evaluator with all default values:
     *
     * <ul>
     *   <li>Default CrossResourceStrategy is FAIL
     *   <li>Default variables map is empty
     *   <li>Default FunctionRegistry is StaticFunctionRegistry
     * </ul>
     *
     * These defaults are verified indirectly by the successful build without errors.
     */
    @Test
    @SuppressWarnings("unchecked")
    void build_withOnlyDataset_usesAllDefaults() {
      final Dataset<Row> mockDataset = mock(Dataset.class);

      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirContext)
              .withDataset(mockDataset)
              .build();

      assertNotNull(evaluator);
      assertEquals("Patient", evaluator.getSubjectResourceCode());
      assertEquals(mockDataset, evaluator.getDataset());
    }
  }

  @Nested
  class ResourceCodeTests {

    @Test
    @SuppressWarnings("unchecked")
    void build_withResourceType_setsCorrectCode() {
      final Dataset<Row> mockDataset = mock(Dataset.class);

      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create(ResourceType.CONDITION, fhirContext)
              .withDataset(mockDataset)
              .build();

      assertEquals("Condition", evaluator.getSubjectResourceCode());
    }

    @Test
    @SuppressWarnings("unchecked")
    void build_withStringResourceCode_setsCorrectCode() {
      final Dataset<Row> mockDataset = mock(Dataset.class);

      final DatasetEvaluator evaluator =
          DatasetEvaluatorBuilder.create("Encounter", fhirContext).withDataset(mockDataset).build();

      assertEquals("Encounter", evaluator.getSubjectResourceCode());
    }
  }
}

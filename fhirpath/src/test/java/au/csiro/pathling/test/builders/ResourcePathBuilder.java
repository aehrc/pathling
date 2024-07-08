/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.builders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.fixtures.PatientResourceRowFixture;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
public class ResourcePathBuilder {

  @Nonnull
  private FhirContext fhirContext;

  @Nonnull
  private DataSource dataSource;

  @Nonnull
  private ResourceType resourceType;

  @Nonnull
  private String expression;

  @Nonnull
  private Dataset<Row> dataset;

  @Nonnull
  private Column idColumn;

  @Nonnull
  private Optional<Column> eidColumn = Optional.empty();

  @Nonnull
  private Column valueColumn;

  private boolean singular;

  @Nullable
  private Column thisColumn;

  public ResourcePathBuilder(@Nonnull final SparkSession spark) {
    fhirContext = mock(FhirContext.class);
    dataSource = mock(DataSource.class);
    dataset = PatientResourceRowFixture.createCompleteDataset(spark);
    when(dataSource.read(any(ResourceType.class))).thenReturn(dataset);
    resourceType = ResourceType.PATIENT;
    expression = "Patient";
    idColumn = dataset.col(dataset.columns()[0]);
    valueColumn = idColumn;
    singular = false;
    thisColumn = null;
  }

  @Nonnull
  public ResourcePathBuilder idAndValueColumns() {
    idColumn = functions.col(dataset.columns()[0]);
    valueColumn = idColumn;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder idEidAndValueColumns() {
    idColumn = functions.col(dataset.columns()[0]);
    eidColumn = Optional.of(functions.col(dataset.columns()[1]));
    valueColumn = idColumn;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder fhirContext(final FhirContext fhirContext) {
    this.fhirContext = fhirContext;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder database(final DataSource dataSource) {
    this.dataSource = dataSource;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder resourceType(final ResourceType resourceType) {
    this.resourceType = resourceType;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder expression(final String expression) {
    this.expression = expression;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder dataset(final Dataset<Row> dataset) {
    this.dataset = dataset;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder idColumn(final Column idColumn) {
    this.idColumn = idColumn;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder valueColumn(final Column valueColumn) {
    this.valueColumn = valueColumn;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder singular(final boolean singular) {
    this.singular = singular;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder thisColumn(final Column thisColumn) {
    this.thisColumn = thisColumn;
    return this;
  }

  // TODO: check

  //
  // @Nonnull
  // public ResourceCollection build() {
  //   return ResourceCollection.build(fhirContext, dataSource, resourceType, expression);
  // }
  //
  // @Nonnull
  // public ResourceCollection buildCustom() {
  //   final String resourceCode = resourceType.toCode();
  //   final RuntimeResourceDefinition hapiDefinition = fhirContext
  //       .getResourceDefinition(resourceCode);
  //   final ResourceDefinition definition = new ResourceDefinition(resourceType, hapiDefinition,
  //       Optional.empty());
  //   // in most cases value column should be the same as id
  //   final DatasetWithColumn datasetWithColumn = createColumn(dataset, valueColumn);
  //
  //   final Map<String, Column> elementsToColumns = new HashMap<>();
  //   for (final String columnName : dataset.columns()) {
  //     elementsToColumns.put(columnName, dataset.col(columnName));
  //   }
  //
  //   try {
  //     final Constructor<ResourceCollection> constructor = ResourceCollection.class
  //         .getDeclaredConstructor(String.class, Dataset.class, Column.class, Optional.class,
  //             Column.class, boolean.class, Optional.class, ResourceDefinition.class, Map.class);
  //     constructor.setAccessible(true);
  //     return constructor
  //         .newInstance(expression, datasetWithColumn.getDataset(), idColumn, eidColumn,
  //             datasetWithColumn.getColumn(), singular, Optional.ofNullable(thisColumn), definition,
  //             elementsToColumns);
  //   } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException |
  //                  InstantiationException e) {
  //     throw new RuntimeException("Problem building ResourcePath", e);
  //   }
  // }

}

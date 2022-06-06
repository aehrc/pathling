/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.builders;

import static au.csiro.pathling.QueryHelpers.createColumn;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.fixtures.PatientResourceRowFixture;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
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
  private Database database;

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
    database = mock(Database.class);
    dataset = PatientResourceRowFixture.createCompleteDataset(spark);
    when(database.read(any(ResourceType.class))).thenReturn(dataset);
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
  public ResourcePathBuilder database(final Database database) {
    this.database = database;
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

  @Nonnull
  public ResourcePath build() {
    return ResourcePath.build(fhirContext, database, resourceType, expression, singular);
  }

  @Nonnull
  public ResourcePath buildCustom() {
    final String resourceCode = resourceType.toCode();
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(resourceCode);
    final ResourceDefinition definition = new ResourceDefinition(resourceType, hapiDefinition);
    // in most cases value column should be the same as id
    final DatasetWithColumn datasetWithColumn = createColumn(dataset, valueColumn);

    final Map<String, Column> elementsToColumns = new HashMap<>();
    for (final String columnName : dataset.columns()) {
      elementsToColumns.put(columnName, dataset.col(columnName));
    }

    try {
      final Constructor<ResourcePath> constructor = ResourcePath.class
          .getDeclaredConstructor(String.class, Dataset.class, Column.class, Optional.class,
              Column.class, boolean.class, Optional.class, ResourceDefinition.class, Map.class);
      constructor.setAccessible(true);
      return constructor
          .newInstance(expression, datasetWithColumn.getDataset(), idColumn, eidColumn,
              datasetWithColumn.getColumn(), singular, Optional.ofNullable(thisColumn), definition,
              elementsToColumns);
    } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException |
                   InstantiationException e) {
      throw new RuntimeException("Problem building ResourcePath", e);
    }
  }

}

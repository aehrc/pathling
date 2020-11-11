/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.builders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.fixtures.PatientResourceRowFixture;
import au.csiro.pathling.test.helpers.SparkHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
public class ResourcePathBuilder {

  @Nonnull
  private FhirContext fhirContext;

  @Nonnull
  private ResourceReader resourceReader;

  @Nonnull
  private ResourceType resourceType;

  @Nonnull
  private String expression;

  @Nonnull
  private Dataset<Row> dataset;

  @Nonnull
  private Column idColumn;

  @Nonnull
  private List<Column> valueColumns;

  private boolean singular;

  @Nullable
  private List<Column> thisColumns;

  public ResourcePathBuilder() {
    fhirContext = mock(FhirContext.class);
    resourceReader = mock(ResourceReader.class);
    final SparkSession spark = SparkHelpers.getSparkSession();
    dataset = PatientResourceRowFixture.createCompleteDataset(spark);
    when(resourceReader.read(any(ResourceType.class))).thenReturn(dataset);
    resourceType = ResourceType.PATIENT;
    expression = "Patient";
    idColumn = dataset.col(dataset.columns()[0]);
    valueColumns = Stream.of(dataset.columns())
        .map(dataset::col)
        .collect(Collectors.toList());
    singular = false;
    thisColumns = null;
  }

  @Nonnull
  public ResourcePathBuilder fhirContext(final FhirContext fhirContext) {
    this.fhirContext = fhirContext;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder resourceReader(final ResourceReader resourceReader) {
    this.resourceReader = resourceReader;
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
  public ResourcePathBuilder valueColumns(final List<Column> valueColumns) {
    this.valueColumns = valueColumns;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder singular(final boolean singular) {
    this.singular = singular;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder thisColumns(final List<Column> thisColumns) {
    this.thisColumns = thisColumns;
    return this;
  }

  @Nonnull
  public ResourcePath build() {
    return ResourcePath.build(fhirContext, resourceReader, resourceType, expression, singular);
  }

  @Nonnull
  public ResourcePath buildCustom() {
    final String resourceCode = resourceType.toCode();
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(resourceCode);
    final ResourceDefinition definition = new ResourceDefinition(resourceType, hapiDefinition);

    final Map<String, Column> elementsToColumns = new HashMap<>();
    for (final String columnName : dataset.columns()) {
      elementsToColumns.put(columnName, dataset.col(columnName));
    }

    try {
      final Method build = ResourcePath.class
          .getDeclaredMethod("build", String.class, Dataset.class, Optional.class, List.class,
              boolean.class, Optional.class, ResourceDefinition.class, Map.class);
      build.setAccessible(true);
      return (ResourcePath) build
          .invoke(ResourcePath.class, expression, dataset, Optional.of(idColumn), valueColumns,
              singular, Optional.ofNullable(thisColumns), definition, elementsToColumns);
    } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Problem building ResourcePath", e);
    }
  }

}

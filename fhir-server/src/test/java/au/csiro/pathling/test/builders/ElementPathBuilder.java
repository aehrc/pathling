/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.builders;

import static au.csiro.pathling.test.helpers.SparkHelpers.getIdAndValueColumns;
import static org.apache.spark.sql.functions.lit;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.SparkHelpers;
import au.csiro.pathling.test.helpers.SparkHelpers.IdAndValueColumns;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * @author John Grimes
 */
public class ElementPathBuilder {

  @Nonnull
  private FhirPath parentPath;

  @Nonnull
  private String expression;

  @Nonnull
  private Dataset<Row> dataset;

  @Nonnull
  private Column valueColumn;

  private boolean singular;

  @Nonnull
  private ElementDefinition definition;

  @Nonnull
  private Column idColumn;

  @Nonnull
  private FHIRDefinedType fhirType;

  public ElementPathBuilder() {
    parentPath = mock(FhirPath.class);
    expression = "";
    dataset = SparkHelpers.getSparkSession().emptyDataFrame();
    valueColumn = lit(null);
    singular = false;
    definition = mock(ElementDefinition.class);
    idColumn = lit(null);
    fhirType = FHIRDefinedType.NULL;
  }

  @Nonnull
  public ElementPathBuilder idAndValueColumns() {
    final IdAndValueColumns idAndValueColumns = getIdAndValueColumns(dataset);
    idColumn = idAndValueColumns.getId();
    valueColumn = idAndValueColumns.getValue();
    return this;
  }

  @Nonnull
  public ElementPathBuilder parentPath(@Nonnull final FhirPath parentPath) {
    this.parentPath = parentPath;
    return this;
  }

  @Nonnull
  public ElementPathBuilder expression(@Nonnull final String expression) {
    this.expression = expression;
    return this;
  }

  @Nonnull
  public ElementPathBuilder dataset(@Nonnull final Dataset<Row> dataset) {
    this.dataset = dataset;
    return this;
  }

  @Nonnull
  public ElementPathBuilder valueColumn(@Nonnull final Column valueColumn) {
    this.valueColumn = valueColumn;
    return this;
  }

  @Nonnull
  public ElementPathBuilder singular(final boolean singular) {
    this.singular = singular;
    return this;
  }

  @Nonnull
  public ElementPathBuilder definition(@Nonnull final ElementDefinition definition) {
    this.definition = definition;
    return this;
  }

  @Nonnull
  public ElementPathBuilder withDefinitionFromResource(
      Class<? extends IBaseResource> resourceClazz, String elementName) {
    RuntimeResourceDefinition resourceDefinition = FhirHelpers.getFhirContext()
        .getResourceDefinition(resourceClazz);
    return definition(ElementDefinition.build(resourceDefinition.getChildByName(elementName), elementName));
  }

  @Nonnull
  public ElementPathBuilder idColumn(@Nonnull final Column idColumn) {
    this.idColumn = idColumn;
    return this;
  }

  @Nonnull
  public ElementPathBuilder fhirType(@Nonnull final FHIRDefinedType fhirType) {
    this.fhirType = fhirType;
    return this;
  }

  @Nonnull
  public ElementPath build() {
    return ElementPath
        .build(expression, dataset, Optional.of(idColumn), valueColumn, singular, fhirType);
  }

  @Nonnull
  public ElementPath buildDefined() {
    // This defaults the ID column to that of this path, if a parent path was not provided. This
    // prevents us from having to create parent paths for every element path we create in the tests.
    if (!parentPath.getIdColumn().isPresent()) {
      when(parentPath.getIdColumn()).thenReturn(Optional.of(idColumn));
    }
    return ElementPath.build(parentPath, expression, dataset, valueColumn, singular, definition);
  }

}

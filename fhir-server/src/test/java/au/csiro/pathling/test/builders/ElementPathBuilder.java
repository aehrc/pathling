/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.builders;

import static au.csiro.pathling.test.helpers.SparkHelpers.getIdAndValueColumns;
import static org.apache.spark.sql.functions.col;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.test.helpers.SparkHelpers.IdAndValueColumns;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * @author John Grimes
 */
public class ElementPathBuilder {

  @Nonnull
  private String expression;

  @Nonnull
  private Dataset<Row> dataset;

  @Nonnull
  private Column idColumn;

  @Nonnull
  private Column valueColumn;

  private boolean singular;

  @Nonnull
  private FHIRDefinedType fhirType;

  @Nullable
  private ResourcePath foreignResource;

  @Nullable
  private Column thisColumn;

  @Nonnull
  private ElementDefinition definition;

  public ElementPathBuilder() {
    expression = "";
    dataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .build();
    idColumn = col(dataset.columns()[0]);
    valueColumn = col(dataset.columns()[1]);
    singular = false;
    fhirType = FHIRDefinedType.NULL;
    definition = mock(ElementDefinition.class);
  }

  @Nonnull
  public ElementPathBuilder idAndValueColumns() {
    final IdAndValueColumns idAndValueColumns = getIdAndValueColumns(dataset);
    idColumn = idAndValueColumns.getId();
    valueColumn = idAndValueColumns.getValues().get(0);
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
  public ElementPathBuilder idColumn(@Nonnull final Column idColumn) {
    this.idColumn = idColumn;
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
  public ElementPathBuilder fhirType(@Nonnull final FHIRDefinedType fhirType) {
    this.fhirType = fhirType;
    return this;
  }

  @Nonnull
  public ElementPathBuilder foreignResource(@Nonnull final ResourcePath foreignResource) {
    this.foreignResource = foreignResource;
    return this;
  }

  @Nonnull
  public ElementPathBuilder thisColumn(@Nonnull final Column thisColumn) {
    this.thisColumn = thisColumn;
    return this;
  }

  @Nonnull
  public ElementPathBuilder definition(@Nonnull final ElementDefinition definition) {
    this.definition = definition;
    return this;
  }

  @Nonnull
  public ElementPath build() {
    return ElementPath
        .build(expression, dataset, idColumn, valueColumn, singular,
            Optional.ofNullable(foreignResource), Optional.ofNullable(thisColumn), fhirType);
  }

  @Nonnull
  public ElementPath buildDefined() {
    return ElementPath
        .build(expression, dataset, idColumn, valueColumn, singular,
            Optional.ofNullable(foreignResource), Optional.ofNullable(thisColumn), definition);
  }

}

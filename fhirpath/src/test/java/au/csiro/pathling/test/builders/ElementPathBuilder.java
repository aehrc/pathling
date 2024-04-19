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

import static au.csiro.pathling.test.helpers.SparkHelpers.getIdAndValueColumns;
import static org.apache.spark.sql.functions.col;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.test.helpers.SparkHelpers.IdAndValueColumns;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
  private Optional<Column> eidColumn;

  @Nonnull
  private Column valueColumn;

  private boolean singular;

  @Nonnull
  private FHIRDefinedType fhirType;

  @Nullable
  private ResourcePath currentResource;

  @Nullable
  private Column thisColumn;

  @Nonnull
  private ElementDefinition definition;

  public ElementPathBuilder(@Nonnull final SparkSession spark) {
    expression = "";
    dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .build();
    idColumn = col(dataset.columns()[0]);
    valueColumn = col(dataset.columns()[1]);
    eidColumn = Optional.empty();
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
  public ElementPathBuilder idAndEidAndValueColumns() {
    final IdAndValueColumns idAndValueColumns = getIdAndValueColumns(dataset, true);
    idColumn = idAndValueColumns.getId();
    eidColumn = idAndValueColumns.getEid();
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
  public ElementPathBuilder currentResource(@Nonnull final ResourcePath currentResource) {
    this.currentResource = currentResource;
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
        .build(expression, dataset, idColumn, eidColumn,
            valueColumn, singular, Optional.ofNullable(currentResource),
            Optional.ofNullable(thisColumn), fhirType);
  }

  @Nonnull
  public ElementPath buildDefined() {
    return ElementPath
        .build(expression, dataset, idColumn, eidColumn,
            valueColumn, singular, Optional.ofNullable(currentResource),
            Optional.ofNullable(thisColumn), definition);
  }
}

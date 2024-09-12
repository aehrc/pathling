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

import static org.apache.spark.sql.functions.col;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * @author John Grimes
 */
public class UntypedResourcePathBuilder {

  @Nonnull
  private String expression;

  @Nonnull
  private Dataset<Row> dataset;

  @Nonnull
  private Column idColumn;

  @Nonnull
  private Column valueColumn;

  private boolean singular;

  @Nullable
  private Column thisColumn;

  public UntypedResourcePathBuilder(@Nonnull final SparkSession spark) {
    expression = "";
    dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .build();
    idColumn = col(dataset.columns()[0]);
    valueColumn = col(dataset.columns()[1]);
    singular = false;
  }

  @Nonnull
  public UntypedResourcePathBuilder idAndValueColumns() {
    idColumn = functions.col(dataset.columns()[0]);
    valueColumn = functions.col(dataset.columns()[1]);
    return this;
  }

  @Nonnull
  public UntypedResourcePathBuilder idEidAndValueColumns() {
    idColumn = functions.col(dataset.columns()[0]);
    valueColumn = functions.col(dataset.columns()[2]);
    return this;
  }

  @Nonnull
  public UntypedResourcePathBuilder expression(@Nonnull final String expression) {
    this.expression = expression;
    return this;
  }

  @Nonnull
  public UntypedResourcePathBuilder dataset(@Nonnull final Dataset<Row> dataset) {
    this.dataset = dataset;
    return this;
  }

  @Nonnull
  public UntypedResourcePathBuilder valueColumn(@Nonnull final Column valueColumn) {
    this.valueColumn = valueColumn;
    return this;
  }

  @Nonnull
  public UntypedResourcePathBuilder singular(final boolean singular) {
    this.singular = singular;
    return this;
  }

  @Nonnull
  public UntypedResourcePathBuilder thisColumn(@Nonnull final Column thisColumn) {
    this.thisColumn = thisColumn;
    return this;
  }

  // TODO: check
  //
  // @Nonnull
  // public UntypedResourcePath build() {
  //   final ReferencePath referencePath = mock(ReferencePath.class);
  //   when(referencePath.getIdColumn()).thenReturn(idColumn);
  //   when(referencePath.getValueColumn()).thenReturn(valueColumn);
  //   when(referencePath.getDataset()).thenReturn(dataset);
  //   when(referencePath.isSingular()).thenReturn(singular);
  //   when(referencePath.getCurrentResource()).thenReturn(Optional.empty());
  //   when(referencePath.getThisColumn()).thenReturn(Optional.ofNullable(thisColumn));
  //   when(referencePath.getFhirType()).thenReturn(FHIRDefinedType.REFERENCE);
  //   return UntypedResourcePath.build(referencePath, expression);
  // }

}

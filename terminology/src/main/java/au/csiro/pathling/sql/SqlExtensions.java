/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql;

import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;


/**
 * Custom dataset operations.
 */
public interface SqlExtensions {

  /**
   * Creates a new {@link Dataset} with the additional column as specified in {@code resultField}
   * with the results of mapping each of the rows in the input dataset. The mapping extracts the
   * value of the specified input column from the input dataset and the uses the provided mapper to
   * (a) create a state for each partition, and; (b) to map the values for each row given access to
   * per partition state.
   *
   * @param ds the input dataset
   * @param inputColumn the column to extract from the input dataset
   * @param columnDecoder the decoder to use to convert the extracted column value to the input type
   * of the mapper
   * @param mapper the mapping operation to use. The mapper will be allowed to preview all input
   * objects in the dataset partition and create the state for them
   * @param resultField the definition of the column with the result of the mapping
   * @param <I> input type of the mapper
   * @param <R> result type of the mapper
   * @param <S> state type of the mapper
   * @return the dataset with an additional column as specified in the resultField with the results
   * of the mapping operation for each row
   */
  @Nonnull
  static <I, R, S> Dataset<Row> mapWithPartitionPreview(@Nonnull final Dataset<Row> ds,
      @Nonnull final Column inputColumn,
      @Nonnull final ObjectDecoder<I> columnDecoder,
      @Nonnull final MapperWithPreview<I, R, S> mapper, @Nonnull final StructField resultField) {
    return Dataset.ofRows(ds.sparkSession(),

        MapWithPartitionPreview
            .fromJava(inputColumn.expr(), columnDecoder, mapper, resultField, ds.logicalPlan()));
  }
}

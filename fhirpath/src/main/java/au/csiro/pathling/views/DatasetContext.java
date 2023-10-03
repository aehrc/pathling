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

package au.csiro.pathling.views;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;

import static au.csiro.pathling.utilities.Strings.randomAlias;

/**
 * Wrapper that allows to apply side effect operations to the dataset. This is needed to support the
 * forEach clause, and more specifically to materialize exploded columns.
 * TODO: consider refactoring this somehow futher loclalize the side-effect
 * possibly using something similar to the functional IO monad (i.e. accumulate
 * the necessary transformations of the dataset rather then applying it directly)
 */
@Getter
@AllArgsConstructor
public class DatasetContext {

  @Nonnull
  Dataset<Row> dataset;

  /**
   * Takes a given column and return an alias to the materialized version of it, that is an alias to
   * evaluated projection of the column. This is needed for some types of expressions, e.g.:
   * `explode` (so most likely all generators) in order to be able to use the column in further
   * expressions, including being able to traverse to its fields.
   *
   * @param column the column to materialize
   * @return the materialized column
   */
  @Nonnull
  public Column materialize(@Nonnull final Column column, final boolean removeNulls) {
    final String materializedColumnName = randomAlias();
    dataset = dataset.withColumn(materializedColumnName, column);
    if (removeNulls) {
      dataset = dataset.filter(dataset.col(materializedColumnName).isNotNull());
    }
    return dataset.col(materializedColumnName);
  }
}

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

package au.csiro.pathling.fhirpath;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Represents any FHIRPath expression - all expressions implement this interface.
 *
 * @author John Grimes
 */
public interface FhirPath extends Orderable {

  /**
   * @return the FHIRPath expression that represents this path
   */
  @Nonnull
  String getExpression();

  /**
   * @return the {@link Dataset} that can be used to evaluate this path against data
   */
  @Nonnull
  Dataset<Row> getDataset();

  /**
   * @return a {@link Column} within the dataset containing the identity of the subject resource
   */
  @Nonnull
  Column getIdColumn();

  /**
   * @return a {@link Column} within the dataset containing the values of the nodes
   */
  @Nonnull
  Column getValueColumn();

  /**
   * @return an indicator of whether this path represents a single-valued collection
   */
  boolean isSingular();

  /**
   * @param target the path to test
   * @return an indicator of whether this path's values can be combined into a single collection
   * with values from the supplied expression type
   */
  boolean canBeCombinedWith(@Nonnull FhirPath target);

  /**
   * Creates a copy of the path with a different expression.
   *
   * @param expression the new expression
   * @return the new FhirPath
   */
  @Nonnull
  FhirPath withExpression(@Nonnull String expression);

  /**
   * Trims the columns to those common with the target and sorts them, ready for a union operation.
   * The value column of the path is always included as the last column of the dataset.
   *
   * @param target the expression that this path will be combined with
   * @return a new {@link Dataset} with a subset of columns
   */
  @Nonnull
  Dataset<Row> getUnionableDataset(@Nonnull final FhirPath target);

  /**
   * Creates a path that can be used to represent a collection which includes elements from both a
   * source and a target path.
   *
   * @param target the path the merge the invoking path with
   * @param dataset the {@link Dataset} that can be used to evaluate this path against data
   * @param expression the FHIRPath expression that represents the result
   * @param idColumn a {@link Column} within the dataset containing the identity of the subject
   * resource
   * @param eidColumn a {@link Column} that represents the unique ID for an element within a
   * collection
   * @param valueColumn a {@link Column} within the dataset containing the values of the nodes
   * @param singular an indicator of whether this path represents a single-valued collection
   * @param thisColumn for paths that traverse from the {@code $this} keyword, this column refers to
   * the values in the collection
   * @return the resulting new {@link NonLiteralPath}
   */
  @Nonnull
  NonLiteralPath combineWith(@Nonnull FhirPath target, @Nonnull Dataset<Row> dataset,
      @Nonnull String expression, @Nonnull Column idColumn, @Nonnull Optional<Column> eidColumn,
      @Nonnull Column valueColumn, boolean singular, @Nonnull Optional<Column> thisColumn);


  /**
   * Prints out to stdout all the ids and values of all the elements in this path. For debugging
   * purposes only.
   */
  default void dumpAll() {
    getDataset().select(getIdColumn(), getValueColumn()).collectAsList()
        .forEach(System.out::println);
  }

}

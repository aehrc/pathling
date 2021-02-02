/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import java.util.Optional;
import javax.annotation.Nonnull;
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
  NonLiteralPath mergeWith(FhirPath target, Dataset<Row> dataset, String expression,
      Column idColumn, Optional<Column> eidColumn, Column valueColumn, boolean singular,
      Optional<Column> thisColumn);

}

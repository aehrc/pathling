/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.parser.ParserContext;
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
   * Trims the path's dataset down to only essential rows, ready for a union operation.
   *
   * @param context the current {@link ParserContext}, used for detecting things like $this and
   * grouping context
   * @return a new {@link Dataset} with a subset of columns
   */
  @Nonnull
  Dataset<Row> trimDataset(@Nonnull final ParserContext context);

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

}

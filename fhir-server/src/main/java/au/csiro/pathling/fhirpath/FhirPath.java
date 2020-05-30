/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.element.ElementDefinition;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Represents any FHIRPath expression - all expressions implement this interface.
 *
 * @author John Grimes
 */
public interface FhirPath {

  /**
   * Returns the FHIRPath expression that represents this path.
   *
   * @return A FHIRPath string.
   */
  @Nonnull
  String getExpression();

  /**
   * Returns the {@link Dataset} that can be used to evaluate this path against data.
   *
   * @return A {@link Dataset}
   */
  @Nonnull
  Dataset<Row> getDataset();

  /**
   * Returns a {@link Column} within the dataset containing the identity of the subject resource
   *
   * @return A {@link Column}
   */
  @Nonnull
  Column getIdColumn();

  /**
   * Returns a {@link Column} within the dataset containing the values of the nodes.
   *
   * @return A {@link Column}
   */
  @Nonnull
  Column getValueColumn();

  /**
   * Returns an indicator of whether this path represents a single-valued collection.
   *
   * @return {@code true} if this path represents a single-valued collection
   */
  boolean isSingular();

  /**
   * Returns the specified child of this path, if there is one.
   *
   * @param name The name of the child element
   * @return An {@link ElementDefinition} object
   */
  @Nonnull
  Optional<ElementDefinition> getChildElement(@Nonnull String name);

  /**
   * Returns the resource value column from the resource at the root of this path.
   * <p>
   * This is required for reverse reference resolution, where we need to get to the resource to be
   * joined to the source of the reverse resolve operation.
   *
   * @return A {@link Column}
   */
  @Nonnull
  Optional<Column> getOriginColumn();

  /**
   * Returns the resource type of the resource at the root of this path.
   * <p>
   * This is required for reverse reference resolution, where we need to get to the resource to be
   * joined to the source of the reverse resolve operation.
   *
   * @return A {@link ResourceDefinition}
   */
  @Nonnull
  Optional<ResourceDefinition> getOriginType();

  /**
   * Builds a comparison function for directly comparable paths.
   *
   * @param source The path to build the comparison function for
   * @param sparkFunction The Spark column function to use
   * @return A new {@link Function}
   */
  @Nonnull
  static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
      final BiFunction<Column, Column, Column> sparkFunction) {
    return target -> sparkFunction.apply(source.getValueColumn(), target.getValueColumn());
  }

}

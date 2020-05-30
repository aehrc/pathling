/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Describes a path that is able to be compared with other paths, e.g. for equality.
 *
 * @author John Grimes
 */
public interface Comparable {

  /**
   * Get a function that can take two Comparable paths and return a {@link Column} that contains a
   * comparison condition. The type of condition is controlled by supplying a Spark column function,
   * e.g. {@link Column#equalTo}.
   *
   * @param sparkFunction A Spark column function
   * @return A {@link Function} that takes a Comparable as its parameter, and returns a {@link
   * Column}
   */
  Function<Comparable, Column> getComparison(BiFunction<Column, Column, Column> sparkFunction);

  /**
   * Returns a {@link Column} within the dataset containing the values of the nodes.
   *
   * @return A {@link Column}
   */
  Column getValueColumn();

  /**
   * @param type A subtype of {@link FhirPath}
   * @return {@code true} if this path can be compared to the specified class
   */
  boolean isComparableTo(@Nonnull Class<? extends Comparable> type);

}

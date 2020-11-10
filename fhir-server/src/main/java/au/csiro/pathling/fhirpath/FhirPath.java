/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import java.util.Optional;
import java.util.stream.Stream;
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
   * Returns the FHIRPath expression that represents this path.
   *
   * @return a FHIRPath string
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
   * Returns a {@link Column} within the dataset containing the identity of the subject resource.
   * This is optional as sometimes we can have paths that do not contain a resource identity, e.g. a
   * path representing the result of an aggregation over groupings.
   *
   * @return a {@link Column}
   */
  @Nonnull
  Optional<Column> getIdColumn();

  /**
   * Returns a {@link Column} within the dataset containing the values of the nodes.
   *
   * @return a {@link Column}
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
   * Gets an ID {@link Column} from any of the inputs, if there is one.
   *
   * @param inputs a collection of objects
   * @return a {@link Column}, if one was found
   */
  @Nonnull
  static Optional<Column> findIdColumn(@Nonnull final Object... inputs) {
    return Stream.of(inputs)
        .filter(path -> path instanceof FhirPath)
        .map(path -> (FhirPath) path)
        .filter(fhirPath -> fhirPath.getIdColumn().isPresent())
        .findFirst()
        .flatMap(FhirPath::getIdColumn);
  }

}

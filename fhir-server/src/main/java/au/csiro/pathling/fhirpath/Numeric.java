/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Describes a path that represents a numeric value, and can be the subject of math operations.
 *
 * @author John Grimes
 */
public interface Numeric {

  /**
   * Get a function that can take two Numeric paths and return a {@link NonLiteralPath} that
   * contains the result of a math operation. The type of operation is controlled by supplying a
   * {@link MathOperation}.
   *
   * @param operation The {@link MathOperation} type to retrieve a result for
   * @param expression The FHIRPath expression to use within the result
   * @param dataset The {@link Dataset} to use within the result
   * @return A {@link Function} that takes a Numeric as its parameter, and returns a {@link
   * NonLiteralPath}.
   */
  @Nonnull
  Function<Numeric, NonLiteralPath> getMathOperation(@Nonnull MathOperation operation,
      @Nonnull String expression, @Nonnull Dataset<Row> dataset);

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
   * Represents a type of math operator.
   */
  enum MathOperation {
    /**
     * Addition operator.
     */
    ADDITION("+", Column::plus),
    /**
     * Subtraction operator.
     */
    SUBTRACTION("-", Column::minus),
    /**
     * Multiplication operator.
     */
    MULTIPLICATION("*", Column::multiply),
    /**
     * Division operator.
     */
    DIVISION("/", Column::divide),
    /**
     * Modulus operator.
     */
    MODULUS("mod", Column::mod);

    @Nonnull
    private final String fhirPath;

    /**
     * A Spark function that can be used to execute this type of math operation for simple types.
     * Complex types such as Quantity will implement their own math operation functions.
     */
    @Nonnull
    @Getter
    private final BiFunction<Column, Column, Column> sparkFunction;

    MathOperation(@Nonnull final String fhirPath,
        @Nonnull final BiFunction<Column, Column, Column> sparkFunction) {
      this.fhirPath = fhirPath;
      this.sparkFunction = sparkFunction;
    }

    @Override
    public String toString() {
      return fhirPath;
    }

  }
}

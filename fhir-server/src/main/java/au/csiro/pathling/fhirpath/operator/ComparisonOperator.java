/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.QueryHelpers.joinOnId;
import static au.csiro.pathling.fhirpath.operator.Operator.buildExpression;
import static au.csiro.pathling.fhirpath.operator.Operator.checkArgumentsAreComparable;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Provides the functionality of the family of comparison operators within FHIRPath, i.e. =, !=, <=,
 * <, >, >=.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#equality">Equality</a>
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#comparison">Comparison</a>
 */
public class ComparisonOperator implements Operator {

  @Nonnull
  private final ComparisonOperatorType type;

  /**
   * @param type The type of operator
   */
  public ComparisonOperator(@Nonnull final ComparisonOperatorType type) {
    this.type = type;
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final OperatorInput input) {
    checkArgumentsAreComparable(input, type.toString());

    final FhirPath left = input.getLeft();
    final FhirPath right = input.getRight();
    final String expression = buildExpression(input, type.toString());

    final Dataset<Row> dataset = joinOnId(left, right, JoinType.LEFT_OUTER);

    final Comparable leftComparable = (Comparable) left;
    final Comparable rightComparable = (Comparable) right;
    final Column valueColumn = leftComparable.getComparison(type.getSparkFunction())
        .apply(rightComparable);

    return new BooleanPath(expression, dataset,
        left.getIdColumn(), valueColumn, true, FHIRDefinedType.BOOLEAN);
  }

  /**
   * Represents a type of comparison operator.
   */
  public enum ComparisonOperatorType {
    /**
     * The equals operator.
     */
    EQUALS("=", Column::equalTo),

    /**
     * The not equals operator.
     */
    NOT_EQUALS("!=", Column::notEqual),

    /**
     * The less than or equal to operator.
     */
    LESS_THAN_OR_EQUAL_TO("<=", Column::leq),

    /**
     * The less than operator.
     */
    LESS_THAN("<", Column::lt),

    /**
     * The greater than or equal to operator.
     */
    GREATER_THAN_OR_EQUAL_TO(">=", Column::geq),

    /**
     * The greater than operator.
     */
    GREATER_THAN(">", Column::gt);

    @Nonnull
    private final String fhirPath;

    @Nonnull
    private final BiFunction<Column, Column, Column> sparkFunction;

    ComparisonOperatorType(@Nonnull final String fhirPath,
        @Nonnull final BiFunction<Column, Column, Column> sparkFunction) {
      this.fhirPath = fhirPath;
      this.sparkFunction = sparkFunction;
    }

    @Nonnull
    public BiFunction<Column, Column, Column> getSparkFunction() {
      return sparkFunction;
    }

    @Override
    public String toString() {
      return fhirPath;
    }

  }

}

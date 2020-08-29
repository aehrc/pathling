/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.QueryHelpers.updateGroupingColumns;
import static au.csiro.pathling.fhirpath.operator.Operator.checkArgumentsAreComparable;
import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.QueryHelpers.IdAndValue;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.Comparable.ComparisonOperation;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * An expression that tests whether a singular value is present within a collection.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#membership">Membership</a>
 */
public class MembershipOperator implements Operator {

  private final MembershipOperatorType type;

  /**
   * @param type The type of operator
   */
  public MembershipOperator(final MembershipOperatorType type) {
    this.type = type;
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final OperatorInput input) {
    final FhirPath left = input.getLeft();
    final FhirPath right = input.getRight();
    final FhirPath element = type.equals(MembershipOperatorType.IN)
                             ? left
                             : right;
    final FhirPath collection = type.equals(MembershipOperatorType.IN)
                                ? right
                                : left;
    final ParserContext context = input.getContext();
    final Optional<Column> leftIdColumn = left.getIdColumn();

    checkUserInput(element.isSingular(),
        "Element operand used with " + type + " operator is not singular: " + element
            .getExpression());
    checkArgumentsAreComparable(input, type.toString());
    check(context.getGroupBy().isPresent() || leftIdColumn.isPresent());

    final String expression =
        left.getExpression() + " " + type + " " + right.getExpression();
    final Comparable leftComparable = (Comparable) left;
    final Comparable rightComparable = (Comparable) right;
    final Column equality = leftComparable.getComparison(ComparisonOperation.EQUALS)
        .apply(rightComparable);

    // If the left-hand side of the operator (element) is empty, the result is empty. If the
    // right-hand side (collection) is empty, the result is false. Otherwise, a Boolean is returned
    // based on whether the element is present in the collection, using equality semantics.
    final Column equalityWithNullChecks = when(element.getValueColumn().isNull(), lit(null))
        .when(collection.getValueColumn().isNull(), lit(false))
        .otherwise(equality);

    // In order to reduce the result to a single Boolean, we take the max of the boolean equality
    // values.
    final Column aggColumn = max(equalityWithNullChecks).as("value");

    // Group by the grouping columns if present, or the ID column from the input.
    @SuppressWarnings("OptionalGetWithoutIsPresent") final Dataset<Row> dataset = QueryHelpers
        .joinOnId(left, right, JoinType.LEFT_OUTER)
        .groupBy(context.getGroupBy().orElse(new Column[]{leftIdColumn.get()}))
        .agg(aggColumn);

    // If there were grouping columns, there will no longer be an ID column.
    final Optional<Column> updatedIdColumn = context.getGroupBy().isPresent()
                                             ? Optional.empty()
                                             : leftIdColumn;

    final IdAndValue idAndValue = updateGroupingColumns(context, dataset, updatedIdColumn);
    return new BooleanPath(expression, dataset, idAndValue.getIdColumn(),
        idAndValue.getValueColumn(), true, FHIRDefinedType.BOOLEAN);
  }

  /**
   * Represents a type of membership operator.
   */
  public enum MembershipOperatorType {
    /**
     * Contains operator
     */
    CONTAINS("contains"),
    /**
     * In operator
     */
    IN("in");

    @Nonnull
    private final String fhirPath;

    MembershipOperatorType(@Nonnull final String fhirPath) {
      this.fhirPath = fhirPath;
    }

    @Override
    public String toString() {
      return fhirPath;
    }
  }

}

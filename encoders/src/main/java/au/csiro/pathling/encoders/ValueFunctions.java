/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
 *
 */

package au.csiro.pathling.encoders;

import static org.apache.spark.sql.classic.ExpressionUtils.column;
import static org.apache.spark.sql.classic.ExpressionUtils.expression;

import au.csiro.pathling.sql.PruneSyntheticFields;
import jakarta.annotation.Nonnull;
import java.util.function.UnaryOperator;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.classic.ColumnConversions$;

/**
 * Java-based utility class for value-based Column operations. This class uses Java to access
 * package-private methods in Spark that are not accessible from Scala.
 */
@UtilityClass
public class ValueFunctions {

  /**
   * Helper method to lift a Column-based operator to an Expression-based operator.
   *
   * @param columExpression The Column-based operator
   * @return The Expression-based operator
   */
  @Nonnull
  private static UnaryOperator<Expression> liftToExpression(
      @Nonnull UnaryOperator<Column> columExpression) {
    // This needs to be used rather than ExpressionUtils.expression()
    // to correctly unwrap the underlying expression.
    return e -> ColumnConversions$.MODULE$.toRichColumn(
            columExpression.apply(column(e)))
        .expr();
  }

  /**
   * Applies an expression to an array value, or an else expression if the value is not an array.
   *
   * @param value The value to check
   * @param arrayExpression The expression to apply to the array value
   * @param elseExpression The expression to apply if the value is not an array
   * @return A Column with the conditional expression
   */
  @Nonnull
  public static Column ifArray(
      @Nonnull final Column value,
      @Nonnull final UnaryOperator<Column> arrayExpression,
      @Nonnull final UnaryOperator<Column> elseExpression) {

    return column(
        new UnresolvedIfArray(
            expression(value),
            liftToExpression(arrayExpression)::apply,
            liftToExpression(elseExpression)::apply
        ));
  }

  /**
   * Applies an expression to array of arrays value, or an else expression if the value is an array.
   * Throws an exception if the value is not an array.
   *
   * @param value The value to check
   * @param arrayExpression The expression to apply to the array of arrays value
   * @param elseExpression The expression to apply to the arrays of non-array values
   * @return A Column with the conditional expression
   */
  @Nonnull
  public static Column ifArray2(
      @Nonnull final Column value,
      @Nonnull final UnaryOperator<Column> arrayExpression,
      @Nonnull final UnaryOperator<Column> elseExpression) {
    return column(
        new UnresolvedIfArray2(
            expression(value),
            liftToExpression(arrayExpression)::apply,
            liftToExpression(elseExpression)::apply
        ));
  }

  /**
   * Unnests an array column by flattening nested arrays.
   *
   * @param value The column to unnest
   * @return A Column with the unnested expression
   */
  @Nonnull
  public static Column unnest(@Nonnull final Column value) {
    final Expression valueExpr = expression(value);
    final Expression unnestExpr = new UnresolvedUnnest(valueExpr);
    return column(unnestExpr);
  }

  /**
   * Removes all fields starting with '_' (underscore) from struct values.
   * <p>
   * This function is used to clean up internal/synthetic fields from FHIR resources before
   * presenting them to users. Fields that don't start with underscore are preserved. Non-struct
   * values are not affected by this function.
   *
   * @param col The column containing struct values to prune
   * @return A new column with underscore-prefixed fields removed from structs
   */
  @Nonnull
  public static Column pruneAnnotations(@Nonnull final Column col) {
    return column(new PruneSyntheticFields(expression(col)));
  }
}

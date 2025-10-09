/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific and Industrial Research
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
 *
 */

package au.csiro.pathling.encoders;

import jakarta.annotation.Nonnull;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.classic.ColumnConversions$;
import org.apache.spark.sql.classic.ExpressionUtils;

/**
 * Java-based utility class for value-based Column operations. This class uses Java to access
 * package-private methods in Spark that are not accessible from Scala.
 */
public class ValueFunctions {

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
      @Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> elseExpression) {

    final Expression valueExpr = ExpressionUtils.expression(value);

    final Function<Expression, Expression> arrayExprFunc = e ->
        // This needs to be user rather than ExpressionUtils.expression() 
        // to correctly unwrap the underlying expression.
        ColumnConversions$.MODULE$.toRichColumn(arrayExpression.apply(ExpressionUtils.column(e)))
            .expr();

    final Function<Expression, Expression> elseExprFunc = e ->
        ColumnConversions$.MODULE$.toRichColumn(elseExpression.apply(ExpressionUtils.column(e)))
            .expr();

    final Expression resultExpr = new UnresolvedIfArray(
        valueExpr,
        arrayExprFunc::apply,
        elseExprFunc::apply
    );

    return ExpressionUtils.column(resultExpr);
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
      @Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> elseExpression) {

    final Expression valueExpr = ExpressionUtils.expression(value);

    final Function<Expression, Expression> arrayExprFunc = e ->
        ExpressionUtils.expression(arrayExpression.apply(ExpressionUtils.column(e)));

    final Function<Expression, Expression> elseExprFunc = e ->
        ExpressionUtils.expression(elseExpression.apply(ExpressionUtils.column(e)));

    final Expression resultExpr = new UnresolvedIfArray2(
        valueExpr,
        arrayExprFunc::apply,
        elseExprFunc::apply
    );

    return ExpressionUtils.column(resultExpr);
  }

  /**
   * Unnests an array column by flattening nested arrays.
   *
   * @param value The column to unnest
   * @return A Column with the unnested expression
   */
  @Nonnull
  public static Column unnest(@Nonnull final Column value) {
    final Expression valueExpr = ExpressionUtils.expression(value);
    final Expression unnestExpr = new UnresolvedUnnest(valueExpr);
    return ExpressionUtils.column(unnestExpr);
  }
}

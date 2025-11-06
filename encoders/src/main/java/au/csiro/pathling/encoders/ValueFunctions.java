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
import java.util.List;
import java.util.function.UnaryOperator;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.classic.ColumnConversions$;
import scala.Function1;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.FunctionConverters;

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
   * Wraps a column expression to resolve to null if the field is not found during resolution.
   * <p>
   * This is useful for handling optional fields in nested structures where the field may not exist
   * in all instances. Instead of throwing an error when a field is not found, this function will
   * return null.
   *
   * @param value The column expression to wrap
   * @return A new column that resolves to null if the field is not found
   */
  @Nonnull
  public static Column nullIfUnresolved(@Nonnull final Column value) {
    final Expression valueExpr = expression(value);
    final Expression nullOrExpr = new UnresolvedNullIfUnresolved(valueExpr);
    return column(nullOrExpr);
  }


  /**
   * Performs a recursive tree traversal with value extraction at each level.
   * <p>
   * This method implements a depth-first traversal of nested structures, applying a sequence
   * of traversal operations recursively and extracting values at each level. The result is
   * a flattened array containing all extracted values from the tree traversal.
   * </p>
   * <p>
   * The traversal process works as follows:
   * <ol>
   *   <li>Apply the extractor to the current value to get the result for this level</li>
   *   <li>For each traversal operation, apply it to the current value to get child values</li>
   *   <li>Recursively apply the same process to each child value</li>
   *   <li>Concatenate all results into a single array</li>
   * </ol>
   * </p>
   * <p>
   * This is particularly useful for traversing self-referential FHIR structures like
   * Questionnaire.item, where items can contain nested items. The method handles missing
   * fields gracefully by returning empty arrays when fields are not found.
   * </p>
   *
   * @param value The starting value column to traverse
   * @param extractor An extraction operation to apply at each node to get the desired value
   * @param traversals A list of traversal operations to apply recursively to reach child nodes
   * @return A Column containing an array of all extracted values from the tree traversal
   */
  @Nonnull
  public static Column transformTree(@Nonnull final Column value,
      @Nonnull final UnaryOperator<Column> extractor,
      @Nonnull final List<UnaryOperator<Column>> traversals
  ) {

    final List<Function1<Expression, Expression>> x = traversals.stream()
        .map(ValueFunctions::liftToExpression)
        .map(FunctionConverters::asScalaFromUnaryOperator)
        .toList();

    final Seq<Function1<Expression, Expression>> scalaSeq = CollectionConverters.asScala(x).toSeq();
    return column(new UnresolvedTransformTree(
        expression(value),
        liftToExpression(extractor)::apply,
        scalaSeq
        ));
  }

  /**
   * Performs a recursive tree traversal with value extraction at each level using a single
   * traversal operation.
   * <p>
   * This is a convenience overload of {@link #transformTree(Column, UnaryOperator, List)} for
   * the common case where there is only one traversal operation to apply recursively.
   * </p>
   *
   * @param value The starting value column to traverse
   * @param extractor An extraction operation to apply at each node to get the desired value
   * @param traversal A single traversal operation to apply recursively to reach child nodes
   * @return A Column containing an array of all extracted values from the tree traversal
   * @see #transformTree(Column, UnaryOperator, List)
   */
  @Nonnull
  public static Column transformTree(@Nonnull final Column value,
      @Nonnull final UnaryOperator<Column> extractor, @Nonnull final UnaryOperator<Column> traversal
  ) {
    return transformTree(value, extractor, List.of(traversal));
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

/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
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
      @Nonnull final UnaryOperator<Column> columExpression) {
    // This needs to be used rather than ExpressionUtils.expression()
    // to correctly unwrap the underlying expression.
    return e -> ColumnConversions$.MODULE$.toRichColumn(columExpression.apply(column(e))).expr();
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
            liftToExpression(elseExpression)::apply));
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
            liftToExpression(elseExpression)::apply));
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
   * Returns SQL NULL when a struct field doesn't exist in the schema, instead of throwing an error.
   *
   * <p>This function is essential for handling optional fields in nested structures where a field
   * may not be present in all instances of a struct type. When the specified field is missing from
   * the struct schema, this returns null rather than causing a FIELD_NOT_FOUND analysis error.
   *
   * <p><strong>Important:</strong> This only handles fields that don't exist in the schema. If a
   * field exists but has a null value, that null value is returned normally.
   *
   * <p><strong>Typical usage:</strong>
   *
   * <pre>{@code
   * // Safe access to optional field that may not exist in all structs
   * dataset.withColumn("email", nullIfMissingField(col("person").getField("email")))
   * }</pre>
   *
   * @param value The column expression that may reference a non-existent field
   * @return A column that resolves to null if the field is not found in the schema, or the field's
   *     value (including null) if the field exists
   * @see org.apache.spark.sql.Column#getField(String)
   */
  @Nonnull
  public static Column nullIfMissingField(@Nonnull final Column value) {
    final Expression valueExpr = expression(value);
    final Expression nullOrExpr = new UnresolvedNullIfMissingField(valueExpr);
    return column(nullOrExpr);
  }

  /**
   * Returns an empty array when a struct field doesn't exist in the schema, instead of throwing an
   * error. This is similar to {@link #nullIfMissingField} but returns an empty array instead of
   * null, making it suitable for use in array concatenation contexts where type compatibility is
   * required.
   *
   * @param value The column expression that may reference a non-existent field
   * @return A column that resolves to an empty array if the field is not found in the schema, or
   *     the field's value if the field exists
   */
  @Nonnull
  public static Column emptyArrayIfMissingField(@Nonnull final Column value) {
    final Expression valueExpr = expression(value);
    final Expression emptyOrExpr = new UnresolvedEmptyArrayIfMissingField(valueExpr);
    return column(emptyOrExpr);
  }

  /**
   * Performs a recursive tree traversal with value extraction at each level.
   *
   * <p>This method implements a depth-first traversal of nested structures, applying a sequence of
   * traversal operations recursively and extracting values at each level. The result is a flattened
   * array containing all extracted values from the tree traversal.
   *
   * <p>The traversal process works as follows:
   *
   * <ol>
   *   <li>Apply the extractor to the current value to get the result for this level
   *   <li>For each traversal operation, apply it to the current value to get child values
   *   <li>Recursively apply the same process to each child value
   *   <li>Concatenate all results into a single array
   * </ol>
   *
   * <p>This is particularly useful for traversing self-referential FHIR structures like
   * QuestionnaireResponse.item, where items can contain nested items through multiple paths (e.g.,
   * item.item and item.answer.item). The method handles missing fields gracefully by returning
   * empty arrays when fields are not found.
   *
   * <p><strong>Depth Limiting:</strong> The {@code maxDepth} parameter controls recursion depth to
   * prevent infinite loops in self-referential structures. Critically, the depth counter only
   * increments when traversing to a node of the same type as its parent. This allows finite paths
   * through different types to traverse deeper than {@code maxDepth} while still preventing
   * infinite recursion in truly self-referential cases. For example, traversing through alternating
   * types (Item → Answer → Item) will not count against the depth limit, but traversing with an
   * identity function (Item → Item) will be limited.
   *
   * <p><strong>Important Requirements:</strong>
   *
   * <ul>
   *   <li>The {@code extractor} function must return an array type. If you need to extract a scalar
   *       value, wrap it in an array (e.g., {@code c -> functions.array(c.getField("id"))}).
   *   <li>The array type returned by the {@code extractor} must be consistent across all traversed
   *       nodes. Mixing different array element types will result in a type mismatch error.
   * </ul>
   *
   * @param value The starting value column to traverse
   * @param extractor An extraction operation to apply at each node that must return an array type
   * @param traversals A list of traversal operations to apply recursively to reach child nodes
   * @param maxDepth The maximum recursion depth for same-type traversals to prevent infinite loops
   * @return A Column containing an array of all extracted values from the tree traversal
   */
  @Nonnull
  public static Column transformTree(
      @Nonnull final Column value,
      @Nonnull final UnaryOperator<Column> extractor,
      @Nonnull final List<UnaryOperator<Column>> traversals,
      final int maxDepth) {

    final List<Function1<Expression, Expression>> x =
        traversals.stream()
            .map(ValueFunctions::liftToExpression)
            .map(FunctionConverters::asScalaFromUnaryOperator)
            .toList();

    final Seq<Function1<Expression, Expression>> scalaSeq = CollectionConverters.asScala(x).toSeq();
    return column(
        new UnresolvedTransformTree(
            expression(value), liftToExpression(extractor)::apply, scalaSeq, maxDepth));
  }

  /**
   * Removes all fields starting with '_' (underscore) from struct values.
   *
   * <p>This function is used to clean up internal/synthetic fields from FHIR resources before
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

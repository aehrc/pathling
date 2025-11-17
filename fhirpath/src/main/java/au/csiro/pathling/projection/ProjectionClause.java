/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
 */

package au.csiro.pathling.projection;

import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import jakarta.annotation.Nonnull;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;

/**
 * Represents a component of an abstract query for projecting FHIR data.
 */
public interface ProjectionClause {

  /**
   * Converts this clause into a result using the supplied context.
   *
   * @param context The context in which to evaluate this clause
   * @return The result of evaluating this clause
   */
  @Nonnull
  ProjectionResult evaluate(@Nonnull final ProjectionContext context);

  /**
   * Converts this projection clause into a column operator that can be applied to individual
   * elements in a collection.
   * <p>
   * This is useful for per-element evaluation in transformations like unnesting and recursive
   * traversal. The returned operator evaluates this projection clause using the provided column as
   * input context.
   * </p>
   *
   * @param context The projection context containing execution environment
   * @return A unary operator that takes a column and returns the projection result column
   */
  @Nonnull
  default UnaryOperator<Column> asColumnOperator(@Nonnull final ProjectionContext context) {
    return column -> evaluate(context.withInputColumn(column)).getResultColumn();
  }


  /**
   * Evaluates this projection clause element-wise on the input column in the provided context.
   *
   * @param context The projection context containing execution environment
   * @return The resulting column after element-wise evaluation
   */
  @Nonnull
  default Column evaluateElementWise(@Nonnull final ProjectionContext context) {
    return new DefaultRepresentation(context.inputContext().getColumnValue())
        .transform(asColumnOperator(context))
        .flatten()
        .getValue();
  }

  /**
   * Returns a stream of child projection clauses.
   * <p>
   * The default implementation returns an empty stream for leaf nodes. Implementations with
   * children should override this method.
   * </p>
   *
   * @return a stream of child projection clauses
   */
  @Nonnull
  default Stream<ProjectionClause> getChildren() {
    return Stream.empty();
  }

  /**
   * Returns an expression string representation of this clause.
   * <p>
   * Used by {@link #toExpressionTree(int)} to display this node in the tree.
   * </p>
   *
   * @return a string representation of this clause's expression
   */
  @Nonnull
  String toExpression();


  /**
   * Returns a tree-like string representation of this clause for debugging purposes.
   *
   * @return a formatted tree string representation
   */
  @Nonnull
  default String toExpressionTree() {
    return toExpressionTree(0);
  }

  /**
   * Returns a tree-like string representation of this clause for debugging purposes.
   * <p>
   * The default implementation uses {@link #toExpression()} for this node and recursively calls
   * itself on children from {@link #getChildren()}.
   * </p>
   *
   * @param level the indentation level for the tree structure
   * @return a formatted tree string representation
   */
  @Nonnull
  default String toExpressionTree(final int level) {
    final String indent = "  ".repeat(level);
    final String childrenStr = getChildren()
        .map(child -> child.toExpressionTree(level + 1))
        .collect(Collectors.joining("\n"));

    if (childrenStr.isEmpty()) {
      return indent + toExpression();
    }
    return indent + toExpression() + "\n" + childrenStr;
  }

}

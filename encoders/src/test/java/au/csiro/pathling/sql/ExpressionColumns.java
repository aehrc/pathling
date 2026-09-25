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
 */
package au.csiro.pathling.sql;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.classic.ExpressionUtils;

/**
 * Converts between columns and Catalyst expressions for the Scala tests, which cannot reach {@link
 * ExpressionUtils} directly because it is private to the Spark SQL package in Scala.
 */
public final class ExpressionColumns {

  private ExpressionColumns() {}

  /**
   * Wraps a Catalyst expression as a column.
   *
   * @param expression the expression to wrap
   * @return the column
   */
  @Nonnull
  public static Column column(@Nonnull final Expression expression) {
    return ExpressionUtils.column(expression);
  }

  /**
   * Unwraps a column to its Catalyst expression.
   *
   * @param column the column to unwrap
   * @return the expression
   */
  @Nonnull
  public static Expression expression(@Nonnull final Column column) {
    return ExpressionUtils.expression(column);
  }
}

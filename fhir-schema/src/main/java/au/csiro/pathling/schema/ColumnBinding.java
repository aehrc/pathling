/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.schema;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.transform;

import jakarta.annotation.Nonnull;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;

/** Binds a column to a single evaluation, for the engine and the annotation step alike. */
public final class ColumnBinding {

  private ColumnBinding() {}

  /**
   * Evaluates {@code value} exactly once per row, whether it is deterministic or not, and passes
   * the result to {@code body}.
   *
   * <p>It always materialises the operand, through {@code element_at(transform(array(value), body),
   * 1)}. It is for an operand that is expensive to compute, such as a user-defined function, which
   * {@code body} references more than once: each reference to the same deterministic expression in
   * a Spark tree is otherwise computed separately, as it is wherever common subexpressions are not
   * eliminated, such as inside a lambda.
   *
   * @param value the operand to evaluate once per row
   * @param body the lambda that consumes the evaluated operand
   * @return a column expression applying {@code body} to a single evaluation of {@code value}
   */
  @Nonnull
  public static Column bindOnce(
      @Nonnull final Column value, @Nonnull final UnaryOperator<Column> body) {
    return element_at(transform(array(value), body::apply), 1);
  }
}

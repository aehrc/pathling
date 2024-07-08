/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.query;

import static au.csiro.pathling.utilities.Preconditions.requireNonBlank;
import static java.util.Objects.nonNull;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.Value;

@Value
public class ExpressionWithLabel {

  @Nonnull
  String expression;

  @Nullable
  String label;

  private ExpressionWithLabel(@Nonnull final String expression, @Nullable final String label) {
    this.expression = requireNonBlank(expression, "Column expression cannot be blank");
    this.label = nonNull(label)
                 ? requireNonBlank(label, "Column label cannot be blank")
                 : null;
  }

  public static ExpressionWithLabel of(@Nonnull final String expression,
      @Nonnull final String label) {
    return new ExpressionWithLabel(expression, label);
  }

  public static ExpressionWithLabel withExpressionAsLabel(@Nonnull final String expression) {
    return new ExpressionWithLabel(expression, expression);
  }

  public static ExpressionWithLabel withNoLabel(@Nonnull final String expression) {
    return new ExpressionWithLabel(expression, null);
  }


  /**
   * Returns a list of the expressions from the given list of {@link ExpressionWithLabel} objects.
   *
   * @param expressionWithLabels the list of {@link ExpressionWithLabel} objects
   * @return a list of the expressions
   */
  @Nonnull
  public static List<String> expressionsAsList(
      @Nonnull final List<ExpressionWithLabel> expressionWithLabels) {
    return expressionWithLabels.stream().map(ExpressionWithLabel::getExpression)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Returns a list of the optional labels from the given list of {@link ExpressionWithLabel}
   * objects.
   *
   * @param expressionWithLabels the list of {@link ExpressionWithLabel} objects
   * @return a list of the optional labels
   */
  @Nonnull
  public static Stream<Optional<String>> labelsAsStream(
      @Nonnull final List<ExpressionWithLabel> expressionWithLabels) {
    return expressionWithLabels.stream().map(ExpressionWithLabel::getLabel)
        .map(Optional::ofNullable);
  }

  /**
   * Returns a list of {@link ExpressionWithLabel} objects, where each expression has no label.
   *
   * @param expressions the list of expressions
   * @return a list of {@link ExpressionWithLabel} objects
   */
  @Nonnull
  public static List<ExpressionWithLabel> fromUnlabelledExpressions(
      @Nonnull final List<String> expressions) {
    return expressions.stream().map(ExpressionWithLabel::withNoLabel)
        .collect(Collectors.toUnmodifiableList());
  }
}

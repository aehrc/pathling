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

package au.csiro.pathling.test.yaml.format;

import au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase;
import jakarta.annotation.Nonnull;
import java.util.function.Predicate;
import lombok.Value;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

@Value(staticConstructor = "of")
class SpELPredicate implements Predicate<TestCase> {

  private static final ExpressionParser PARSER = new SpelExpressionParser();

  @Nonnull String spELExpression;

  @Override
  public boolean test(final TestCase testCase) {
    final EvaluationContext context = new StandardEvaluationContext();
    context.setVariable("testCase", testCase);
    final Expression exp = PARSER.parseExpression(spELExpression);
    return Boolean.TRUE.equals(exp.getValue(context, Boolean.class));
  }
}

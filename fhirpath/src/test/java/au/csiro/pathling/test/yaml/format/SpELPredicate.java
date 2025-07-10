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

  @Nonnull
  String spELExpression;

  @Override
  public boolean test(final TestCase testCase) {
    final EvaluationContext context = new StandardEvaluationContext();
    context.setVariable("testCase", testCase);
    final Expression exp = PARSER.parseExpression(spELExpression);
    return Boolean.TRUE.equals(exp.getValue(context, Boolean.class));
  }
}

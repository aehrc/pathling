import au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/** Throwaway probe for T100h: can an exclusion's SpEL matcher depend on the test layout? */
public class SpelProbe {

  public static void main(final String[] args) {
    final String spel =
        "#testCase.expression matches '^StructureDefinition.*'"
            + " and !T(au.csiro.pathling.test.layout.TestLayout).active().isPof()";
    final TestCase tc =
        new TestCase(
            "** StructureDefinition.snapshot.element.type.code is uri",
            "StructureDefinition.snapshot.element.type.code is uri",
            null, null, null, null, null, false, null);
    for (final String layout : new String[] {"previous", "pof"}) {
      System.setProperty("pathling.testLayout", layout);
      final StandardEvaluationContext ctx = new StandardEvaluationContext();
      ctx.setVariable("testCase", tc);
      System.out.println(
          layout + " -> " + new SpelExpressionParser().parseExpression(spel).getValue(ctx, Boolean.class));
    }
  }
}
